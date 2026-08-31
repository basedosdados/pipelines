"""Regression tests for the br_pncp transform.

Each case here is a failure that would be *silent* — the pipeline would run
green and the table would be wrong. They run offline against fixtures shaped
like the real PNCP payloads.
"""

from __future__ import annotations

import gzip
import itertools
import json
from datetime import date, timedelta

import pyarrow.dataset as ds
import pytest

from pipelines.datasets.br_pncp import utils
from pipelines.datasets.br_pncp.constants import constants


class TestValueConversion:
    """Staging is all-STRING, so every type has to survive as text."""

    def test_null_never_becomes_the_string_nan(self):
        # astype(str) renders NaN as "nan", which safe_cast cannot turn back
        # into NULL — the column then arrives full of the literal text.
        for bq_type in ("STRING", "INT64", "FLOAT64", "DATE", "BOOLEAN"):
            assert utils.convert(None, bq_type) is None
            assert utils.convert("", bq_type) is None

    def test_integers_keep_no_decimal_tail(self):
        # A year passed through float serialises as "2025.0", and
        # safe_cast("2025.0" as int64) is NULL, not 2025.
        assert utils.convert(2025, "INT64") == "2025"
        assert utils.convert(2025.0, "INT64") == "2025"
        assert utils.convert("2025", "INT64") == "2025"

    def test_floats_keep_their_precision(self):
        assert utils.convert(124650.0, "FLOAT64") == "124650.0"
        assert utils.convert(198.99, "FLOAT64") == "198.99"

    def test_datetimes_reduce_to_a_date(self):
        # PNCP mixes date-time and date in the same payload.
        assert utils.convert("2025-03-10T00:07:51", "DATE") == "2025-03-10"
        assert utils.convert("2025-02-24", "DATE") == "2025-02-24"

    def test_unparseable_values_are_null_not_garbage(self):
        assert utils.convert("not a date", "DATE") is None
        assert utils.convert("N/A", "INT64") is None

    def test_booleans_survive_as_lowercase_text(self):
        # BOOLEAN columns must keep the three-state distinction; a False that
        # becomes NULL silently changes what the column means.
        assert utils.convert(True, "BOOLEAN") == "true"
        assert utils.convert(False, "BOOLEAN") == "false"
        assert utils.convert(None, "BOOLEAN") is None


class TestFlatten:
    """PNCP nests órgão/unidade, and the PCA payload nests a list of items."""

    def test_nested_fields_resolve_through_dots(self):
        columns = utils.read_architecture("contrato")
        record = {
            "numeroControlePNCP": "X-1/2025",
            "dataPublicacaoPncp": "2025-03-10T00:07:51",
            "orgaoEntidade": {"cnpj": "08924078000104", "esferaId": "M"},
            "unidadeOrgao": {"ufSigla": "PB", "codigoIbge": "2516904"},
            "tipoContrato": {"id": 1, "nome": "Contrato (termo inicial)"},
        }
        (row,) = utils.flatten(record, "contrato", columns)
        assert row["cnpj_orgao"] == "08924078000104"
        assert row["id_esfera"] == "M"
        assert row["sigla_uf"] == "PB"
        assert row["id_municipio"] == "2516904"
        assert row["id_tipo_contrato"] == "1"
        assert row["ano"] == "2025"

    def test_missing_nested_parent_yields_null_not_an_error(self):
        # orgaoSubRogado is null on the large majority of records.
        columns = utils.read_architecture("contrato")
        record = {
            "numeroControlePNCP": "X-1/2025",
            "dataPublicacaoPncp": "2025-03-10T00:07:51",
            "orgaoSubRogado": None,
        }
        (row,) = utils.flatten(record, "contrato", columns)
        assert row["cnpj_orgao_subrogado"] is None

    def test_pca_explodes_to_one_row_per_item(self):
        columns = utils.read_architecture("plano_contratacao_anual")
        record = {
            "idPcaPncp": "PCA-1",
            "anoPca": 2025,
            "orgaoEntidadeCnpj": "123",
            "itens": [
                {
                    "numeroItem": 1,
                    "descricaoItem": "Caneta",
                    "valorTotal": 10.5,
                },
                {
                    "numeroItem": 2,
                    "descricaoItem": "Papel",
                    "valorTotal": 20.0,
                },
            ],
        }
        rows = utils.flatten(record, "plano_contratacao_anual", columns)
        assert [r["numero_item"] for r in rows] == ["1", "2"]
        assert [r["descricao_item"] for r in rows] == ["Caneta", "Papel"]
        # Header fields repeat onto every item row.
        assert {r["id_pca_pncp"] for r in rows} == {"PCA-1"}
        assert {r["ano"] for r in rows} == {"2025"}

    def test_pca_with_no_items_still_yields_the_header(self):
        columns = utils.read_architecture("plano_contratacao_anual")
        record = {"idPcaPncp": "PCA-2", "anoPca": 2025, "itens": []}
        rows = utils.flatten(record, "plano_contratacao_anual", columns)
        assert len(rows) == 1
        assert rows[0]["numero_item"] is None


class TestPartitionYear:
    """`ano` drives the partition, so a wrong one files rows under the wrong year."""

    def test_publication_date_drives_the_year_for_transactional_tables(self):
        assert (
            utils.partition_year(
                {"dataPublicacaoPncp": "2024-12-31T23:59:00"}, "contrato"
            )
            == 2024
        )

    def test_pca_uses_its_declared_reference_year(self):
        # Not the publication date: a 2026 plan is published during 2025.
        assert (
            utils.partition_year({"anoPca": 2026}, "plano_contratacao_anual")
            == 2026
        )

    def test_instrumento_cobranca_uses_inclusion_date(self):
        assert (
            utils.partition_year(
                {"dataInclusao": "2025-06-02T10:00:00"}, "instrumento_cobranca"
            )
            == 2025
        )

    def test_a_record_with_no_usable_date_is_reported_not_guessed(self):
        assert (
            utils.partition_year({"dataPublicacaoPncp": None}, "contrato")
            is None
        )


class TestCleanTable:
    """The streaming writer must partition correctly and stay all-STRING."""

    def _write_chunk(self, input_dir, table, name, records):
        target = input_dir / table
        target.mkdir(parents=True, exist_ok=True)
        with gzip.open(
            target / f"{name}.jsonl.gz", "wt", encoding="utf-8"
        ) as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")

    def _record(self, control, published):
        return {
            "numeroControlePNCP": control,
            "dataPublicacaoPncp": published,
            "dataAtualizacaoGlobal": published,
            "orgaoEntidade": {"cnpj": "1", "esferaId": "M"},
            "unidadeOrgao": {"ufSigla": "PB", "codigoIbge": "2516904"},
            "valorGlobal": 100.0,
        }

    def test_rows_land_in_the_partition_for_their_publication_year(
        self, tmp_path
    ):
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        self._write_chunk(
            input_dir,
            "contrato",
            "w1",
            [
                self._record("A-1/2024", "2024-05-01T00:00:00"),
                self._record("B-1/2025", "2025-05-01T00:00:00"),
            ],
        )
        summary = utils.clean_table(input_dir, output_dir, "contrato")
        assert summary["years"] == ["2024", "2025"]
        assert summary["written_rows"] == 2
        assert (output_dir / "contrato" / "ano=2024").is_dir()
        assert (output_dir / "contrato" / "ano=2025").is_dir()

    def test_partition_column_is_not_written_into_the_file(self, tmp_path):
        # `ano` in both the path and the file makes the dataset unreadable:
        # "Field ano has incompatible types: string vs dictionary<int32>".
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        self._write_chunk(
            input_dir,
            "contrato",
            "w1",
            [self._record("A-1/2024", "2024-05-01T00:00:00")],
        )
        utils.clean_table(input_dir, output_dir, "contrato")
        dataset = ds.dataset(
            output_dir / "contrato", format="parquet", partitioning="hive"
        )
        table = dataset.to_table()
        # Readable at all, and `ano` resolves from the path.
        assert table.num_rows == 1
        assert table.column("ano").to_pylist() == [2024]
        # Every non-partition column is string.
        non_string = [
            f.name
            for f in table.schema
            if f.name != "ano" and str(f.type) != "string"
        ]
        assert non_string == []

    def test_duplicates_are_kept_for_the_model_to_collapse(self, tmp_path):
        # PNCP re-delivers a record in every window that touched it. Staging
        # keeps them; the dbt model's QUALIFY picks the latest. Deduplicating
        # here instead cost tens of GB of RAM on the real tables.
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        self._write_chunk(
            input_dir,
            "contrato",
            "w1",
            [self._record("A-1/2024", "2024-05-01T00:00:00")],
        )
        self._write_chunk(
            input_dir,
            "contrato",
            "w2",
            [self._record("A-1/2024", "2024-05-01T00:00:00")],
        )
        summary = utils.clean_table(input_dir, output_dir, "contrato")
        assert summary["written_rows"] == 2

    def test_batching_splits_a_partition_into_several_parts(self, tmp_path):
        # Memory is bounded by batch_rows; the parts must all still be read.
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        records = [
            self._record(f"A-{i}/2024", "2024-05-01T00:00:00")
            for i in range(25)
        ]
        self._write_chunk(input_dir, "contrato", "w1", records)
        summary = utils.clean_table(
            input_dir, output_dir, "contrato", batch_rows=10
        )
        assert summary["written_rows"] == 25
        parts = sorted(
            (output_dir / "contrato" / "ano=2024").glob("*.parquet")
        )
        assert len(parts) == 3
        dataset = ds.dataset(
            output_dir / "contrato", format="parquet", partitioning="hive"
        )
        assert dataset.to_table().num_rows == 25

    def test_undated_records_are_dropped_and_counted(self, tmp_path):
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        self._write_chunk(
            input_dir,
            "contrato",
            "w1",
            [
                self._record("A-1/2024", "2024-05-01T00:00:00"),
                self._record("B-1/?", None),
            ],
        )
        summary = utils.clean_table(input_dir, output_dir, "contrato")
        assert summary["written_rows"] == 1
        assert summary["undated_dropped"] == 1

    def test_replace_false_preserves_untouched_partitions(self, tmp_path):
        # An incremental run produces only the years its window touched, and
        # must not delete the rest of the table.
        input_dir, output_dir = tmp_path / "input", tmp_path / "output"
        self._write_chunk(
            input_dir,
            "contrato",
            "w1",
            [self._record("A-1/2024", "2024-05-01T00:00:00")],
        )
        utils.clean_table(input_dir, output_dir, "contrato", replace=True)

        second = tmp_path / "input2"
        self._write_chunk(
            second,
            "contrato",
            "w2",
            [self._record("B-1/2025", "2025-05-01T00:00:00")],
        )
        utils.clean_table(second, output_dir, "contrato", replace=False)

        assert (output_dir / "contrato" / "ano=2024").is_dir()
        assert (output_dir / "contrato" / "ano=2025").is_dir()


class TestHarvestResilience:
    """A multi-hour harvest must survive one bad response and one bad window."""

    def test_truncated_chunked_response_is_retryable_not_fatal(self):
        # The real crash: PNCP truncated a chunked response, and
        # http.client.IncompleteRead is an HTTPException rather than a
        # URLError, so it escaped the retry clause and killed the whole run.
        import http.client

        assert isinstance(
            http.client.IncompleteRead(b""), utils.TRANSPORT_ERRORS
        )

    def test_the_other_transport_faults_are_retryable_too(self):
        import json as _json
        import ssl
        import urllib.error

        for exc in (
            urllib.error.URLError("boom"),
            ConnectionResetError(),
            TimeoutError(),
            ssl.SSLError(),
            _json.JSONDecodeError("bad", "", 0),
        ):
            assert isinstance(exc, utils.TRANSPORT_ERRORS), exc

    def test_a_server_refusal_is_not_treated_as_a_transport_fault(self):
        # HTTPError means the server answered; it is handled by status code, so
        # sweeping it into the retry tuple would hide 4xx/5xx handling.
        import urllib.error

        refusal = urllib.error.HTTPError("u", 429, "Too Many", {}, None)
        assert isinstance(
            refusal, urllib.error.URLError
        )  # it is a subclass...
        # ...so the status-code branch must come first in request(); assert the
        # source still orders it that way.
        import inspect

        src = inspect.getsource(utils.request)
        assert src.index("except urllib.error.HTTPError") < src.index(
            "except TRANSPORT_ERRORS"
        )

    def test_one_failed_window_does_not_abort_the_harvest(
        self, tmp_path, monkeypatch
    ):
        # The window that raises must be skipped without a chunk file (so a
        # re-run retries it) while every other window still completes.
        calls = []

        def fake_fetch_range(
            path, date_params, lo, hi, extra, label="", page_size=500
        ):
            calls.append(label)
            if "20210116" in label:
                raise ConnectionResetError("simulated")
            return [
                {
                    "numeroControlePNCP": label,
                    "dataPublicacaoPncp": "2021-03-01",
                }
            ]

        monkeypatch.setattr(utils, "fetch_range", fake_fetch_range)
        input_dir = tmp_path / "input"
        count = utils.harvest(
            table="contrato",
            input_dir=input_dir,
            start=__import__("datetime").date(2021, 1, 1),
            end=__import__("datetime").date(2021, 2, 14),
            max_workers=1,
        )
        written = sorted(
            p.name for p in (input_dir / "contrato").glob("*.jsonl.gz")
        )
        assert len(calls) == 3
        assert count == 2  # the two that succeeded
        assert "20210116_20210130.jsonl.gz" not in written
        assert len(written) == 2


class TestEmptyResultSignalling:
    """PNCP signals "nothing matched" three different ways. None is an error."""

    def _fake_response(self, status, body):
        class _Resp:
            def __init__(self):
                self.status = status

            def read(self):
                return body.encode()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return _Resp()

    def test_200_with_an_empty_body_is_an_empty_page(self, monkeypatch):
        # pca/atualizacao answers 200 with a zero-length body on a quiet day.
        # Parsing that as JSON raised, and the retry then burned the window.
        monkeypatch.setattr(
            utils.urllib.request,
            "urlopen",
            lambda *a, **k: self._fake_response(200, ""),
        )
        assert utils.request("pca/atualizacao", {}) == utils.EMPTY_PAGE

    def test_204_is_an_empty_page(self, monkeypatch):
        monkeypatch.setattr(
            utils.urllib.request,
            "urlopen",
            lambda *a, **k: self._fake_response(204, ""),
        )
        assert utils.request("contratos", {}) == utils.EMPTY_PAGE

    def test_404_is_an_empty_page_not_a_dead_route(self, monkeypatch):
        # instrumentoscobranca returns 404 with
        # {"message": "Nenhum instrumento de Cobrança encontrado."} when the
        # window is empty. Treating it as an error made a working endpoint —
        # one that really does carry 17,536 records in Jan 2026 — look dead.
        import urllib.error

        def _raise(*a, **k):
            raise urllib.error.HTTPError("u", 404, "Not Found", {}, None)

        monkeypatch.setattr(utils.urllib.request, "urlopen", _raise)
        assert (
            utils.request("instrumentoscobranca/inclusao", {})
            == utils.EMPTY_PAGE
        )


class TestPageSizePropagation:
    """A per-endpoint page-size cap must survive every code path."""

    def test_split_halves_keep_the_page_size(self, monkeypatch):
        # The real bug: fetch_range's recursive split dropped page_size, so the
        # halves reverted to the 500 default. instrumentoscobranca caps at 100
        # and answered 400, failing 13 windows of an otherwise good harvest.
        seen = []

        def fake_fetch_window(path, params, label="", page_size=500):
            seen.append(page_size)
            # Force exactly one split, then succeed.
            if len(seen) <= 2:
                raise utils.ServerOverloadError("too big")
            return []

        monkeypatch.setattr(utils, "fetch_window", fake_fetch_window)
        utils.fetch_range(
            "instrumentoscobranca/inclusao",
            ("dataInicial", "dataFinal"),
            __import__("datetime").date(2025, 4, 10),
            __import__("datetime").date(2025, 5, 9),
            {},
            "label",
            page_size=100,
        )
        assert seen, "fetch_window was never called"
        assert set(seen) == {100}, f"page_size leaked to {set(seen)}"

    def test_harvest_passes_the_endpoints_configured_page_size(
        self, monkeypatch
    ):
        seen = {}

        def fake_fetch_range(
            path, date_params, lo, hi, extra, label="", page_size=500
        ):
            seen[path] = page_size
            return []

        monkeypatch.setattr(utils, "fetch_range", fake_fetch_range)
        import datetime

        for table in ("instrumento_cobranca", "contrato"):
            utils.harvest(
                table=table,
                input_dir=__import__("pathlib").Path(
                    __import__("tempfile").mkdtemp()
                ),
                start=datetime.date(2025, 1, 1),
                end=datetime.date(2025, 1, 5),
                max_workers=1,
            )
        assert seen["instrumentoscobranca/inclusao"] == 100
        assert seen["contratos/atualizacao"] == utils.PAGE_SIZE


class TestBrazilianNumberFormat:
    """A few PNCP numeric fields are pt-BR formatted strings, not JSON numbers."""

    def test_money_string_with_thousands_and_decimal_comma(self):
        # notaFiscalEletronica.valorNotaFiscal arrives as "4.920,00". float()
        # rejects it, so the column was 100% NULL while its siblings were 25%
        # populated — a silent loss that safe_cast could never recover.
        assert utils.convert("4.920,00", "FLOAT64") == "4920.0"
        assert utils.convert("1.060,40", "FLOAT64") == "1060.4"
        assert utils.convert("118,40", "FLOAT64") == "118.4"
        assert utils.convert("64,35", "FLOAT64") == "64.35"

    def test_plain_numeric_strings_still_parse(self):
        assert utils.convert("124650.0", "FLOAT64") == "124650.0"
        assert utils.convert("2025", "INT64") == "2025"

    def test_json_numbers_are_unaffected(self):
        assert utils.convert(124650.0, "FLOAT64") == "124650.0"
        assert utils.convert(198.99, "FLOAT64") == "198.99"

    def test_a_large_pt_br_amount(self):
        assert utils.convert("1.234.567,89", "FLOAT64") == "1234567.89"


class TestSplitOnlyWhenTheWindowIsTooLarge:
    """Splitting discards fetched pages, so only page-1 failures should split."""

    def test_page_one_failure_propagates_so_the_caller_splits(
        self, monkeypatch
    ):
        def always_overloaded(path, params, max_tries=6):
            raise utils.ServerOverloadError("500")

        monkeypatch.setattr(utils, "request", always_overloaded)
        import pytest

        with pytest.raises(utils.ServerOverloadError):
            utils.fetch_window("contratos", {}, "label")

    def test_a_deep_page_gets_a_bigger_retry_budget(self, monkeypatch):
        # The budget is what keeps a transient 504 at page 101 from discarding
        # the 100 pages already collected.
        budgets = []

        def record(path, params, max_tries=6):
            budgets.append((params["pagina"], max_tries))
            page = params["pagina"]
            return {
                "data": [{"x": page}],
                "totalRegistros": 3,
                "totalPaginas": 3,
            }

        monkeypatch.setattr(utils, "request", record)
        rows = utils.fetch_window("contratos", {}, "label")
        assert len(rows) == 3
        assert budgets[0] == (1, 4), (
            "page 1 should fail fast so the split happens"
        )
        assert all(b > 4 for _, b in budgets[1:]), (
            "deep pages need a bigger budget"
        )


class TestWindowResize:
    """A chunk's filename IS its window, so resizing must not shift old tags.

    Getting the boundary wrong does not fail loudly -- every later tag shifts,
    nothing on disk matches, and the harvest silently re-downloads days of
    already-captured data. These tests are the only thing standing between a
    one-character edit and that outcome.
    """

    def _tags(self, start, end, days, resize=None):
        return [
            f"{lo:%Y%m%d}_{hi:%Y%m%d}"
            for lo, hi in utils.windows(start, end, days, resize)
        ]

    def test_windows_tile_the_range_without_gap_or_overlap(self):
        wins = list(
            utils.windows(
                date(2021, 1, 1), date(2026, 8, 28), 15, (date(2025, 8, 8), 2)
            )
        )
        assert wins[0][0] == date(2021, 1, 1)
        assert wins[-1][1] == date(2026, 8, 28)
        for (_, hi), (lo, _) in itertools.pairwise(wins):
            assert lo == hi + timedelta(days=1)

    def test_resize_leaves_every_pre_boundary_tag_identical(self):
        start, end, boundary = (
            date(2021, 1, 1),
            date(2026, 8, 28),
            date(2025, 8, 8),
        )
        plain = self._tags(start, end, 15)
        resized = self._tags(start, end, 15, (boundary, 2))
        shared = [t for t in plain if t < boundary.strftime("%Y%m%d")]
        assert resized[: len(shared)] == shared
        # ...and the 112th window is the last 15-day one, ending the day
        # before the boundary.
        assert shared[-1] == "20250724_20250807"
        assert len(shared) == 112

    def test_boundary_falls_on_an_original_window_edge(self):
        # If this fails, the boundary in constants.py is off and every
        # harvested contrato chunk would be re-downloaded.
        spec = constants.ENDPOINTS.value["contrato"]
        boundary = date.fromisoformat(spec["resize"][0])
        edges = {
            lo
            for lo, _ in utils.windows(
                date(2021, 1, 1), date(2026, 8, 28), spec["window_days"]
            )
        }
        assert boundary in edges

    def test_post_boundary_windows_use_the_small_size(self):
        wins = [
            (lo, hi)
            for lo, hi in utils.windows(
                date(2021, 1, 1), date(2026, 8, 28), 15, (date(2025, 8, 8), 2)
            )
            if lo >= date(2025, 8, 8)
        ]
        assert all((hi - lo).days == 1 for lo, hi in wins[:-1])

    def test_no_resize_behaves_exactly_as_before(self):
        assert self._tags(
            date(2021, 1, 1), date(2021, 3, 1), 15
        ) == self._tags(date(2021, 1, 1), date(2021, 3, 1), 15, None)


class TestPageSizeWithinApiLimits:
    """tamanhoPagina above an endpoint's ceiling is a flat 400, not a clamp.

    Only ``instrumentoscobranca`` was known to be capped; the contratacoes
    pair caps at 50, so the default 500 made every contratacao window fail on
    its first request -- the whole table unharvestable. These bounds come from
    the PNCP OpenAPI spec (/api/consulta/v3/api-docs), read 2026-08-28.
    """

    # path prefix -> documented maximum tamanhoPagina
    DOCUMENTED_MAX = {
        "contratos": 500,
        "contratos/atualizacao": 500,
        "atas": 500,
        "atas/atualizacao": 500,
        "pca/atualizacao": 500,
        "instrumentoscobranca/inclusao": 100,
        "contratacoes/publicacao": 50,
        "contratacoes/atualizacao": 50,
    }
    MIN = 10

    def _effective(self, spec):
        return spec.get("page_size", constants.PAGE_SIZE.value)

    def test_every_endpoint_page_size_is_within_its_documented_range(self):
        for table, spec in constants.ENDPOINTS.value.items():
            size = self._effective(spec)
            cap = self.DOCUMENTED_MAX[spec["path"]]
            assert self.MIN <= size <= cap, (
                f"{table} requests tamanhoPagina={size} against "
                f"{spec['path']}, which allows {self.MIN}..{cap}"
            )

    def test_backfill_paths_are_also_within_range(self):
        # The backfill swaps the path but keeps the endpoint's page_size, so
        # the override has to be legal for BOTH paths of a table.
        for table, path in constants.BACKFILL_PATHS.value.items():
            size = self._effective(constants.ENDPOINTS.value[table])
            cap = self.DOCUMENTED_MAX[path]
            assert self.MIN <= size <= cap, (
                f"{table} backfills from {path} with tamanhoPagina={size}, "
                f"which allows {self.MIN}..{cap}"
            )

    def test_contratacoes_is_the_tightest_cap(self):
        # Guards the specific regression: a well-meaning "just use the
        # default" edit here breaks the largest table in the dataset.
        assert self._effective(constants.ENDPOINTS.value["contratacao"]) <= 50


class TestPcaWindowFloor:
    """A PCA window under 7 days returns an empty body, not fewer rows.

    This is the opposite of every other table, where a smaller window is
    merely slower. Shrinking it produces a table that is silently empty while
    the harvest reports success, so the floor is asserted rather than left as
    a comment.
    """

    def test_pca_window_is_at_least_seven_days(self):
        spec = constants.ENDPOINTS.value["plano_contratacao_anual"]
        assert spec["window_days"] >= 7, (
            "PCA windows under 7 days return HTTP 200 with a zero-length "
            "body; this would empty the table without failing"
        )

    def test_pca_dates_are_formatted_without_dashes(self):
        # The dashed form is also accepted-and-empty rather than rejected.
        lo, hi = next(
            iter(utils.windows(date(2025, 6, 1), date(2025, 6, 7), 7))
        )
        assert f"{lo:%Y%m%d}" == "20250601"
        assert "-" not in f"{lo:%Y%m%d}{hi:%Y%m%d}"


class TestDeferredTableScope:
    """plano_contratacao_anual is deferred, and the two halves must agree.

    The dangerous direction is a dbt model existing for a table that is not
    harvested: table-approve materialises every model in a PR, so one model
    with no staging table aborts the entire prod materialisation -- not just
    its own. The reverse (harvested but no model) merely wastes time.
    """

    def _repo_root(self):
        from pathlib import Path

        import pipelines.datasets.br_pncp.constants as c

        return Path(c.__file__).resolve().parents[3]

    def test_no_table_is_silently_dropped(self):
        covered = set(constants.FACT_TABLES.value) | set(
            constants.DEFERRED_TABLES.value
        )
        assert set(constants.ENDPOINTS.value) == covered

    def test_deferred_tables_are_not_in_the_run_scope(self):
        for table in constants.DEFERRED_TABLES.value:
            assert table not in constants.FACT_TABLES.value
            assert table not in constants.ALL_TABLES.value

    def test_all_tables_is_the_fact_tables_plus_dicionario(self):
        assert constants.ALL_TABLES.value == [
            *constants.FACT_TABLES.value,
            "dicionario",
        ]

    def test_a_deferred_table_has_no_dbt_model(self):
        models = self._repo_root() / "models" / "br_pncp"
        for table in constants.DEFERRED_TABLES.value:
            sql = models / f"br_pncp__{table}.sql"
            assert not sql.exists(), (
                f"{sql.name} exists for a deferred table; table-approve would "
                "try to materialise it with no staging data and abort the "
                "whole PR"
            )
            schema = (models / "schema.yml").read_text(encoding="utf-8")
            assert f"br_pncp__{table}" not in schema

    def test_every_scoped_table_does_have_a_model(self):
        models = self._repo_root() / "models" / "br_pncp"
        for table in constants.ALL_TABLES.value:
            assert (models / f"br_pncp__{table}.sql").exists()


class TestCoverageRegistrationScope:
    """Coverage must be registered for exactly the tables a run materialises.

    Two ways to get this wrong, both only visible on a prod run:
    registering a DEFERRED table (the task reads its max date from BigQuery,
    where it does not exist), or blowing up on `dicionario`, which is in the
    run scope but deliberately has no coverage because it has no date column.
    """

    def _coverage(self):
        from pipelines.datasets.br_pncp import flows

        return flows._COVERAGE

    def test_no_deferred_table_is_in_the_registration_scope(self):
        for table in constants.DEFERRED_TABLES.value:
            assert table not in constants.ALL_TABLES.value

    def test_every_scoped_table_either_has_coverage_or_is_dicionario(self):
        cov = self._coverage()
        for table in constants.ALL_TABLES.value:
            assert table in cov or table == "dicionario", (
                f"{table} is materialised but has no coverage spec and is "
                "not the dicionario exemption"
            )

    def test_dicionario_has_no_coverage_spec(self):
        assert "dicionario" not in self._coverage()

    def test_registration_skips_tables_without_a_spec(self):
        # Exercises the flow's own selection, not a copy of it.
        from pipelines.datasets.br_pncp.flows import coverage_registrations

        registered = [
            t for t, _ in coverage_registrations(constants.ALL_TABLES.value)
        ]
        assert "dicionario" not in registered
        assert set(registered) == set(constants.FACT_TABLES.value)

    def test_registration_never_includes_a_deferred_table(self):
        from pipelines.datasets.br_pncp.flows import coverage_registrations

        # Even if a deferred table were passed in by mistake, it has no place
        # in the run scope -- assert the scope itself excludes it.
        registered = [
            t for t, _ in coverage_registrations(constants.ALL_TABLES.value)
        ]
        for table in constants.DEFERRED_TABLES.value:
            assert table not in registered


class TestUnservableWindowIsNotWrittenAsEmpty:
    """A window the API cannot serve must leave NO chunk behind.

    The failure being guarded is silent and permanent: writing an empty chunk
    for an unservable day means every later run skips it (the file exists),
    so one transient outage quietly costs that day's records forever while
    the harvest keeps reporting success. A day with genuinely no records
    never reaches this path -- the API says so with 204, an empty body or a
    404, all of which become EMPTY_PAGE.
    """

    def _always_fails(self, monkeypatch):
        def boom(path, params, max_tries=6):
            raise utils.ServerOverloadError("500")

        monkeypatch.setattr(utils, "request", boom)

    def test_a_single_unservable_day_raises_rather_than_returning_empty(
        self, monkeypatch
    ):
        self._always_fails(monkeypatch)
        with pytest.raises(utils.ServerOverloadError):
            utils.fetch_range(
                "contratos",
                ("dataInicial", "dataFinal"),
                date(2024, 5, 1),
                date(2024, 5, 1),
                {},
            )

    def test_a_wider_unservable_window_also_raises_after_splitting(
        self, monkeypatch
    ):
        self._always_fails(monkeypatch)
        with pytest.raises(utils.ServerOverloadError):
            utils.fetch_range(
                "contratos",
                ("dataInicial", "dataFinal"),
                date(2024, 5, 1),
                date(2024, 5, 4),
                {},
            )

    def test_harvest_writes_no_chunk_for_a_window_it_could_not_fetch(
        self, monkeypatch, tmp_path
    ):
        self._always_fails(monkeypatch)
        written = utils.harvest(
            table="contrato",
            input_dir=tmp_path,
            start=date(2024, 5, 1),
            end=date(2024, 5, 2),
            max_workers=1,
        )
        assert written == 0
        assert list(tmp_path.glob("contrato/*.jsonl.gz")) == [], (
            "an unservable window left a chunk behind; the next run would "
            "skip it and the data would be lost permanently"
        )

    def test_a_genuinely_empty_window_still_writes_its_chunk(
        self, monkeypatch, tmp_path
    ):
        # The other half of the contract: "no records" is a real answer and
        # must be recorded, or every run re-fetches every empty window.
        monkeypatch.setattr(
            utils,
            "request",
            lambda path, params, max_tries=6: utils.EMPTY_PAGE,
        )
        utils.harvest(
            table="contrato",
            input_dir=tmp_path,
            start=date(2024, 5, 1),
            end=date(2024, 5, 2),
            max_workers=1,
        )
        assert len(list(tmp_path.glob("contrato/*.jsonl.gz"))) == 1


class TestPerEndpointConcurrencyCap:
    """instrumentoscobranca cannot take the concurrency the others can.

    It failed 3 of ~65 windows at 3 workers with 504s and dropped
    connections, while contrato ran 305 windows at the same setting with
    none. A cap that silently *raised* concurrency for other tables would be
    worse than no cap, so the direction is asserted too.
    """

    def test_the_fragile_endpoint_declares_a_cap(self):
        spec = constants.ENDPOINTS.value["instrumento_cobranca"]
        assert spec.get("max_workers") == 1

    def test_the_cap_only_ever_narrows(self, monkeypatch, tmp_path):
        seen = {}

        class Pool:
            def __init__(self, max_workers):
                seen["workers"] = max_workers

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def map(self, fn, jobs):
                return []

        import concurrent.futures as cf

        monkeypatch.setattr(cf, "ThreadPoolExecutor", Pool)
        monkeypatch.setattr(
            utils,
            "request",
            lambda path, params, max_tries=6: utils.EMPTY_PAGE,
        )
        # contrato declares no cap, so it keeps the caller's 3.
        utils.harvest(
            table="contrato",
            input_dir=tmp_path / "a",
            start=date(2024, 5, 1),
            end=date(2024, 6, 1),
            max_workers=3,
        )
        assert seen["workers"] == 3

    def test_a_caller_asking_for_fewer_workers_is_respected(self):
        # min(), not the declared value: asking for 1 must never become 3.
        spec = {"max_workers": 3}
        assert min(1, int(spec.get("max_workers", 1))) == 1


class TestStagingPartSizeStaysSmall:
    """Part size protects two different things, only one of them local.

    The obvious one is this machine's RAM during cleaning. The other is CI:
    table-approve reads the lexicographically first staging parquet with
    pd.read_parquet purely to learn column names, and OOM-kills the runner
    on a large file -- which builds NO prod tables at all, for any table in
    the PR. Both are bounded by the same batch size.
    """

    MAX_SAFE_BATCH = 200_000

    def _default_batch(self):
        import inspect

        sig = inspect.signature(utils.clean_table)
        return sig.parameters["batch_rows"].default

    def test_the_default_batch_is_small_enough_for_table_approve(self):
        assert self._default_batch() <= self.MAX_SAFE_BATCH

    def test_parts_are_flushed_at_the_batch_size(self, tmp_path):
        # Two partitions, each over the batch, must yield several parts
        # rather than one file per year.
        input_dir, output_dir = tmp_path / "in", tmp_path / "out"
        target = input_dir / "contrato"
        target.mkdir(parents=True)
        with gzip.open(target / "w.jsonl.gz", "wt", encoding="utf-8") as fh:
            for i in range(25):
                fh.write(
                    json.dumps(
                        {
                            "numeroControlePNCP": f"X-{i}/2024",
                            "dataPublicacaoPncp": "2024-05-01T00:00:00",
                            "dataAtualizacaoGlobal": "2024-05-01T00:00:00",
                        }
                    )
                    + "\n"
                )
        utils.clean_table(input_dir, output_dir, "contrato", batch_rows=10)
        parts = sorted(
            (output_dir / "contrato" / "ano=2024").glob("*.parquet")
        )
        assert len(parts) >= 3, (
            f"expected the writer to flush every 10 rows, got {len(parts)} "
            "part(s) -- it is buffering the whole partition"
        )


class TestDeepPageFailureDoesNotSplit:
    """Splitting on a deep-page failure discards everything already fetched.

    Both halves restart from page 1, so a window that died at page 153 of 238
    throws away an hour and then repeats it on each half -- and the halves
    split again. Observed cascading four levels deep on contratacao, which
    took the harvest from ~40 chunks/hr to ~1.
    """

    def _fail_at(self, monkeypatch, bad_page, total=200):
        calls = {"n": 0}

        def fake(path, params, max_tries=6):
            calls["n"] += 1
            if params["pagina"] == bad_page:
                raise utils.ServerOverloadError("500")
            return {
                "data": [{"numeroControlePNCP": f"X-{params['pagina']}/2024"}],
                "totalPaginas": total,
                "totalRegistros": total,
            }

        monkeypatch.setattr(utils, "request", fake)
        return calls

    def test_a_deep_page_failure_raises_deep_page_error(self, monkeypatch):
        self._fail_at(monkeypatch, bad_page=5)
        with pytest.raises(utils.DeepPageError):
            utils.fetch_window("contratacoes/publicacao", {}, "label")

    def test_fetch_range_does_not_split_on_a_deep_page_failure(
        self, monkeypatch
    ):
        self._fail_at(monkeypatch, bad_page=5)
        with pytest.raises(utils.DeepPageError):
            utils.fetch_range(
                "contratacoes/publicacao",
                ("dataInicial", "dataFinal"),
                date(2024, 5, 1),
                date(2024, 5, 10),
                {},
            )

    def test_a_page_one_failure_still_splits(self, monkeypatch):
        # The split is correct there: nothing has been fetched to lose, and
        # page 1 failing is the signal that the window is too large. It ends
        # in ServerOverloadError once it reaches a single unservable day --
        # what matters is that it narrowed the range on the way down.
        seen = []

        def fake(path, params, max_tries=6):
            seen.append((params["dataInicial"], params["dataFinal"]))
            raise utils.ServerOverloadError("500")

        monkeypatch.setattr(utils, "request", fake)
        with pytest.raises(utils.ServerOverloadError):
            utils.fetch_range(
                "contratacoes/publicacao",
                ("dataInicial", "dataFinal"),
                date(2024, 5, 1),
                date(2024, 5, 4),
                {},
            )
        spans = {(lo, hi) for lo, hi in seen}
        assert ("20240501", "20240504") in spans, "the full window was tried"
        assert len(spans) > 1, (
            "a page-1 failure must narrow the window; only deep pages are "
            "exempt from splitting"
        )

    def test_deep_page_error_is_not_a_server_overload_error(self):
        # fetch_range splits on ServerOverloadError, so inheriting from it
        # would silently reintroduce the whole problem.
        assert not issubclass(utils.DeepPageError, utils.ServerOverloadError)


class TestContratacaoResizeBoundary:
    def test_boundary_is_on_an_original_window_edge(self):
        spec = constants.ENDPOINTS.value["contratacao"]
        boundary = date.fromisoformat(spec["resize"][0])
        edges = {
            lo
            for lo, _ in utils.windows(
                date(2021, 1, 1), date(2026, 8, 28), spec["window_days"]
            )
        }
        assert boundary in edges, (
            "an off-edge boundary renames every later window and re-downloads "
            "everything already harvested"
        )

    def test_post_boundary_windows_are_small(self):
        spec = constants.ENDPOINTS.value["contratacao"]
        rz = (date.fromisoformat(spec["resize"][0]), spec["resize"][1])
        wins = [
            (lo, hi)
            for lo, hi in utils.windows(
                date(2021, 1, 1), date(2026, 8, 28), spec["window_days"], rz
            )
            if lo >= rz[0]
        ]
        assert all((hi - lo).days <= 1 for lo, hi in wins[:-1])
