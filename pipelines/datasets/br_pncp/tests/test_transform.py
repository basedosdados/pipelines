"""Regression tests for the br_pncp transform.

Each case here is a failure that would be *silent* — the pipeline would run
green and the table would be wrong. They run offline against fixtures shaped
like the real PNCP payloads.
"""

from __future__ import annotations

import gzip
import json

import pyarrow.dataset as ds

from pipelines.datasets.br_pncp import utils


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
