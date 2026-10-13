"""Tasks for br_rf_cno — thin, table_id-parametrized wrappers over the shared
`rf` crawler (`pipelines.crawler.rf`).
"""

from collections.abc import Callable

from pipelines.crawler.rf.tasks import (
    check_need_for_update,
    crawl,
    process_file,
)
from pipelines.datasets.br_rf_cno.constants import (
    CHUNKSIZE,
    COVERAGE,
    DATASET_ID,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas — ver constants.py.
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas
# parametrizadas por `table_id` (mesmo shape de `br_ibge_ipca`/`tasks.py`).
# Cada tabela roda check_update/extract_and_load de forma independente: o
# `extract_load_data` de cada uma baixa+extrai o `cno.zip` (~306 MB) de novo
# via `crawl` e processa só o próprio CSV via `process_file` — redundância
# aceita (ver banner de constants.py), já que `crawl`/`process_file`
# (`pipelines.crawler.rf.tasks`) não mudam: o zip inteiro é baixado de
# qualquer forma, só o recorte por tabela acontece depois, no
# `process_file`.
#
# `get_latest_update` não varia por tabela (mesmo zip, mesmo header
# `Last-Modified`) — `make_get_latest_update` recebe `table_id` só porque é
# o shape que `pipeline_factory` espera; o parâmetro fica sem uso ali.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        """Read the source zip's ``Last-Modified`` header (cheap, no download).

        Reuses `check_need_for_update` (`pipelines.crawler.rf.tasks`),
        unchanged from the old per-table flow
        (`pipelines.crawler.rf.flows._run_rf`): a `HEAD` on `cno.zip`, not a
        date derived from the file's contents — see README's *Acesso à
        fonte e o WAF*.

        Returns:
            `SourceInspection` with the resolved date as `reference_date`.
            No `extra_download_params`: the download URL is a fixed
            constant (`pipelines.crawler.rf.constants.constants.URLS`), not
            resolved per run, so there's nothing to round-trip through
            `extract_load_data`.
        """
        reference_date = check_need_for_update(dataset_id=DATASET_ID)
        return SourceInspection(reference_date=reference_date)

    return get_latest_update


def make_extract_load_data(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        """Download `cno.zip` again, process only this table's CSV.

        The source has no way to fetch a single table's slice — every
        table's `extract_and_load` downloads+extracts the whole zip on its
        own (see the banner above), then processes just its own file.

        Args:
            download_params: `{"reference_date": "<data_extracao>"}` —
                `reference_date` is `get_latest_update`'s resolved date,
                round-tripped through `check_update_and_dispatch`
                (`date.isoformat()`), as a plain `"YYYY-MM-DD"` string (no
                time component — `process_file`/`process_chunk` write the
                partition folder as `data=<date>`, and a datetime instead
                of a date there made `SAFE_CAST(data AS DATE)` return NULL
                in the incremental dbt models, so the filter never
                inserted; see README's *Causa raiz do congelamento*,
                2026-07-17 fix).

        Returns:
            `ExtractAndLoad` for `table_id`, with `dump_mode="append"`/
            `source_format="parquet"` — same as the old `_run_rf` (each run
            appends a full `data_extracao` snapshot; the incremental dbt
            models accumulate history, the staging dump itself doesn't need
            to retain it).

        Raises:
            RuntimeError: `process_file` didn't produce an output (file not
                recognized in `TABLES_RENAME`, or a processing failure —
                see logs).
        """
        reference_date = download_params["reference_date"]

        crawl(dataset_id=DATASET_ID, input_dir="input")

        output_path = process_file(
            dataset_id=DATASET_ID,
            table_id=table_id,
            input_dir="input",
            output_dir="output",
            partition_date=reference_date,
            chunksize=CHUNKSIZE,
        )
        if output_path is None:
            raise RuntimeError(
                f"process_file não gerou saída para a tabela {table_id!r} "
                f"de {DATASET_ID} — arquivo não reconhecido em "
                "TABLES_RENAME ou falha durante o processamento (ver logs)."
            )

        return ExtractAndLoad(
            coverage=COVERAGE.model_dump(),
            data_path=output_path,
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
