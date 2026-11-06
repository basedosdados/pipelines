"""Prefect 3 tasks for br_cgu_despesas_publicas — thin wrappers over utils.py.

check_update/extract_and_load pair per table (execucao, favorecido): `probe_source`
is a single HEAD request (no download) against the table's current-month URL, so
check_update stays cheap even though extract_and_load re-pulls a whole trailing
window. `months_back`/`full_refresh` are accepted as optional keys in
`download_params` (defaulting to the same values the old monolithic flow used) so a
manual direct trigger of the extract_and_load deployment can still ask for a wider
or full backfill — check_update itself never sets them, same trade-off as
`br_bcb_taxa_cambio`'s `anos` override.
"""

import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_despesas_publicas.constants import (
    DATASET_ID,
)
from pipelines.datasets.br_cgu_despesas_publicas.utils import (
    clean_all,
    download_all,
    month_range,
    probe_latest,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)


@task(retries=2, retry_delay_seconds=300)
def probe_source(table: str = "execucao") -> str:
    """Return when the portal last regenerated the current month, ``YYYY-MM-DD``.

    One HEAD request. Unlike ``orcamento-despesa``, each month here carries its
    own ``Last-Modified``, so this timestamp speaks only for the current month —
    which is the one that moves on every release and therefore the right
    freshness signal for a scheduled run.

    Retries are spaced five minutes apart because the failure worth retrying is
    an AWS WAF rate block, which a fast retry only deepens.
    """
    return probe_latest(table=table)


@task(retries=2, retry_delay_seconds=600)
def download_despesas(
    work_dir: str,
    months_back: int = 6,
    full_refresh: bool = False,
    table: str = "execucao",
) -> dict:
    """Download the months to refresh into ``<work_dir>/input``.

    Args:
        work_dir: Scratch directory for this run.
        months_back: Size of the trailing window to refresh. The source restates
            closed months, but re-pulling all 150+ of them costs ~25 minutes and
            ~10 GB, so a scheduled run refreshes only the tail.
        full_refresh: Pull every month from 2014-01 instead. Use when the source
            has restated older months and the tail window would miss it.

    Returns:
        ``{"input_dir": str, "months": [[year, month], ...]}``.
    """
    input_dir = Path(work_dir) / "input"
    if full_refresh:
        wanted = month_range()
    else:
        now = datetime.now(UTC)
        start_index = now.year * 12 + (now.month - 1) - max(months_back - 1, 0)
        first = (start_index // 12, start_index % 12 + 1)
        wanted = month_range(first=first)
    months = download_all(input_dir, months=wanted, table=table)
    return {"input_dir": str(input_dir), "months": [list(m) for m in months]}


@task
def clean_despesas(
    work_dir: str, input_dir: str, months: list, table: str = "execucao"
) -> dict:
    """Clean the downloaded months into hive-partitioned all-STRING parquet.

    Returns:
        ``{"path": str, "rows": {"YYYY-MM": n}, "total": n, "max_period": str}``.
    """
    result = clean_all(
        Path(input_dir),
        Path(work_dir) / "output",
        [tuple(m) for m in months],
        table=table,
    )
    return {**result, "path": str(result["path"])}


# ──────────────────────────────────────────────────────────────────────────────
# As 2 tabelas (execucao, favorecido) — ver constants.py
#
# `make_get_latest_update`/`make_extract_load_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 2 tabelas, só muda o endpoint e a
# posição/formato da coluna de período dentro de `utils.PRODUCTS`.
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        max_modified = probe_source(table=table_id)
        return SourceInspection(
            reference_date=date.fromisoformat(max_modified),
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Opcionais, só usados num disparo manual direto do deployment de
        # extract_and_load — check_update nunca os preenche via
        # extra_download_params (ver banner do módulo).
        months_back = download_params.get("months_back", 6)
        full_refresh = download_params.get("full_refresh", False)

        # Não limpo ao final (sem shutil.rmtree/finally): `data_path` aponta
        # pra dentro de `work_dir` e só é lido (via upload_to_gcs) logo depois
        # deste retorno, dentro do mesmo flow run — mesmo trade-off de
        # au_ato_abr/au_geoscape_gnaf/br_cgu_sancoes.
        work_dir = tempfile.mkdtemp(
            prefix=f"br_cgu_despesas_publicas_{table_id}_"
        )
        downloaded = download_despesas(
            work_dir=work_dir,
            months_back=months_back,
            full_refresh=full_refresh,
            table=table_id,
        )
        result = clean_despesas(
            work_dir=work_dir,
            input_dir=downloaded["input_dir"],
            months=downloaded["months"],
            table=table_id,
        )
        print(
            f"cleaned {len(result['rows'])} months, {result['total']:,} rows "
            f"(through {result['max_period']})"
        )

        return ExtractAndLoad(
            data_path=result["path"],
            dump_mode="append",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
