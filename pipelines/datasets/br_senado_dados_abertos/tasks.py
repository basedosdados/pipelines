"""Tasks for br_senado_dados_abertos — Prefect 3, staged pipeline.

18 independent check_update/extract_and_load pairs, one per table_id (see
``flows.py``'s module banner for why no table is grouped with another despite
some of them sharing a crawl). ``make_get_latest_update``/
``make_extract_load_data`` are fabricas parametrizadas por ``table_id`` via
``pipeline_factory`` (``pipelines.utils.stage_dispatch``), same shape as
``br_cgu_servidores_executivo_federal.tasks`` — each one dispatches to the
Group A or Group B logic below depending on which group ``table_id`` belongs
to.

Reuses the cleaning transforms in ``senado_clean.py`` (``sc.clean_*``)
unchanged — no business logic is rewritten here, only reorganized into one
independent call per table.

Group A (10 dimensions — ``constants.DIMS``): cadastro-style tables with no
publication date to poll. ``get_latest_update`` is table-blind (same function
for all 10, wired in via ``lambda _: ...``, same pattern as
``au_ato_abr.tasks``): it always returns today as ``reference_date`` with
``compare_against="table_update"``, since there is no real baseline to
compare against — this is "always full-rebuild" by nature, not a bug (see
``constants.GROUP_A_COVERAGE``'s docstring for how materializing with
``NonHistorical`` leaves the onboarding ``Coverage`` untouched).

Group B (8 time series — ``constants.PARTITIONED``): real date signal.
``get_latest_update`` re-extracts the routine refresh window
(``recent_window(REFRESH_PRIOR_YEARS)``, same window used by
``extract_load_data``) and reads the max of the table's date column straight
out of the cleaned dataframe.

Crawl acoplamento (same source call feeds more than one table) is NOT a
reason to group tables into one flow here (see ``flows.py``) — each table's
extract_and_load (and check_update, for Group B) redundantly reruns whatever
upstream call it needs and keeps only its own half:
  - ``votacao_parlamentar`` reruns ``sc.clean_votacao(years)`` and discards
    the ``votacao`` half.
  - ``votacao_comissao_parlamentar`` reruns ``sc.clean_comissao()`` (for
    ``siglas``) and ``sc.clean_votacao_comissao(siglas)``, discarding the
    ``votacao_comissao`` half.
  - ``senador_mandato``/``senador_filiacao``/``senador_comissao``/
    ``senador_cargo`` each rerun ``sc.clean_senador()`` solely to recompute
    ``codes``.
Same trade-off already taken elsewhere in this migration for a shared-crawl,
multi-table source (``au_ato_abr``, ``us_bls_oes``): uniform
one-flow-per-table now outweighs the saved bandwidth/CPU.
"""

import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

from pipelines.datasets.br_senado_dados_abertos import senado_clean as sc
from pipelines.datasets.br_senado_dados_abertos.constants import (
    DATASET_ID,
)
from pipelines.datasets.br_senado_dados_abertos.constants import (
    constants as senado_constants,
)
from pipelines.datasets.br_senado_dados_abertos.utils import (
    DIMS,
    recent_window,
    write_table,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

_REFRESH_PRIOR_YEARS = senado_constants.REFRESH_PRIOR_YEARS.value

# Sentinel for a Group B routine window that happens to come back empty (e.g.
# a dry spell with zero committee votes in the current + prior year) — older
# than any real `Coverage` baseline, so the poll correctly reads it as "no new
# data" instead of raising on a missing max date.
_NO_NEW_DATA_DATE = date(1900, 1, 1)


def _senador_codes() -> list[str]:
    """`id_senador` of every senator, same filter `utils.clean_all` uses."""
    df = sc.clean_senador()
    return [c for c in df["id_senador"].tolist() if c]


def _comissao_siglas() -> list[str]:
    """`sigla_comissao` of every committee, same filter `utils.clean_all` uses."""
    df = sc.clean_comissao()
    return [x for x in df["sigla_comissao"].dropna().unique().tolist() if x]


# ──────────────────────────────────────────────────────────────────────────────
# Group A — 10 dimensions (see module banner).
# ──────────────────────────────────────────────────────────────────────────────

_GROUP_A_CLEANERS: dict[str, Callable[[], pd.DataFrame]] = {
    "partido": sc.clean_partido,
    "bloco": sc.clean_bloco,
    "lideranca": sc.clean_lideranca,
    "comissao": sc.clean_comissao,
    "mesa": sc.clean_mesa,
    "senador": sc.clean_senador,
}
_GROUP_A_SENADOR_SUBTABLE_CLEANERS: dict[
    str, Callable[[list[str]], pd.DataFrame]
] = {
    "senador_mandato": sc.clean_senador_mandato,
    "senador_filiacao": sc.clean_senador_filiacao,
    "senador_comissao": sc.clean_senador_comissao,
    "senador_cargo": sc.clean_senador_cargo,
}


def _get_latest_update_group_a() -> SourceInspection:
    """Table-blind check for every Group A dimension (see module banner).

    No publication date exists to poll — this cadastro-style data has no
    historical signal, only a current state. Always returns today so the
    extract_and_load is always dispatched; ``compare_against="table_update"``
    reads `Table.Update.latest`, which the previous run's
    `NonHistorical` materialization stamped with that run's own BigQuery
    `last_modified_time` — always older than today, by construction.
    """
    return SourceInspection(
        reference_date=date.today(), compare_against="table_update"
    )


def make_extract_load_data_group_a(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        # Not cleaned up afterwards (no `shutil.rmtree`/`finally`): the
        # returned `ExtractAndLoad.data_path` points inside `work_dir` and is
        # only read (via `upload_to_gcs`) right after this returns, inside the
        # same flow run — same trade-off as `au_ato_abr.tasks.
        # make_extract_load_data`.
        work_dir = tempfile.mkdtemp(
            prefix=f"br_senado_dados_abertos_{table_id}_"
        )
        out_root = str(Path(work_dir) / "output")

        if table_id in _GROUP_A_CLEANERS:
            df = _GROUP_A_CLEANERS[table_id]()
        else:
            df = _GROUP_A_SENADOR_SUBTABLE_CLEANERS[table_id](_senador_codes())

        table_dir = write_table(out_root, table_id, df)
        return ExtractAndLoad(
            data_path=table_dir,
            source_format="parquet",
        )

    return extract_load_data


# ──────────────────────────────────────────────────────────────────────────────
# Group B — 8 time series (see module banner).
# ──────────────────────────────────────────────────────────────────────────────


def _votacao_votacao(years: range) -> pd.DataFrame:
    return sc.clean_votacao(years)[0]


def _votacao_votacao_parlamentar(years: range) -> pd.DataFrame:
    return sc.clean_votacao(years)[1]


def _votacao_comissao_h(_years: range) -> pd.DataFrame:
    return sc.clean_votacao_comissao(_comissao_siglas())[0]


def _votacao_comissao_p(_years: range) -> pd.DataFrame:
    return sc.clean_votacao_comissao(_comissao_siglas())[1]


# table_id -> (builder(years) -> df, date column to extract the max of).
_GROUP_B_BUILDERS: dict[str, tuple[Callable[[range], pd.DataFrame], str]] = {
    "votacao": (_votacao_votacao, "data_sessao"),
    "votacao_parlamentar": (_votacao_votacao_parlamentar, "data_sessao"),
    "votacao_orientacao_bancada": (sc.clean_orientacao, "data_votacao"),
    "processo": (sc.clean_processo, "data_apresentacao"),
    "relatoria": (sc.clean_relatoria, "data_designacao"),
    "votacao_comissao": (_votacao_comissao_h, "data_reuniao"),
    "votacao_comissao_parlamentar": (_votacao_comissao_p, "data_reuniao"),
    "discurso": (sc.clean_discurso, "data_sessao"),
}


def make_get_latest_update_group_b(
    table_id: str,
) -> Callable[[], SourceInspection]:
    builder, date_col = _GROUP_B_BUILDERS[table_id]

    def get_latest_update() -> SourceInspection:
        years = recent_window(_REFRESH_PRIOR_YEARS)
        df = builder(years)
        max_date = df[date_col].dropna().max() if len(df) else None
        reference_date = (
            date.fromisoformat(max_date) if max_date else _NO_NEW_DATA_DATE
        )
        return SourceInspection(
            reference_date=reference_date, compare_against="coverage"
        )

    return get_latest_update


def make_extract_load_data_group_b(
    table_id: str,
) -> Callable[[dict], ExtractAndLoad]:
    builder, _date_col = _GROUP_B_BUILDERS[table_id]

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        years = recent_window(_REFRESH_PRIOR_YEARS)
        df = builder(years)

        work_dir = tempfile.mkdtemp(
            prefix=f"br_senado_dados_abertos_{table_id}_"
        )
        out_root = str(Path(work_dir) / "output")
        table_dir = write_table(out_root, table_id, df)

        return ExtractAndLoad(
            data_path=table_dir,
            source_format="parquet",
        )

    return extract_load_data


# ──────────────────────────────────────────────────────────────────────────────
# pipeline_factory wiring — one `make_pipeline(table_id)` per group.
# ──────────────────────────────────────────────────────────────────────────────

make_pipeline_group_a = pipeline_factory(
    DATASET_ID,
    # `get_latest_update` doesn't vary by table (see module banner) — wired in
    # via `lambda _: ...` rather than a real per-table factory, same pattern
    # as `au_ato_abr.tasks`.
    lambda _table_id: _get_latest_update_group_a,
    make_extract_load_data_group_a,
)

make_pipeline_group_b = pipeline_factory(
    DATASET_ID,
    make_get_latest_update_group_b,
    make_extract_load_data_group_b,
)

# Sanity: every Group A/B table_id used above must belong to its own group —
# keeps `_GROUP_A_CLEANERS`/`_GROUP_A_SENADOR_SUBTABLE_CLEANERS`/
# `_GROUP_B_BUILDERS` in sync with `constants.DIMS` if either ever changes.
assert set(_GROUP_A_CLEANERS) | set(_GROUP_A_SENADOR_SUBTABLE_CLEANERS) == set(
    DIMS
)
