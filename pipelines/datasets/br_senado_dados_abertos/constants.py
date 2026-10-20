"""Constants for the br_senado_dados_abertos recurring pipeline (Prefect 3).

Senado Federal legislative open data (senators, votes, bills, committees,
parties, blocs, leaderships, Directing Board), sourced from the public
Legislative Open Data API. See models/br_senado_dados_abertos/ for the design
and the architecture source of truth (code/architecture_spec.py).

Migrated to the staged pipeline (check_update -> extract_and_load ->
build_and_promote, see ``pipelines.utils.stage_dispatch``) — 18 independent
table_id pairs, see ``flows.py``'s module banner for the architecture
rationale. Table ids below are plain literals (not Enum members) so
``tasks.py``/``flows.py`` can import them directly, one per
check_update/extract_and_load pair.
"""

from enum import Enum
from pathlib import Path

from pipelines.datasets.br_senado_dados_abertos.utils import (
    ALL_TABLES,
    DIMS,
    PARTITIONED,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_senado_dados_abertos pipeline (lowercase per the
    repo-wide convention for dataset constant enums)."""

    DATASET_ID = "br_senado_dados_abertos"

    ALL_TABLES = ALL_TABLES
    DIMS = DIMS
    PARTITIONED = PARTITIONED

    # Routine runs re-extract the current year plus this many prior years for
    # the time-series tables (catches late edits); older partitions are left
    # untouched in staging. Dimensions are always rebuilt in full.
    REFRESH_PRIOR_YEARS = 1

    # Architecture source of truth (column order + types), also used by the
    # one-shot onboarding under models/.
    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "br_senado_dados_abertos" / "code"
    )


DATASET_ID = constants.DATASET_ID.value

# ──────────────────────────────────────────────────────────────────────────────
# Group A — 10 dimensions (== DIMS above): cadastro-style tables with no
# publication date to poll — "always full-rebuild" by nature, not a bug (see
# tasks.py `get_latest_update_group_a`). Same members as `DIMS`, named
# individually for direct import in flows.py.
# ──────────────────────────────────────────────────────────────────────────────
PARTIDO_TABLE_ID = "partido"
BLOCO_TABLE_ID = "bloco"
LIDERANCA_TABLE_ID = "lideranca"
COMISSAO_TABLE_ID = "comissao"
MESA_TABLE_ID = "mesa"
SENADOR_TABLE_ID = "senador"
SENADOR_MANDATO_TABLE_ID = "senador_mandato"
SENADOR_FILIACAO_TABLE_ID = "senador_filiacao"
SENADOR_COMISSAO_TABLE_ID = "senador_comissao"
SENADOR_CARGO_TABLE_ID = "senador_cargo"

# ──────────────────────────────────────────────────────────────────────────────
# Group B — 8 time series (== PARTITIONED above): real date signal, BD Pro
# rolling window.
# ──────────────────────────────────────────────────────────────────────────────
VOTACAO_TABLE_ID = "votacao"
VOTACAO_PARLAMENTAR_TABLE_ID = "votacao_parlamentar"
VOTACAO_ORIENTACAO_BANCADA_TABLE_ID = "votacao_orientacao_bancada"
PROCESSO_TABLE_ID = "processo"
RELATORIA_TABLE_ID = "relatoria"
VOTACAO_COMISSAO_TABLE_ID = "votacao_comissao"
VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID = "votacao_comissao_parlamentar"
DISCURSO_TABLE_ID = "discurso"

# ──────────────────────────────────────────────────────────────────────────────
# Coverage — Group A: `NonHistorical` (see `pipelines.utils.metadata.domain`).
# `register_table_materialization`/`sync_table_coverage` special-case this
# tier: they update `Table.Update.latest` from the BigQuery table's own
# `last_modified_time` and return immediately, WITHOUT touching
# `Coverage.DateTimeRange` at all — the one mechanism the capsule already
# supports for "materialize without re-registering coverage". The old
# monolithic flow never called `register_table_materialization_task` for
# these 10 tables at all (see the removed `_COVERAGE` dict, which only ever
# covered the 8 Group B tables), so onboarding's original `Coverage` stays
# exactly as it was — this is each table's first `Table.Update` baseline, not
# a change to a previously decided value. Same precedent already used
# elsewhere in this migration for a dimension/no-date table: `au_ato_abr`'s
# `dicionario`, `br_cgu_sancoes`'s `dicionario`, `br_cvm_administradores_
# carteira`'s `responsavel`.
GROUP_A_COVERAGE = NonHistorical()

# Coverage — Group B: exact `PartBdpro` specs already armed in production,
# reused unchanged from the pre-migration `flows.py` `_COVERAGE` dict. All
# eight paywall their most recent 6 months; `register_table_materialization_
# task` recomputes `free_end = source_end - free_lag` and re-issues the Row
# Access Policies every run, so the window slides on its own.
_PART_BDPRO_6M = FreeLag(unit="months", value=6)
COVERAGE = {
    VOTACAO_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    VOTACAO_PARLAMENTAR_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    VOTACAO_ORIENTACAO_BANCADA_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_votacao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    PROCESSO_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_apresentacao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    RELATORIA_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_designacao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    VOTACAO_COMISSAO_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_reuniao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_reuniao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
    DISCURSO_TABLE_ID: PartBdpro(
        date_column=DateOnly(col="data_sessao"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_PART_BDPRO_6M,
    ),
}
