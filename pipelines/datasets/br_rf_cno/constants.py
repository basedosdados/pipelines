"""Constants for the br_rf_cno recurring pipeline (Prefect 3).

**CNO** (Cadastro Nacional de Obras), Receita Federal. The source publishes a
single non-decomposable ZIP (``cno.zip``, ~306 MB) containing the CSVs for
all 4 published tables (microdados, vinculos, areas, cnaes) in one pass — see
``pipelines/datasets/br_rf_cno/README.md`` for the WAF access workarounds and
the per-table data quality notes. ``totais`` has a rename entry in
``pipelines.crawler.rf.constants`` but no flow/dbt model, and ``dicionario``
is a static table with no CSV in the zip — neither is part of this pipeline.

Every table shares the same coverage shape: incremental, partitioned by
``data_extracao`` (DATE) — unchanged from the old per-table flow
(``pipelines.crawler.rf.flows._run_rf``, which passed the same
``PartBdpro(...)`` to ``register_table_materialization_task`` for every
table_id).
"""

from enum import Enum

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro


class constants(Enum):
    """Constants for the br_rf_cno pipeline.

    Lowercase class name follows the repo-wide convention for dataset
    constant enums.
    """

    DATASET_ID = "br_rf_cno"

    # microdados is the main/largest table (basic cadastral data of the
    # obra) — check_update's poll is anchored on it.
    CORE_TABLE = "microdados"

    # The 4 published, flow-backed tables (see README's arquivo->tabela
    # map). Excludes `totais` (no flow/dbt model) and `dicionario` (static
    # table, no CSV in the zip).
    ALL_TABLES = ["microdados", "vinculos", "areas", "cnaes"]

    # Same chunksize the old per-table flows (`_cno_flow` in the previous
    # `flows.py`) passed to `process_file`.
    CHUNKSIZE = 100000


# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
# Identical to the `PartBdpro(date_column=DateOnly(col="data_extracao"),
# date_format=DateFormat.YEAR_MD)` the old `_run_rf`
# (`pipelines/crawler/rf/flows.py`) passed for every table_id — same date
# column, same default `free_lag` (6 months), not a new decision.
_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)
COVERAGE = {table: _PART_BDPRO for table in constants.ALL_TABLES.value}
