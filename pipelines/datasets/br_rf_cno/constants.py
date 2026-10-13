"""Constant values for br_rf_cno.

**CNO** (Cadastro Nacional de Obras), Receita Federal. The source publishes a
single non-decomposable ZIP (``cno.zip``, ~306 MB) containing the CSVs for
all 4 published tables (microdados, vinculos, areas, cnaes) in one pass —
see ``pipelines/datasets/br_rf_cno/README.md`` for the WAF access
workarounds and the per-table data quality notes. ``totais`` has a rename
entry in ``pipelines.crawler.rf.constants`` but no flow/dbt model, and
``dicionario`` is a static table with no CSV in the zip — neither is part
of this pipeline.

Each table has its own check_update/extract_and_load pair (the repo-wide
one-flow-per-table rule — see ``pipeline_factory`` in
``pipelines.utils.stage_dispatch``, used the same way by ``br_ibge_ipca``/
``br_ms_cnes``), even though that means re-downloading the same zip up to 4x
per cycle: accepted redundancy for uniformity, since the 4 tables don't
genuinely depend on each other (no cross-table dbt test tying them
together).
"""

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro

DATASET_ID = "br_rf_cno"

# As 4 tabelas publicadas (ver README's arquivo->tabela map). Exclui
# `totais` (tem rename em `pipelines.crawler.rf.constants` mas sem
# flow/modelo dbt) e `dicionario` (tabela estática, sem CSV no zip).
MICRODADOS_TABLE_ID = "microdados"
VINCULOS_TABLE_ID = "vinculos"
AREAS_TABLE_ID = "areas"
CNAES_TABLE_ID = "cnaes"

ALL_TABLES = [
    MICRODADOS_TABLE_ID,
    VINCULOS_TABLE_ID,
    AREAS_TABLE_ID,
    CNAES_TABLE_ID,
]

# Mesmo chunksize que os flows antigos por tabela passavam pro
# `process_file`.
CHUNKSIZE = 100000

# Mesma coverage pras 4 tabelas: incremental, particionada por
# `data_extracao` (DATE) — idêntica ao
# `PartBdpro(date_column=DateOnly(col="data_extracao"),
# date_format=DateFormat.YEAR_MD)` que o `_run_rf` antigo
# (`pipelines/crawler/rf/flows.py`) passava pra toda table_id — mesma
# coluna de data, mesmo `free_lag` default (6 meses), não é uma decisão
# nova.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)
