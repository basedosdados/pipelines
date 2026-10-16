"""
Constant values for br_bcb_taxa_cambio.
"""

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)

DATASET_ID = "br_bcb_taxa_cambio"

# Tabela única do dataset — migrada pro pipeline em estágios (staged
# pipeline), substituindo o antigo flow monolítico em `flows.py` (que segue
# existindo em `crawler/bcb_taxa_cambio/`, só como fonte das tasks/utils
# reaproveitadas aqui).
TAXA_CAMBIO_TABLE_ID = "taxa_cambio"

# Mesma coverage que o flow monolítico antigo passava pra
# `register_table_materialization_task`: diária, com a parte recente BD pro
# (defasagem de seis meses) e o histórico livre.
COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_cotacao"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
