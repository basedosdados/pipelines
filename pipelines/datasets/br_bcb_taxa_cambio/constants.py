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
TAXA_CAMBIO_TABLE_ID = "taxa_cambio"

COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_cotacao"),
    date_format=DateFormat.YEAR_MD,
    free_lag=FreeLag(unit="months", value=6),
)
