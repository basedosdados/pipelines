"""
Constant values for br_bcb_taxa_selic.
"""

from pipelines.utils.metadata.domain import AllBdpro, DateFormat, DateOnly

DATASET_ID = "br_bcb_taxa_selic"
TABLE_ID = "taxa_selic"

COVERAGE = AllBdpro(
    date_column=DateOnly(col="data"),
    date_format=DateFormat.YEAR_MD,
)
