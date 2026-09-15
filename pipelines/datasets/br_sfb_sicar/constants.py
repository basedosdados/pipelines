"""Constant values for br_sfb_sicar."""

from pipelines.crawler.sfb_sicar.constants import Constants

DATASET_ID = Constants.DATASET_ID.value

# Nine theme tables, area_imovel first — it is the poll/commit anchor (the
# per-UF release date that drives the source-update poll comes from it).
# `dicionario` is static and not refreshed by this pipeline.
THEME_TABLES = Constants.THEME_TABLES.value
ANCHOR_TABLE = Constants.ANCHOR_TABLE.value
TABLE_TO_POLYGON = Constants.TABLE_TO_POLYGON.value
UF_SIGLAS = Constants.UF_SIGLAS.value
DOWNLOAD_TRIES = Constants.DOWNLOAD_TRIES.value
DOWNLOAD_MAX_RETRIES = Constants.DOWNLOAD_MAX_RETRIES.value
