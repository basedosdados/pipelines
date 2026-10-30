"""Constants for the br_ibama_fiscalizacao recurring pipeline (Prefect 3).

Ibama publishes its enforcement records on the open-data portal as full-history
files regenerated every day: the autos de infração (SIFISC, one CSV per year in a
zip), the termos de embargo (one CSV) and the SICAFI fine series that supplies
the debt status of each auto. Every run downloads all three and overwrites the
staging tables (``dump_mode="overwrite"``).

The cleaning transform lives in ``utils.py``; the one-shot onboarding scripts in
``models/br_ibama_fiscalizacao/code/`` import it from there.
"""

from enum import Enum

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
)


class constants(Enum):
    """Constants for the br_ibama_fiscalizacao pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "br_ibama_fiscalizacao"

    BLOB_BASE = "https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados"

    # Source file -> (path under BLOB_BASE, local name under input/). The zips
    # are extracted into a directory named after the local name's stem.
    # The CKAN record for the embargo file points at TERMOS/TERMO_EMBARGO/, which
    # 404s. The live path is TERMOS_DE_EMBARGO/TERMO_EMBARGO/.
    SOURCE_FILES = {
        "auto_infracao": (
            "SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip",
            "ai.zip",
        ),
        "termo_de_embargo": (
            "TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv",
            "termo_de_embargo.csv",
        ),
        "multas_bens_tutelados": (
            "MULTAS_AMBIENTAIS_DISTRIBUIDAS_POR_BENS_TUTELADOS"
            "/multas_ambientais_distribuidas_por_bens_tutelados_csv.zip",
            "mt.zip",
        ),
    }

    # Tables built from staging. `dicionario` is a dbt model derived from these
    # two, so it has no staging table and is never uploaded.
    DATA_TABLES = ["auto_infracao", "area_embargada"]

    # Every dbt model, in build order: the dictionary reads both fact models.
    ALL_TABLES = ["auto_infracao", "area_embargada", "dicionario"]


# Coverage spec per table, read by the flow and by the onboarding's
# register_metadata.py, so the static registration and the flow cannot disagree
# on which tables are part_bdpro.
#
# Both fact tables refresh more often than monthly, so both paywall their most
# recent six months, keyed on the date of the act. Each run recomputes
# free_end = max(<date>) - 6 months, rewrites both DateTimeRanges and re-issues
# the BigQuery Row Access Policies, so the window slides on its own.
#
# The window is only as sane as max(<date>): Ibama prints a few embargo dates
# centuries ahead (2925-09-25), which would push free_end past every row and
# paywall nothing. The dbt models null any act date later than its extraction
# date for that reason.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. register_metadata.py creates the pro one.
# `dicionario` has no date column and takes no spec.
COVERAGE: dict[str, PartBdpro] = {
    "auto_infracao": PartBdpro(
        date_column=DateOnly(col="data_auto"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "area_embargada": PartBdpro(
        date_column=DateOnly(col="data_embargo"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
}
