"""Constants for the fr_meteofrance recurring pipeline (Prefect 3).

Météo-France SYNOP surface observations and the 1991-2020 climate normals.
See models/fr_meteofrance/CLAUDE.md for the full design and the source quirks.

The legacy ``donneespubliques.meteofrance.fr`` host is dead; everything is
served from data.gouv.fr and the OVH object store below.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the fr_meteofrance pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/fr_meteofrance/code/``, which are the schema source of truth for
    both this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "fr_meteofrance"

    # OVH object store; publicly readable, no key, no special User-Agent needed.
    SYNOP_BASE = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/OBS/SYNOP"
    FICHE_BASE = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/REF_STATION"

    # The SYNOP archive starts in 1996; the current year's file is rewritten
    # daily, every earlier year is frozen.
    SYNOP_FIRST_YEAR = 1996

    # Geographic register of the SYNOP stations (altitude + opening date).
    POSTES_GEOJSON = "postes_synop.geojson"
    # Register of the stations that have a published climatological sheet.
    FICHES_GEOJSON = "liste_fiches_clim.geojson"

    # Tables refreshed by the daily flow (only the observation table moves daily).
    DAILY_TABLES = ["synop"]
    # Tables refreshed by the monthly flow.
    MONTHLY_TABLES = [
        "station_synop",
        "normale_climatologique",
        "station_climatologique",
        "dicionario",
    ]
    ALL_TABLES = [
        "synop",
        "station_synop",
        "normale_climatologique",
        "station_climatologique",
        "dicionario",
    ]

    # Tables refreshed by the climatological-archive flow. The station register
    # is rebuilt from every source file, not just the refreshed slice, so it is
    # listed here rather than derived from whichever series ran.
    CLIMATOLOGIE_BASE_TABLES = ["poste", "mensuelle", "quotidienne"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "fr_meteofrance" / "code" / "architecture"
    )
