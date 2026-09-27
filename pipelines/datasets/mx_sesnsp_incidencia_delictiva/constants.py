"""Constants for the mx_sesnsp_incidencia_delictiva recurring pipeline (Prefect 3).

Mexican monthly crime counts from SESNSP (Secretariado Ejecutivo del Sistema
Nacional de Seguridad Pública). The pipeline refreshes only the four
new-methodology tables that gain a month on every release
(``municipio_delitos``, ``estatal_delitos``, ``estatal_victimas``,
``municipio_victimas``); the three ``*_2015_2025`` legacy tables are frozen and
never touched here.

See models/mx_sesnsp_incidencia_delictiva/CLAUDE.md for the full design, the
SharePoint download trick, and the label→token mapping.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the mx_sesnsp_incidencia_delictiva pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/mx_sesnsp_incidencia_delictiva/code/``, which are the schema source
    of truth for both this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "mx_sesnsp_incidencia_delictiva"

    # SESNSP landing page. It is behind an Imperva bot challenge and JS-rendered —
    # plain requests get an 1850-byte "Challenge Validation" shell. A real
    # headless Chrome (selenium + the in-image google-chrome-stable) passes the
    # challenge natively and renders the SharePoint share links into the DOM.
    GOB_MX_URL = (
        "https://www.gob.mx/sesnsp/acciones-y-programas/"
        "datos-abiertos-de-incidencia-delictiva"
    )
    # Anonymous SharePoint download form. The share token rotates every monthly
    # release, so it is scraped from the landing page each run — never hardcoded.
    # The SharePoint host is Microsoft (not behind Imperva), so the zip downloads
    # with plain requests + a browser User-Agent.
    SHAREPOINT_DOWNLOAD = (
        "https://sspcgob-my.sharepoint.com/personal/cni_sspc_gob_mx/"
        "_layouts/15/download.aspx?share={token}"
    )
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # The four ongoing (new-methodology, 2026→) tables this pipeline refreshes.
    # slug -> (municipal, victimas) — drives which id/geometry columns the melt
    # keeps.
    ONGOING_TABLES = {
        "municipio_delitos": (True, False),
        "estatal_delitos": (False, False),
        "estatal_victimas": (False, True),
        "municipio_victimas": (True, True),
    }
    ALL_TABLES = [
        "municipio_delitos",
        "estatal_delitos",
        "estatal_victimas",
        "municipio_victimas",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "mx_sesnsp_incidencia_delictiva"
        / "code"
        / "architecture"
    )
