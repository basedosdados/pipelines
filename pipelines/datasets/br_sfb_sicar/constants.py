"""Constants for the br_sfb_sicar recurring pipeline (Prefect 3).

Cadastro Ambiental Rural (SICAR) — nine spatial theme tables refreshed per-UF on
a rolling basis. The schema source of truth is the committed architecture module
``models/br_sfb_sicar/code/architecture.py``; it is loaded here via importlib
(the sanctioned repo-relative pattern, mirroring how ``us_bls_cpi`` loads its
architecture CSVs) so the pipeline and the one-shot bootstrap cannot drift apart.
"""

import importlib.util
from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture module (single schema source of
# truth — column order, bigquery_type, THEME_POLYGON, DICIONARIO, NO_AREA_THEMES).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ARCH_PATH = (
    _REPO_ROOT / "models" / "br_sfb_sicar" / "code" / "architecture.py"
)


def _load_architecture():
    """Load the architecture module from its committed path (importlib).

    The module has no third-party imports, so it loads standalone. Keeping it as
    the single source of truth means the pipeline's cleaning transform, the dbt
    models, and the backend column registration all read the same column specs.
    """
    spec = importlib.util.spec_from_file_location(
        "br_sfb_sicar_architecture", _ARCH_PATH
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load architecture module from {_ARCH_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Module-level handle so utils.py can `from ...constants import architecture`.
architecture = _load_architecture()


class Constants(Enum):
    """Constants for the br_sfb_sicar pipeline.

    Lowercase-ish class name kept as ``Constants`` to match the pre-existing
    dataset module. Architecture-derived values (``TABLES``, ``THEME_POLYGON``,
    ``DICIONARIO``, ``NO_AREA_THEMES``) come straight from the loaded module.
    """

    DATASET_ID = "br_sfb_sicar"

    # Nine theme tables, area_imovel first — it is the poll/commit anchor (the
    # per-UF release date that drives the source-update poll comes from it). The
    # `dicionario` table is static and is NOT refreshed by this pipeline.
    THEME_TABLES = [
        "area_imovel",
        "app",
        "reserva_legal",
        "vegetacao_nativa",
        "area_consolidada",
        "area_pousio",
        "uso_restrito",
        "servidao_administrativa",
        "hidrografia",
    ]

    # The table whose max per-UF release date is polled for source freshness.
    ANCHOR_TABLE = "area_imovel"

    # Architecture (schema source of truth).
    TABLES = architecture.TABLES
    THEME_POLYGON = architecture.THEME_POLYGON
    DICIONARIO = architecture.DICIONARIO
    NO_AREA_THEMES = architecture.NO_AREA_THEMES

    # Polygon enum value (tipoBase) per output table — passed to SICAR's
    # download_state. Same mapping as THEME_POLYGON, duplicated here as the
    # download-facing name.
    TABLE_TO_POLYGON = architecture.THEME_POLYGON

    # All 27 UFs (the two-letter codes SICAR's State enum accepts).
    UF_SIGLAS = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    # The CAR download server is flaky (frequent read timeouts); retry hard.
    DOWNLOAD_MAX_RETRIES = 8
    DOWNLOAD_TRIES = 25  # captcha attempts per download_state call
