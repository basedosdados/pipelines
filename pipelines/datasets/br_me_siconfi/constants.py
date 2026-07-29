"""Constants for the br_me_siconfi recurring pipeline (Prefect 3).

SICONFI (Sistema de Informações Contábeis e Fiscais do Setor Público
Brasileiro), published by the Tesouro Nacional. 19 annual tables across three
government levels (Brasil / UF / município). See models/br_me_siconfi/CLAUDE.md
for the full onboarding design.

The recurring pipeline reuses the validated download + cleaning transform under
``models/br_me_siconfi/code/`` (download_api.py, build.py, tables_final/*,
crosswalk/*), so the transform stays single-sourced. This module only declares
the identifiers, the table set, the trailing-window default, and the GCS cache
location the pipeline layers on top.
"""

from enum import Enum
from pathlib import Path

# pipelines/datasets/br_me_siconfi/constants.py -> repo root is parents[3].
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_me_siconfi pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``CODE_DIR`` points at the validated one-shot bootstrap under
    ``models/br_me_siconfi/code`` — the pipeline imports its download primitives
    and drives its per-year builders rather than duplicating the transform.
    """

    DATASET_ID = "br_me_siconfi"

    # The validated transform lives here; the pipeline reuses it (see utils.py).
    #   CODE_DIR/download_api.py   — per-entity API download primitives
    #   CODE_DIR/build.py          — BUILDERS registry + per-year orchestration
    #   CODE_DIR/tables_final/*    — one builder per table (+ shared.py)
    #   CODE_DIR/crosswalk/*.xlsx  — hand-maintained compatibilização tables
    CODE_DIR = _REPO_ROOT / "models" / "br_me_siconfi" / "code"
    # Parent of CODE_DIR; load_crosswalk expects ``<path_queries>/code/crosswalk``.
    PATH_QUERIES = _REPO_ROOT / "models" / "br_me_siconfi"

    # First year available from the SICONFI API (DCA). Earlier years (Finbra,
    # 1989-2012) are frozen and served from the GCS parquet cache, never the API.
    API_FIRST_YEAR = 2013

    # Trailing window (in years) re-downloaded from the API on every scheduled
    # run. Tesouro revises published years retroactively for a while; a window
    # of 5 captures those revisions while bounding the ~1.1s/call download.
    # Older API years come from the cache. full_refresh=True overrides this and
    # re-downloads from API_FIRST_YEAR to re-catch deeper revisions.
    WINDOW_YEARS = 5

    # Tables by government level. balanco_patrimonial exists only at município.
    TABLES_BY_LEVEL = {
        "municipio": [
            "municipio_receitas_orcamentarias",
            "municipio_despesas_orcamentarias",
            "municipio_despesas_funcao",
            "municipio_balanco_patrimonial",
            "municipio_execucao_restos_pagar",
            "municipio_execucao_restos_pagar_funcao",
            "municipio_variacoes_patrimoniais",
        ],
        "uf": [
            "uf_receitas_orcamentarias",
            "uf_despesas_orcamentarias",
            "uf_despesas_funcao",
            "uf_execucao_restos_pagar",
            "uf_execucao_restos_pagar_funcao",
            "uf_variacoes_patrimoniais",
        ],
        "brasil": [
            "brasil_receitas_orcamentarias",
            "brasil_despesas_orcamentarias",
            "brasil_despesas_funcao",
            "brasil_execucao_restos_pagar",
            "brasil_execucao_restos_pagar_funcao",
            "brasil_variacoes_patrimoniais",
        ],
    }
    ALL_LEVELS = ("brasil", "uf", "municipio")

    # GCS prefix (inside the target bucket) holding the cleaned, all-STRING
    # parquet cache of every year, used to rebuild the full tables while only
    # re-downloading the trailing window. Layout mirrors the staging output:
    #   <prefix>/<table>/ano=YYYY/[sigla_uf=UF/]data.parquet
    CACHE_PREFIX = "staging-cache/br_me_siconfi"

    # GCS prefix (inside the target bucket) archiving the raw source files for
    # provenance — one gzip tarball of raw API JSON per year:
    #   <prefix>/api/dca_YYYY.tar.gz
    # The frozen 1989-2012 Finbra raw files are archived once at seed time under
    # <prefix>/finbra/. Distinct from the parquet cache (derived, not raw).
    RAW_PREFIX = "raw/br_me_siconfi"
