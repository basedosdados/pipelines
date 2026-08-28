"""Constants for br_mgi_compras_publicas.

Source: Compras.gov.br open-data API, https://dadosabertos.compras.gov.br
OpenAPI spec: /v3/api-docs. Licence: CC BY 4.0.

Two procurement regimes live behind one API and they do NOT share conventions:

* The Lei 14.133/2021 modules (`modulo-contratacoes`, `modulo-arp`,
  `modulo-contratos`) take **half-open** date ranges, ``[inicial, final)``.
  Passing the same date twice returns zero rows under HTTP 200.
* The `modulo-legado` (Lei 8.666/1993) takes **closed** ranges,
  ``[inicial, final]``.

Reusing one window generator across both silently double-counts on one side and
drops a day on the other, so `WindowKind` is carried explicitly on every spec.
"""

from __future__ import annotations

from enum import Enum, StrEnum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = (
    REPO_ROOT / "models" / "br_mgi_compras_publicas" / "code" / "architecture"
)


class WindowKind(StrEnum):
    """How an endpoint interprets its date range."""

    HALF_OPEN = "half_open"  # [inicial, final) -- Lei 14.133 modules
    CLOSED = "closed"  # [inicial, final]  -- modulo-legado
    YEAR = "year"  # a single year passed as an integer parameter
    ORGAO = "orgao"  # iterate codigoOrgao; date range spans everything
    MODALIDADE = "modalidade"  # no date filter at all; iterate modalidade
    SNAPSHOT = "snapshot"  # registries and catalogues, no temporal key


class constants(Enum):
    BASE_URL = "https://dadosabertos.compras.gov.br"

    # tamanhoPagina is validated server-side to the inclusive range 10-500 and
    # exceeding it is a 400 with a plain-text body, never a clamp.
    PAGE_SIZE = 500
    MAX_PAGE_SIZE = 500
    MIN_PAGE_SIZE = 10

    # Windows longer than this are rejected: "Período inicial e final maior que
    # 365 dias." A leap-year Jan 1 -> Jan 1 is 366 days and fails.
    MAX_WINDOW_DAYS = 365

    # Measured 2026-08-28: throughput peaks at 8 concurrent requests. Twelve was
    # slower (148 vs 178 rows/s on the heaviest endpoint), so this is a ceiling,
    # not a floor.
    MAX_WORKERS = 8

    REQUEST_TIMEOUT = 300
    MAX_RETRIES = 5

    # Rate limiting is paced, not failed: a 429 must not consume the budget
    # reserved for genuine transient errors. /modulo-contratos/ returns 429
    # almost continuously at six workers, so this needs headroom.
    MAX_THROTTLE_RETRIES = 60
    BACKOFF_BASE = 2.0

    # Only these four SIASG modalidade codes carry any Lei 14.133 contratacao.
    # Codes 1, 2, 4 and 8-19 return zero rows for every window tested.
    MODALIDADES_14133 = (3, 5, 6, 7)

    # Legado modalidade codes present in modulo-legado/2_consultarItemLicitacao.
    # 5 (Pregao), 6 (Dispensa) and 7 (Inexigibilidade) hold 49.1M of the 53.9M
    # rows and are deferred to tier B; see PLAN.md section 6.
    MODALIDADES_LEGADO_TIER_A = (1, 2, 3, 4, 20, 99)
    MODALIDADES_LEGADO_TIER_B = (5, 6, 7)

    # Temporal extent of each regime, measured 2026-08-28.
    ANO_INICIO_14133 = 2021
    ANO_INICIO_LEGADO = 1997
    ANO_FIM_LEGADO = 2025

    # The feed stalled here: no contratacao, item or resultado is published with
    # a later date, verified daily through 2026-08-28. The API itself is healthy.
    ULTIMA_DATA_OBSERVADA = "2026-07-23"

    DATASET_ID = "br_mgi_compras_publicas"
