"""Constants for the au_sa_ecsa_elections dataset.

The Electoral Commission South Australia publishes its results through an open
Azure API Management endpoint that backs the ``results-display`` Angular app. The
routes below were read out of the app's own bundle: it builds every request as
``<BASE><route>``, with the election's poll date as the path parameter and, for
the ``*Change`` routes, a data version that the app uses for delta polling. A
version of ``0`` returns the full payload.
"""

from __future__ import annotations

import os
import pathlib
from enum import Enum

DATASET_ID = "au_sa_ecsa_elections"
DATASET_SLUG = "sa_elections"


class constants(Enum):
    """Enum of dataset constants, per the house pipeline convention."""

    API_BASE = "https://apim-ecsa-production.azure-api.net/results-display/"

    # Route templates. ``date`` is the poll date as ``YYYY-MM-DD``.
    ROUTE_ELECTION_DATES = "ElectionDates"
    ROUTE_HA_STATIC = "HAStatic/{date}"
    ROUTE_HA_CHANGE = "HAChange/{date}/0"
    ROUTE_LC_STATIC = "LCStatic/{date}"
    ROUTE_LC_CHANGE = "LCChange/{date}/0"

    # The four per-election payloads, keyed by the local file stem.
    PER_ELECTION_ROUTES = {
        "ha_static": "HAStatic/{date}",
        "ha_change": "HAChange/{date}/0",
        "lc_static": "LCStatic/{date}",
        "lc_change": "LCChange/{date}/0",
    }

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    # Only the two chambers of the South Australian Parliament are in scope.
    # Local government elections (LGEEvents / LGEStatic / LGEChange) are served by
    # the same API and are deliberately excluded.
    HOUSE_OF_ASSEMBLY = "house_of_assembly"
    LEGISLATIVE_COUNCIL = "legislative_council"

    ARCHITECTURE_DIR = "models/au_sa_ecsa_elections/code/architecture"


def data_dir() -> pathlib.Path:
    """Scratch root for raw downloads and cleaned Parquet, never the repository."""
    return pathlib.Path(
        os.environ.get(
            "ECSA_DATA_DIR",
            str(
                pathlib.Path.home() / "Downloads" / "au_sa_ecsa_elections_data"
            ),
        )
    )
