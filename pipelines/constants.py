"""
Constants for all flows
"""

from enum import Enum


class constants(Enum):
    """
    Constants used in the BD flows.
    """

    MODE_TO_PROJECT_DICT = {"prod": "basedosdados", "dev": "basedosdados-dev"}

    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds

    GOOGLE_SHEETS_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    API_URL = {
        "staging": "https://staging.backend.basedosdados.org/api/v1/graphql",
        "prod": "https://backend.basedosdados.org/api/v1/graphql",
    }
