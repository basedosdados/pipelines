"""
Constants for utils.
"""

from enum import Enum
from typing import ClassVar


class constants(Enum):
    """
    Constants for the database dump stuff.
    """

    WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS = 3
    WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL = 5
    DISABLE_ELEMENTARY_VARS: ClassVar[dict[str, bool]] = {
        "disable_dbt_artifacts_autoupload": True,
        "disable_run_results": True,
        "disable_tests_results": True,
        "disable_dbt_invocation_autoupload": True,
    }
