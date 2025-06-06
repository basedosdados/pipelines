# -*- coding: utf-8 -*-
"""
Constants for utils.
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for the database dump stuff.
    """

    WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS = 3
    WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL = 5
    DISABLE_ELEMENTARY_VARS = {
        "disable_dbt_artifacts_autoupload": True,
        "disable_run_results": True,
        "disable_tests_results": True,
        "disable_dbt_invocation_autoupload": True,
    }
