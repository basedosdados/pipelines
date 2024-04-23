# -*- coding: utf-8 -*-
"""
Constant values for the utils projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the metadata project
    """

    MODE_PROJECT = {"dev": "basedosdados-dev", "prod": "basedosdados"}

    ACCEPTED_TIME_UNITS = [
        "years",
        "months",
        "weeks",
        "days",
    ]

    ACCEPTED_COVERAGE_TYPE = ["all_bdpro", "all_free", "part_bdpro"]

    ACCEPTED_COLUMN_KEY_VALUES = [{"year", "month"}, {"year", "quarter"}, {"date"}]

    API_URL = {
        "staging": "https://staging.api.basedosdados.org/api/v1/graphql",
        "prod":"https://api.basedosdados.org/api/v1/graphql"
    }
