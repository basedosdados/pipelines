"""
Constant values for the utils projects
"""

from enum import Enum
from typing import ClassVar


class constants(Enum):
    """
    Constant values for the metadata project
    """

    MODE_PROJECT: ClassVar[dict[str, str]] = {
        "dev": "basedosdados-dev",
        "prod": "basedosdados",
    }

    ACCEPTED_TIME_UNITS: ClassVar[list[str]] = [
        "years",
        "months",
        "weeks",
        "days",
    ]

    ACCEPTED_COVERAGE_TYPE: ClassVar[list[str]] = [
        "all_bdpro",
        "all_free",
        "part_bdpro",
    ]

    ACCEPTED_COLUMN_KEY_VALUES: ClassVar[list[set[str]]] = [
        {"year", "month"},
        {"year", "quarter"},
        {"date"},
        {"year"},
    ]
