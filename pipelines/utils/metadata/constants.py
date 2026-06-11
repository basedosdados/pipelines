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
