# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""

from enum import Enum
from pipelines.utils.utils import get_credentials_from_secret

cred = get_credentials_from_secret("api_user_prod")


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the temporal_coverage_updater project
    """

    EMAIL = cred.get("email")
    PASSWORD = cred.get("password")
