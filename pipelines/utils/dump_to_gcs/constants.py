# -*- coding: utf-8 -*-
"""
Constants for Dump to GCS pipeline.
"""

from enum import Enum
from pipelines.utils.utils import get_credentials_from_secret

class constants(Enum):  # pylint: disable=c0103
    """
    Constants for utils.
    """

    MAX_BYTES_PROCESSED_PER_TABLE = 5 * 1024 * 1024 * 1024  # 5GB

    url_path = get_credentials_from_secret('url_download_data')
    secret_path_url_free = url_path['URL_DOWNLOAD_OPEN']
    secret_path_url_closed = url_path['URL_DOWNLOAD_CLOSED']



