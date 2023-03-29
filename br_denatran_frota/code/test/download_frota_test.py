import os
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from download_frota import *


class TestDownloadFrota(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        os.rmdir(self.test_dir)

    def test_download_frota_with_valid_args(self):
        with patch("urllib.request.urlretrieve") as mock_urlretrieve:
            download_frota(month=2, year=2022)
            mock_urlretrieve.assert_called_once()

    def test_download_frota_with_invalid_year(self):
        with self.assertRaises(ValueError):
            download_frota(month=1, year=2010)

    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            download_frota(month=13, year=2022)


if __name__ == "__main__":
    unittest.main()
