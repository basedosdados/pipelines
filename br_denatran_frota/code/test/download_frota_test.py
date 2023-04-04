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
        # TODO: This is always assuming a structure of having a files dir under the denatran directory...
        month = 2
        year = 2022
        download_frota(month, year)
        expected_file_path = os.path.join(
            os.getcwd(),
            f"frota_por_município_e_tipo_{month}-{year}.xls",
        )
        self.assertTrue(os.path.isfile(expected_file_path))

    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            download_frota(month=13, year=2022)


if __name__ == "__main__":
    unittest.main()
