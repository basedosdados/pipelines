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
        # TODO: Since there is no teardown, this test doesn't have the same effect running twice.
        # It could always download the file twice in download_frota OR we implement cleanup?
        # Or we generalize with tempd irs. This might just be overkill anyway.
        download_frota(month=2, year=2022)
        expected_file_path = os.path.join(
            os.getcwd(),
            "br_denatran_frota",
            "files",
            "frota_por_município_e_tipo_2-2022.xls",
        )
        self.assertTrue(os.path.isfile(expected_file_path))

    def test_download_frota_with_invalid_year(self):
        with self.assertRaises(ValueError):
            download_frota(month=1, year=2010)

    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            download_frota(month=13, year=2022)


if __name__ == "__main__":
    unittest.main()
