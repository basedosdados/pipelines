# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
import unittest
import pandas as pd

from pipelines.datasets.br_denatran_frota.utils import (
    make_filename,
    make_dir_when_not_exists,
    guess_header,
    get_year_month_from_filename,
)
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.tasks import crawl

DOWNLOAD_PATH = constants.DOWNLOAD_PATH.value


class TestMakeFilename(unittest.TestCase):
    def test_make_filename(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": month,
            "ano": year,
            "filetype": "xlsx",
            "destination_dir": DOWNLOAD_PATH,
        }
        filename = make_filename(i)
        self.assertEqual(
            filename,
            f"{DOWNLOAD_PATH}/frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}.xlsx",
        )

    def test_make_filename_without_ext(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": 2,
            "ano": 2013,
            "filetype": "xlsx",
            "destination_dir": DOWNLOAD_PATH,
        }
        filename = make_filename(i, ext=False)
        self.assertEqual(
            filename,
            f"{DOWNLOAD_PATH}/frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}",
        )


class TestMakeDirWhenNotExists(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_make_dir_when_not_exists(self):
        new_dir = os.path.join(self.temp_dir, "new_dir")
        self.assertFalse(os.path.exists(new_dir))
        make_dir_when_not_exists(new_dir)
        self.assertTrue(os.path.exists(new_dir))


class TestDownloadFrota(unittest.TestCase):
    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            crawl(13, 2013)


class TestFilenameExtraction(unittest.TestCase):
    def test_correct_file(self):
        filename = "indicator_2-2022.xlsx"
        self.assertEqual(get_year_month_from_filename(filename), ("2", "2022"))

    def test_not_excel_file(self):
        filename = "indicator_2-2022"
        with self.assertRaises(ValueError):
            get_year_month_from_filename(filename)

    def test_excel_but_incorrect_format(self):
        filename = "random_2_20.xlsx"
        with self.assertRaises(ValueError):
            get_year_month_from_filename(filename)


if __name__ == "__main__":
    unittest.main()
