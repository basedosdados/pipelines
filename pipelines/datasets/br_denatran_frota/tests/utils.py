# -*- coding: utf-8 -*-
import unittest

from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.utils import (
    get_year_month_from_filename,
    make_file_path,
)

DOWNLOAD_PATH = constants.DOWNLOAD_PATH.value


# Classes to test br_denatran_frota functions with unnittest
class TestMakeFilename(unittest.TestCase):
    """
    Class to test function make_file_path
    """

    def test_make_file_path(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": month,
            "ano": year,
            "filetype": "xlsx",
            "destination_dir": DOWNLOAD_PATH,
        }
        filename = make_file_path(i)
        self.assertEqual(
            filename,
            f"{DOWNLOAD_PATH}/frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}.xlsx",
        )

    def test_make_file_path_without_ext(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": 2,
            "ano": 2013,
            "filetype": "xlsx",
            "destination_dir": DOWNLOAD_PATH,
        }
        filename = make_file_path(i, ext=False)
        self.assertEqual(
            filename,
            f"{DOWNLOAD_PATH}/frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}",
        )


class TestFilenameExtraction(unittest.TestCase):
    """
    Class to test filename extraction
    """

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
