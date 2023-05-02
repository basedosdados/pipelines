# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
import unittest
import glob
import pandas as pd
from parameterized import parameterized

from pipelines.datasets.br_denatran_frota.utils import (
    make_filename,
    make_dir_when_not_exists,
    guess_header,
    get_year_month_from_filename,
)
from pipelines.datasets.br_denatran_frota.constants import constants
from br_denatran_frota.code.download_frota import download_frota


DATASET = constants.DATASET.value

dir_list = glob.glob(f"**/{DATASET}", recursive=True)
if dir_list:
    # I always want to be in the actual folder for this dataset:
    os.chdir(dir_list[0])


class TestMakeFilename(unittest.TestCase):
    def test_make_filename(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": month,
            "ano": year,
            "filetype": "xlsx",
        }
        filename = make_filename(i)
        self.assertEqual(
            filename,
            f"frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}.xlsx",
        )

    def test_make_filename_without_ext(self):
        month = 2
        year = 2013
        i = {
            "txt": "frota-de-veiculos-por-municipio-tipo-e-combustivel",
            "mes": 2,
            "ano": 2013,
            "filetype": "xlsx",
        }
        filename = make_filename(i, ext=False)
        self.assertEqual(
            filename,
            f"frota-de-veiculos-por-municipio-tipo-e-combustivel_{month}-{year}",
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
            download_frota(13, 2013)


class TestGuessHeader(unittest.TestCase):
    def test_correct_header_guess(self):
        # Test case 1: Correct header guess
        df = pd.DataFrame(
            {
                "A": ["Name", "Alice", "Bob"],
                "B": ["Age", 30, 40],
                "C": ["Country", "USA", "Canada"],
            }
        )
        self.assertEqual(guess_header(df), 0)  # Header is in the first row

    def test_no_header_found(self):
        # Test case 2: No header found
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
        self.assertEqual(
            guess_header(df), 0
        )  # Header is assumed to be in the first row

    def test_header_found_beyond_max_header_guess(self):
        # Test case 3: Header found beyond max_header_guess
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": ["Name", "Alice", "Bob"]}
        )
        self.assertEqual(
            guess_header(df, max_header_guess=2), 0
        )  # Header is assumed to be in the first row

    def test_empty_dataframe(self):
        # Test case 4: Empty dataframe
        df = pd.DataFrame()
        self.assertEqual(
            guess_header(df), 0
        )  # Header is assumed to be in the first row

    def test_all_columns_are_strings_not_in_header_row(self):
        # Test case 5: All columns are strings but not in header row
        df = pd.DataFrame(
            {"A": ["1", "2", "3"], "B": ["4", "5", "6"], "C": ["7", "8", "9"]}
        )
        self.assertEqual(
            guess_header(df), 0
        )  # Header is assumed to be in the first row


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
