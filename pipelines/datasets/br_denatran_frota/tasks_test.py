# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
import unittest
import re
import polars as pl

from parameterized import parameterized
from pipelines.datasets.br_denatran_frota.handlers import (
    crawl,
    treat_uf_tipo,
    get_desired_file,
    treat_municipio_tipo,
)
from pipelines.datasets.br_denatran_frota.constants import constants

DATASET = constants.DATASET.value
DOWNLOAD_PATH = constants.DOWNLOAD_PATH.value
MUNIC_TIPO_BASIC_FILENAME = constants.MUNIC_TIPO_BASIC_FILENAME.value
UF_TIPO_BASIC_FILENAME = constants.UF_TIPO_BASIC_FILENAME.value
DICT_UFS = constants.DICT_UFS.value


def custom_name_func(testcase_func, param_num, param):
    return "%s_%s" % (
        testcase_func.__name__,
        parameterized.to_safe_name("_".join(str(x) for x in param.args)),
    )


class TestExtractingAllPossibleYears(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(
            dir=os.path.join(f"{DOWNLOAD_PATH}")
        )

    def tearDown(self):
        print("Deleting temporary directory")
        shutil.rmtree(self.temp_dir.name)

    @parameterized.expand(
        [(month, year) for year in range(2003, 2024) for month in range(1, 2)],
        name_func=custom_name_func,
    )
    def test_extract_denatran_files(self, month, year):
        crawl(month, year, self.temp_dir.name)
        expected_files = {
            f"frota_por_uf_e_tipo_de_veiculo_{month}-{year}",
            f"frota_por_municipio_e_tipo_{month}-{year}",
        }
        list_of_files = os.listdir(os.path.join(self.temp_dir.name, "files", f"{year}"))
        files = set(os.path.splitext(file)[0] for file in list_of_files)
        self.assertTrue(expected_files.issubset(files))


class TestUFTreatmentPostCrawl(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(
            dir=os.path.join(f"{DOWNLOAD_PATH}")
        )

    def tearDown(self):
        print("Deleting temporary directory")
        shutil.rmtree(self.temp_dir.name)

    @parameterized.expand(
        [(month, year) for year in range(2003, 2024) for month in range(1, 2)],
        name_func=custom_name_func,
    )
    def test_treat_files_uf_tipo(self, month, year):
        crawl(month, year, self.temp_dir.name)
        directory_to_search = os.path.join(self.temp_dir.name, "files", f"{year}")
        get_desired_file(year, self.temp_dir.name, UF_TIPO_BASIC_FILENAME)
        for file in os.listdir(directory_to_search):
            if re.search(UF_TIPO_BASIC_FILENAME, file) and file.split(".")[-1] in [
                "xls",
                "xlsx",
            ]:
                treated_df = treat_uf_tipo(os.path.join(directory_to_search, file))
                self.assertEqual(len(treated_df), 21 * 27)

    def test_flow(self):
        year = 2021
        crawl(month=2, year=year, temp_dir=constants.DOWNLOAD_PATH.value)
        # Now get the downloaded file:
        uf_tipo_file = get_desired_file(
            year=year,
            download_directory=constants.DOWNLOAD_PATH.value,
            filetype=constants.UF_TIPO_BASIC_FILENAME.value,
        )
        print(uf_tipo_file)
        df = treat_uf_tipo(file=uf_tipo_file)
        self.assertTrue(type(df), pl.DataFrame)


class TestMunicipioTreatmentPostCrawl(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(
            dir=os.path.join(f"{DOWNLOAD_PATH}")
        )

    def tearDown(self):
        print("Deleting temporary directory")
        shutil.rmtree(self.temp_dir.name)

    @parameterized.expand(
        [(month, year) for year in range(2003, 2024) for month in range(1, 2)],
        name_func=custom_name_func,
    )
    def test_treat_files_municipio_tipo(self, month, year):
        crawl(month, year, self.temp_dir.name)
        directory_to_search = os.path.join(self.temp_dir.name, "files", f"{year}")
        get_desired_file(year, self.temp_dir.name, MUNIC_TIPO_BASIC_FILENAME)
        for file in os.listdir(directory_to_search):
            if re.search(MUNIC_TIPO_BASIC_FILENAME, file) and file.split(".")[-1] in [
                "xls",
                "xlsx",
            ]:
                treated_df = treat_municipio_tipo(
                    os.path.join(directory_to_search, file)
                )
                # It should be a df,
                self.assertTrue(type(treated_df), pl.DataFrame)
                # Every state should be there
                self.assertSetEqual(
                    set(treated_df["sigla_uf"].unique().to_list()), set(DICT_UFS.keys())
                )
                # I expect AT LEAST 5450 cities out of 5570. It's normally higher but historical data.
                self.assertGreaterEqual(
                    len(treated_df["id_municipio"].unique().to_list()), 5450
                )


if __name__ == "__main__":
    unittest.main()
