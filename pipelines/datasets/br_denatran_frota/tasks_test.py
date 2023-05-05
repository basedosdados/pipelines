# -*- coding: utf-8 -*-
import os
import shutil
import tempfile
import unittest

from parameterized import parameterized
from pipelines.datasets.br_denatran_frota.handlers import crawl, treat_uf_tipo
from pipelines.datasets.br_denatran_frota.constants import constants

DATASET = constants.DATASET.value
DOWNLOAD_PATH = constants.DOWNLOAD_PATH.value


def custom_name_func(testcase_func, param_num, param):
    return "%s_%s" % (
        testcase_func.__name__,
        parameterized.to_safe_name("_".join(str(x) for x in param.args)),
    )


class TestAllPossibleYears(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(
            dir=os.path.join(f"{DOWNLOAD_PATH}")
        )

    def tearDown(self):
        print("Deleting temporary directory")
        shutil.rmtree(self.temp_dir.name)

    @parameterized.expand(
        [(month, year) for year in range(2014, 2016) for month in range(1, 3)],
        name_func=custom_name_func,
    )
    def test_download_post_2012(self, month, year):
        crawl(month, year, self.temp_dir.name)
        expected_files = {
            f"frota_por_uf_e_tipo_de_veiculo_{month}-{year}",
            f"frota_por_municipio_e_tipo_{month}-{year}",
        }
        list_of_files = os.listdir(os.path.join(self.temp_dir.name, "files", f"{year}"))
        files = set(os.path.splitext(file)[0] for file in list_of_files)
        self.assertTrue(expected_files.issubset(files))


if __name__ == "__main__":
    unittest.main()
