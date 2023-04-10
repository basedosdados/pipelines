import os
import shutil
import tempfile
import unittest
import glob
from parameterized import parameterized

from download_frota import (
    DATASET,
    MONTHS,
    make_filename,
    make_dir_when_not_exists,
    download_frota,
    download_post_2012,
)


def custom_name_func(testcase_func, param_num, param):
    return "%s_%s" % (
        testcase_func.__name__,
        parameterized.to_safe_name("_".join(str(x) for x in param.args)),
    )


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
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        print("im heir")

    def tearDown(self):
        shutil.rmtree(self.temp_dir.name)

    @unittest.skip("demonstrating skipping")
    def test_download_frota_with_valid_month(self):
        download_frota(MONTHS["fevereiro"], 2013)
        expected_files = {
            "frota_de_veiculos_por_municipio_tipo_e_combustivel_2-2013.xlsx",
            "frota_de_veiculos_por_uf_e_tipo_2-2013.xlsx",
        }
        files = set(os.listdir(os.path.join(self.temp_dir, DATASET, "files", "2013")))
        self.assertEqual(files, expected_files)

    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            download_frota(13, 2013)

    @parameterized.expand(
        [(month, year) for year in range(2022, 2023) for month in range(1, 13)],
        name_func=custom_name_func,
    )
    def test_download_post_2012(self, month, year):
        download_frota(month, year, self.temp_dir.name)
        expected_files = {
            f"frota_por_uf_e_tipo_de_veículo_{month}-{year}.xls",
            f"frota_por_município_e_tipo_{month}-{year}.xls",
        }
        files = set(
            os.listdir(os.path.join(DATASET, self.temp_dir.name, "files", f"{year}"))
        )
        self.assertEqual(files, expected_files)


if __name__ == "__main__":
    unittest.main()
