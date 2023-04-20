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
        file_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(file_dir)
        self.temp_dir = tempfile.TemporaryDirectory(dir=file_dir)

    def tearDown(self):
        print("Deleting temporary directory")
        shutil.rmtree(self.temp_dir.name)

    def test_download_frota_with_invalid_month(self):
        with self.assertRaises(ValueError):
            download_frota(13, 2013)

    @parameterized.expand(
        [(month, year) for year in range(2013, 2014) for month in range(1, 4)],
        name_func=custom_name_func,
    )
    def test_download_post_2012(self, month, year):
        download_frota(month, year, self.temp_dir.name)
        expected_files = {
            f"frota_por_uf_e_tipo_de_veículo_{month}-{year}",
            f"frota_por_município_e_tipo_{month}-{year}",
        }
        list_of_files = os.listdir(
            os.path.join(DATASET, self.temp_dir.name, "files", f"{year}")
        )
        files = set(os.path.splitext(file)[0] for file in list_of_files)
        self.assertEqual(files, expected_files)


if __name__ == "__main__":
    unittest.main()
