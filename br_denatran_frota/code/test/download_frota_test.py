import os
import shutil
import tempfile
import unittest
import glob

from download_frota import (
    DATASET,
    MONTHS,
    make_filename,
    make_dir_when_not_exists,
    download_frota,
    download_post_2012,
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
        # Create a simple temporary directory for files, as opposed to the real one?
        # Problema aqui é que a minha estrutura sempre assume que BR DENATRAN FROTA é um repo, e ai vai criando conforme for files, ano, etc.
        # Idealmente, o teste nesse caso só teria o tear down pro ano testado SÓ QUE isso é mei burro né
        # Pq? Pq oras, se vc rodou 2012, aí testou 2012, vc ia perder tudo q fez pré teste. Seu teste apagar sua execução é meio esquisito.
        # OK, então qual que é a maneira
        self.temp_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

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

    def test_download_post_2012(self):
        download_frota(MONTHS["fevereiro"], 2018, self.temp_dir)
        expected_files = {
            "Frota por UF e tipo_2-2018.xlsx",
            "Frota por Município e tipo e combustível_2-2018.xlsx",
        }
        files = set(os.listdir(os.path.join(self.temp_dir, DATASET, "files", "2018")))
        self.assertEqual(files, expected_files)


if __name__ == "__main__":
    unittest.main()
