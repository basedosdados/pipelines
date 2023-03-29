import os
import shutil
import tempfile
import unittest

from download_frota_old import download_frota_old


class TestDownloadFrotaOld(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.dir = os.getcwd()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        os.chdir(self.dir)

    def test_download_frota_old(self):
        download_frota_old(year=2009, tempdir=self.temp_dir, dir=self.dir)
        self.assertTrue(os.path.exists(os.path.join(self.dir, "frota_12-2009.xlsx")))
        self.assertTrue(os.path.exists(os.path.join(self.dir, "frota_1-2009.xlsx")))
        self.assertTrue(os.path.exists(os.path.join(self.dir, "frota_2-2009.xlsx")))

    def test_download_frota_old_year_above_2012(self):
        with self.assertRaises(ValueError):
            download_frota_old(year=2013, tempdir=self.temp_dir, dir=self.dir)


if __name__ == "__main__":
    unittest.main()
