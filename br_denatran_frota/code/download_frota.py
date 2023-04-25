# -*- coding: utf-8 -*-
import os
import glob
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from br_denatran_frota.code.constants import MONTHS, DATASET
from br_denatran_frota.code.utils import make_dir_when_not_exists, download_post_2012


def download_frota(month: int, year: int, temp_dir: str = ""):
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")

    dir_list = glob.glob(f"**/{DATASET}", recursive=True)
    # Get the directory where this Python file is located
    initial_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the "files" directory relative to this directory
    files_dir = os.path.join(initial_dir, "..", "files")
    if dir_list:
        # I always want to be in the actual folder for this dataset, because I might start in the pipelines full repo:
        os.chdir(dir_list[0])
    if temp_dir:
        os.chdir(temp_dir)
    # I always need a files directory inside my dataset folder.
    make_dir_when_not_exists(files_dir)
    # I should always switch to the files dir now and save stuff inside it.
    os.chdir(files_dir)
    year_dir_name = f"{year}"

    # Create dir for that specific year should it be necessary.
    make_dir_when_not_exists(year_dir_name)
    os.chdir(year_dir_name)
    if year > 2012:
        download_post_2012(month, year)
    else:
        url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"

        generic_zip_filename = f"geral_{year}.zip"
        urlretrieve(url, generic_zip_filename)
        with ZipFile(generic_zip_filename) as zip_file:
            zip_file.extractall()
    os.chdir(initial_dir)


year = 2022
if __name__ == "__main__":
    for month in range(1, 6):
        download_frota(month, year)
