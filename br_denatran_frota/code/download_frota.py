import os
import re
import glob
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from rarfile import RarFile
from bs4 import BeautifulSoup


MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}

DATASET = "br_denatran_frota"


def handle_xl(i: dict) -> None:
    """Actually downloads and deals with Excel files.

    Args:
        i (dict): Dictionary with all the desired downloadable file's info.
    """
    dest_path_file = make_filename(i)
    urlretrieve(i["href"], dest_path_file)


def handle_compact(i: dict):
    tempdir = i["tempdir"]
    path_file_zip = os.path.join(tempdir, make_filename(i))
    dir_file = os.path.join(tempdir, make_filename(i, ext=False))

    if not os.path.isfile(path_file_zip):
        urlretrieve(i["href"], path_file_zip)

    if i["filetype"] == "rar":
        with RarFile(path_file_zip) as rar_file:
            rar_file.extractall(dir_file)
    else:
        with ZipFile(path_file_zip) as zip_file:
            zip_file.extractall(dir_file)

    for filename in os.listdir(dir_file):
        filepath = os.path.join(dir_file, filename)
        if os.path.isfile(filepath):
            if filename.endswith((".xlsx", ".xls")):
                dest_path_file = os.path.join(tempdir, make_filename(i))
                if not os.path.isfile(dest_path_file):
                    os.rename(filepath, dest_path_file)
                else:
                    os.remove(filepath)


def make_filename(i: dict, ext: bool = True) -> str:
    """Creates the filename using the sent dictionary.

    Args:
        i (dict): Dictionary with all the file's info.
        ext (bool, optional): Specifies if the generated file name needs the filetype at the end. Defaults to True.

    Returns:
        str: The full filename.
    """
    txt = i["txt"]
    mes = i["mes"]
    ano = i["ano"]
    filetype = i["filetype"]
    filename = re.sub("\\s+", "_", txt, flags=re.UNICODE).lower()
    filename = f"{filename}_{mes}-{ano}"
    if ext:
        filename += f".{filetype}"
    return filename


def download_file(i):
    if i["filetype"] in ["xlsx", "xls"]:
        handle_xl(i)
    else:
        raise ValueError("A função handle_compact tá bem esquisita por hora.")


def download_post_2012(year: int, month: int):
    """_summary_

    Args:
        year (int): _description_
        month (int): _description_
    """
    url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-{year}"
    soup = BeautifulSoup(urlopen(url), "html.parser")
    # Só queremos os dados de frota nacional.
    nodes = soup.select("p:contains('Frota por ') > a")
    for node in nodes:
        txt = node.text
        href = node.get("href")
        # Pega a parte relevante do arquivo em questão.
        match = re.search(
            r"(?i)\/([\w-]+)\/(\d{4})\/(\w+)\/([\w-]+)\.(?:xls|xlsx|rar|zip)$", href
        )
        if match:
            matched_month = match.group(3)
            matched_year = match.group(2)
            if MONTHS.get(matched_month) == month and matched_year == str(year):
                filetype = match.group(0).split(".")[-1].lower()
                info = {
                    "txt": txt,
                    "href": href,
                    "mes_name": matched_month,
                    "mes": month,
                    "ano": year,
                    "filetype": filetype,
                }
                download_file(info)


def make_dir_when_not_exists(dir_name: str):
    """Auxiliary function to create a subdirectory when it is not present.

    Args:
        dir_name (str): Name of the subdirectory to be created.
    """
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)


def download_frota(month: int, year: int):
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Dá erro caso o mês desejado não seja um mês válido.
    """

    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")

    dir_list = glob.glob(f"**/{DATASET}", recursive=True)
    if dir_list:
        # I always want to be in the actual folder for this dataset:
        os.chdir(dir_list[0])

    # I always need a files directory inside my dataset folder.
    make_dir_when_not_exists("files")
    year_dir_name = f"{year}"
    desired_dir_name = os.path.join("files", year_dir_name)

    # Cria pasta para aquele ano de arquivos caso seja necessário.
    make_dir_when_not_exists(desired_dir_name)
    os.chdir(desired_dir_name)
    if year > 2012:
        download_post_2012(year, month)
    else:
        url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"

        generic_zip_filename = f"geral_{year}.zip"
        urlretrieve(url, generic_zip_filename)
        with ZipFile(generic_zip_filename) as zip_file:
            zip_file.extractall()

        print(2)
