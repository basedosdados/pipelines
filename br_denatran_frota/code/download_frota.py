import os
import re
import glob
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from rarfile import RarFile
from bs4 import BeautifulSoup
import requests

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
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
}


def download_file(url, filename):
    # Send a GET request to the URL

    new_url = url.replace("arquivos-denatran", "arquivos-senatran")
    response = requests.get(new_url, headers=headers)
    # Save the contents of the response to a file
    with open(filename, "wb") as f:
        f.write(response.content)

    print(f"Download of {filename} complete")


def extract_zip(dest_path_file):
    with ZipFile(dest_path_file, "r") as z:
        z.extractall()


def handle_xl(i: dict) -> None:
    """Actually downloads and deals with Excel files.

    Args:
        i (dict): Dictionary with all the desired downloadable file's info.
    """
    dest_path_file = make_filename(i)
    download_file(i["href"], dest_path_file)


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

    print(2)


def call_downloader(i):
    filename = make_filename(i)
    if i["filetype"] in ["xlsx", "xls"]:
        download_file(i["href"], filename)
    elif i["filetype"] == "zip":
        download_file(i["href"], filename)
        extract_zip(filename)


def download_post_2012(month: int, year: int):
    """_summary_

    Args:
        year (int): _description_
        month (int): _description_
    """
    url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-{year}"
    soup = BeautifulSoup(urlopen(url), "html.parser")
    # Só queremos os dados de frota nacional.
    nodes = soup.select("p:contains('rota por ') > a")
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
                call_downloader(info)


def make_dir_when_not_exists(dir_name: str):
    """Auxiliary function to create a subdirectory when it is not present.

    Args:
        dir_name (str): Name of the subdirectory to be created.
    """
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)


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
