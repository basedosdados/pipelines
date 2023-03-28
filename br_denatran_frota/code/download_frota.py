import os
import re
import tempfile
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from rarfile import RarFile
from bs4 import BeautifulSoup


def handle_xl(i: dict):
    tempdir = i["tempdir"]
    dest_path_file = os.path.join(tempdir, make_filename(i))
    if not os.path.isfile(dest_path_file):
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
    """Cria o nome do arquivo usando o dicionário enviado.

    Args:
        i (dict): Dicionário com todas as informações do arquivo.
        ext (bool, optional): Especifica se o nome de arquivo gerado precisa do tipo de arquivo no fim. Defaults to True.

    Returns:
        str: O nome completo do arquivo.
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
    if i["filetype"] in ["rar", "zip"]:
        handle_compact(i)
    elif i["filetype"] in ["xlsx", "xls"]:
        handle_xl(i)


def download_frota(month: int = None, year: int = None, tempdir=None, dir_name=None):
    months = {
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

    if year > 2012:
        url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-{year}"
    else:
        raise ValueError("Para anos anteriores eu não implementei a função ainda.")

    if month not in months.values():
        raise ValueError("Mês inválido.")

    if not tempdir:
        tempdir = tempfile.gettempdir()
    if not dir_name:
        dir_name = os.getcwd()

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
            if months.get(matched_month) == month and matched_year == str(year):
                filetype = match.group(0).split(".")[-1].lower()
                info = {
                    "txt": txt,
                    "href": href,
                    "mes_name": matched_month,
                    "mes": month,
                    "ano": year,
                    "filetype": filetype,
                    "tempdir": tempdir,
                }
                download_file(info)


download_frota(year=2022, month=2)
