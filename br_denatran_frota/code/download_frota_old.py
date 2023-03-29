import os
import re
import requests
from zipfile import ZipFile
import shutil


def download_frota_old(key=None, prefix=None, year=None, tempdir=None, dir=None):
    def create_directory(path):
        if not os.path.exists(path):
            os.makedirs(path)

    def extract_files_from_zip(zip_file, extract_path):
        with ZipFile(zip_file, "r") as zip:
            zip.extractall(extract_path)

    if year > 2012:
        raise ValueError("Utilize download_frota()")

    mi_url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-denatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"

    create_directory(tempdir)
    create_directory(dir)

    dir_temp = os.path.join(tempdir, f"frota__{year}")
    path_file = os.path.join(dir_temp, ".zip")

    if not os.path.exists(path_file):
        try:
            response = requests.get(mi_url)
            with open(path_file, "wb") as f:
                f.write(response.content)
        except Exception as e:
            if os.path.exists(path_file):
                os.remove(path_file)

        extract_files_from_zip(path_file, dir_temp)

    months = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }

    for root, dirs, files in os.walk(dir_temp):
        for name in files:
            if name.endswith(".zip"):
                extract_files_from_zip(
                    os.path.join(root, name), os.path.dirname(os.path.join(root, name))
                )

    for root, dirs, files in os.walk(dir_temp):
        for name in files:
            file_path = os.path.join(root, name)

            get_by = {
                "regiao": "(frota tipo uf)|(frota uf)|(frota regiao uf)|(frota regiao tipo uf)",
                "municipio": "(frota munic)|(frota mun)",
            }

            new_name = (
                os.path.basename(file_path)
                .lower()
                .replace("regiões", "regiao")
                .replace("_", " ")
                .replace("-", " ")
                .replace(".xlsx", "")
                .replace(".xls", "")
                .replace("internet", "")
                .strip()
            )
            new_name = re.sub(r"^(\\d\\_|\\d)+", "", new_name)

            # Match jan|fev|... or number of month (two digits)
            get_month = re.search(
                rf"({'|'.join(months.keys())})|\d{{2}}(?=\d{{4}})",
                new_name,
                re.IGNORECASE,
            )
            if get_month:
                month_i = int(get_month.group(0))
                if get_month.group(1):
                    month_i = months[get_month.group(1)]

            file_ext = re.search(r"\.[a-zA-Z0-9]+$", new_name).group(0)

            if (
                key
                and prefix
                and re.search(get_by[key], new_name, re.IGNORECASE)
                and name.endswith(("xls", "xlsx"))
            ):
                shutil.copy(
                    file_path, os.path.join(dir, f"{prefix}_{month_i}-{year}{file_ext}")
                )
