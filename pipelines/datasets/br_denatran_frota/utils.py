# -*- coding: utf-8 -*-
"""
General purpose functions for the br_denatran_frota project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_denatran_frota.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_denatran_frota.utils import foo
# foo()
# ```
#
###############################################################################
# -*- coding: utf-8 -*-
import pandas as pd
import polars as pl
import difflib
import re
import os
from zipfile import ZipFile
from rarfile import RarFile
import requests
from string_utils import asciify
from pipelines.datasets.br_denatran_frota.constants import constants
from bs4 import BeautifulSoup
from urllib.request import urlopen
import rpy2.robjects.packages as rpackages
import rpy2.robjects as robjects
from rpy2.robjects.vectors import StrVector


DICT_UFS = constants.DICT_UFS.value
SUBSTITUTIONS = constants.SUBSTITUTIONS.value
HEADERS = constants.HEADERS.value
MONTHS = constants.MONTHS.value
UF_TIPO_BASIC_FILENAME = constants.UF_TIPO_BASIC_FILENAME.value
MONTHS_SHORT = constants.MONTHS_SHORT.value
MUNIC_TIPO_BASIC_FILENAME = constants.MUNIC_TIPO_BASIC_FILENAME.value
UF_TIPO_HEADER = constants.UF_TIPO_HEADER.value

def guess_header(df: pd.DataFrame, max_header_guess: int = 10) -> int:
    """Function to deal with problematic dataframes coming from Excel/CSV files.
    Tries to guess which row is the header by examining the first few rows (max given by max_header_guess).
    Assumes that the header is the first row where all of the columns are strings.
    This will NOT properly work for a strings only dataframe and will always guess the first row for it.

    Args:
        df (pd.DataFrame): Dataframe whose header we don't know.
        max_header_guess (int, optional): Number of initial rows we want to check. Defaults to 4.

    Returns:
        int: Index of the row where the header is contained.
    """
    header_guess = 0
    possible_guess_list = []
    while header_guess < max_header_guess:
        if len(df) - 1 < header_guess:
            break
        # If this row minus the first column can be converted to int, then
        #if is_int_row(df.iloc[header_guess]):
        current_row = df.iloc[header_guess].to_list()
        equal_column_names = [(x, y) for x,y in zip(UF_TIPO_HEADER, current_row) if x == y]
        if len(equal_column_names)/len(UF_TIPO_HEADER) > 0.7: # If 70% of the columns match, this is the header.
            return header_guess
        header_guess += 1
    return 0 # If nothing is ever found until the max, let's just assume it's the first row as per usual.


def change_df_header(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """Changes the dataframe's header to a row inside of it and returns the corrected df.
    Ideally, to be used in conjunction with guess_header().
    Args:
        df (pd.DataFrame): Dataframe whose header we want changed.
        header_row (int): Index of the row where the header is located.

    Returns:
        pd.DataFrame: Returns the same dataframe but with the corrected header
    """
    new_header = df.iloc[header_row]
    new_df = df[(header_row + 1) :].reset_index(drop=True)
    new_df.rename(columns=new_header, inplace=True)
    return new_df

def is_int_row(row):
    # convert all elements of the row to strings
    str_row = [str(x) for x in row[1:]]
    
    # try to convert each element to an int, return False if it fails
    for element in str_row:
        try:
            int(element)
        except ValueError:
            return False
    
    # if all elements can be converted to int, return True
    return True


def get_year_month_from_filename(filename: str) -> tuple[int, int]:
    """Helper to extract month and year information from files named indicator_month-year.xls

    Args:
        filename (str): Name of the file.

    Raises:
        ValueError: Errors out if nothing is found, which likely means the filename is not the correct format.

    Returns:
        tuple[int, int]: Month, year.
    """
    match = re.search(r"(\w+)_(\d{1,2})-(\d{4})\.(xls|xlsx)$", filename)
    if match:
        month = match.group(2)
        year = match.group(3)
        return month, year
    else:
        raise ValueError("No match found")


def verify_total(df: pl.DataFrame) -> None:
    """Helper function that is meant to act as a guard to guarantee that we can pivot from wide to long.
    Essentially, gets a Wide dataframe, excludes all string columns and the TOTAL column and sums it all row wise.
    Then verifies if the calculated total column is the same as the TOTAL column.
    If not, raises an error.

    Args:
        df (pl.DataFrame): Wide format dataframe to verify.

    Raises:
        ValueError: Errors out if the sum of the columns is actually different than the total.
    """
    columns_for_total = df.select(pl.exclude("TOTAL")).select(pl.exclude([pl.Utf8]))
    calculated_total = columns_for_total.select(
        pl.fold(
            acc=pl.lit(0), function=lambda acc, x: acc + x, exprs=pl.col("*")
        ).alias("calculated_total")
    )["calculated_total"]
    mask = df["TOTAL"] == calculated_total
    if pl.sum(~mask) != 0:
        raise ValueError(
            "A coluna de TOTAL da base original tem inconsistências e não é igual à soma das demais colunas."
        )


def fix_suggested_nome_ibge(row: tuple[str, ...]) -> str:
    """Gets a row from a dataframe and applies the SUBSTITUTIONS constant ruleset where applicable.
    This fixes the dataframe to have the names of municipalities according to IBGE.
    The fixes are necessary because match_ibge() will fail where the DENATRAN data has typos in city names.

    Args:
        row (tuple[str, ...]): Row from the full DENATRAN dataframe we want to apply the IBGE substitutions to.

    Raises:
        ValueError: Errors out if the desired parts of the row do not conform to the expected format.

    Returns:
        str: Returns the suggested IBGE name, either the pre existing or the one in the ruleset for substitutions.
    """
    key = (row[0], row[1])
    if (not isinstance(row[0], str)) or (not isinstance(row[1], str)):
        raise ValueError("This is not a valid key to be checked.")
    if key in SUBSTITUTIONS:
        return SUBSTITUTIONS[key]
    else:
        return row[-1]


def match_ibge(denatran_uf: pl.DataFrame, ibge_uf: pl.DataFrame) -> None:
    """Takes a dataframe of the Denatran data and an IBGE dataframe of municipalities.
    Joins them using the IBGE name of both. The IBGE name of denatran_uf is ideally filled via get_city_name().
    This verifies if there are any municipalities in denatran_uf that have no corresponding municipality in the IBGE database.
    These must be manually fixed via the constants file and the fix_suggested_nome function.
    Will error out if there are municipalities that have no correspondence whatsover.
    Args:
        denatran_uf (pl.DataFrame): Dataframe with the DENATRAN data, filtered by state (UF).
        ibge_uf (pl.DataFrame): Dataframe with the IBGE municipalities data, filtered by state (UF).

    Raises:
        ValueError: Errors if there are municipalities that have not match and outputs them all with the state to enable manual fix.
    """
    joined_df = denatran_uf.join(
        ibge_uf,
        left_on=["suggested_nome_ibge", "sigla_uf"],
        right_on=["nome", "sigla_uf"],
        how="left",
    )
    mismatched_rows = joined_df.filter(pl.col("id_municipio").is_null())
    if len(mismatched_rows) > 0:
        error_message = "Os seguintes municípios falharam: \n"
        for row in mismatched_rows.rows(named=True):
            error_message += f"{row['nome_denatran']} ({row['sigla_uf']})\n"
        raise ValueError(error_message)


def get_city_name_ibge(denatran_name: str, ibge_uf: pl.DataFrame) -> str:
    """Gets the closest match to the denatran name of the municipality in the IBGE dataframe of the same state.
    This ibge_uf dataframe is pulled directly from Base dos Dados to ensure correctness.
    Returns either the match or an empty string in case no match is found.


    Args:
        denatran_name (str): The name of the municipality according to DENATRAN data.
        ibge_uf (pl.DataFrame): Dataframe with the information from municipalities for a certain state (UF).
    Returns:
        str: Closest match to the denatran name or an empty string if no such match is found.
    """
    matches = difflib.get_close_matches(
        denatran_name.lower(), ibge_uf["nome"].str.to_lowercase(), n=1
    )
    if matches:
        return matches[0]
    else:
        return ""  # I don't want this to error out directly, because then I can get all municipalities.


def download_file(url, filename):
    # Send a GET request to the URL

    new_url = url.replace("arquivos-denatran", "arquivos-senatran")
    response = requests.get(new_url, headers=HEADERS)
    # Save the contents of the response to a file
    with open(filename, "wb") as f:
        f.write(response.content)

    print(f"Download of {filename} complete")


def generic_extractor(dest_path_file: str):
    extension = dest_path_file.split(".")[-1]
    if extension == "rar":
        extractor_function = RarFile
    elif extension == "zip":
        extractor_function = ZipFile
    else:
        raise ValueError(f"Unsupported type {extension} for compressed file.")
    with extractor_function(dest_path_file, "r") as f:
        compressed_filename = f.filename
        compressed_filename_split = compressed_filename.split("/")
        directory = "/".join(
            compressed_filename_split[: len(compressed_filename_split) - 1]
        )
        for file in f.infolist():
            print(file)
            if (
                re.search("UF|municipio", file.filename, re.IGNORECASE)
                and not file.is_dir()
            ):
                new_extension = file.filename.split(".")[-1]
                new_filename = f"{f.filename.split('.')[0]}.{new_extension}"
                f.extract(file.filename, path=directory)
                os.rename(f"{directory}/{file.filename}", new_filename)


def make_filename(i: dict, ext: bool = True) -> str:
    """Creates the filename using the sent dictionary.

    Args:
        i (dict): Dictionary with all the file's info.
        ext (bool, optional): Specifies if the generated file name needs the filetype at the end. Defaults to True.

    Returns:
        str: The full filename.
    """
    txt = asciify(i["txt"])
    mes = i["mes"]
    ano = i["ano"]
    if ano == 2013:
        # Need to treat this specific difference because name is misleading. This should solve
        txt = txt.replace("tipo", "uf e tipo")
        txt = txt.replace("Municipio", "municipio e tipo")
    directory = i["destination_dir"]
    filetype = i["filetype"]
    filename = re.sub("\\s+", "_", txt, flags=re.UNICODE).lower()
    filename = f"{directory}/{filename}_{mes}-{ano}"
    if ext:
        filename += f".{filetype}"
    return filename


def call_downloader(i: dict):
    filename = make_filename(i)
    if i["filetype"] in ["xlsx", "xls"]:
        download_file(i["href"], filename)
    elif i["filetype"] == "zip" or i["filetype"] == "rar":
        download_file(i["href"], filename)
        generic_extractor(filename)


def make_filename_pre_2012(
    type_of_file: str, year: int, filename: str, year_dir_name: str, month: int
):
    if type_of_file == "Tipo":
        basic_filename = UF_TIPO_BASIC_FILENAME
        if year > 2005:
            regex_to_search = r"UF\s+([^\s\d]+\s*)*([12]\d{3})"
        else:
            regex_to_search = rf"UF[_\s]?([^\s\d]+\s*)_{str(year)[2:4]}"
    elif type_of_file == "Munic":
        basic_filename = MUNIC_TIPO_BASIC_FILENAME
        if year > 2003:
            regex_to_search = rf"Munic\.?\s*(.*?)\s*\.?{year}"
        else:
            regex_to_search = rf"Mun\w*_(.*?)_{str(year)[2:4]}"
    else:
        raise ValueError
    match = re.search(regex_to_search, filename)
    if match:
        if (year == 2004 or year == 2005) and type_of_file == "Munic":
            month_value = int(match.group(1))
        else:
            month_in_file = match.group(1).lower().replace(".", "")
            month_value = MONTHS.get(month_in_file) or MONTHS_SHORT.get(month_in_file)
        extension = filename.split(".")[-1]
        new_filename = (
            f"{year_dir_name}/{basic_filename}_{month_value}-{year}.{extension}"
        )
        if month_value == month:
            return new_filename


def extract_links_post_2012(month: int, year: int, directory: str) -> list[dict]:
    """Extracts links of the Denatran files post 2012.

    Args:
        year (int): A year starting from 2013 onwards.
        month (int): A month from 1 to 12.
    """
    url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-{year}"
    soup = BeautifulSoup(urlopen(url), "html.parser")
    # Só queremos os dados de frota nacional.
    nodes = soup.select("p:contains('rota por ') > a")
    valid_links = []
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
                    "destination_dir": directory,
                }
                valid_links.append(info)
    return valid_links


def extraction_pre_2012(month: int, year: int, year_dir_name: str, zip_file: str):
    # Aí depois eu preciso andar pelo zip:
    print("Bom dia")
    with ZipFile(zip_file, "r") as g:
        compressed_files = [file for file in g.infolist() if not file.is_dir()]
        new_filename = None
        for file in compressed_files:
            filename = file.filename.split("/")[-1]
            if re.search("Tipo", filename, re.IGNORECASE) or re.search(
                r"Tipo[-\s]UF", zip_file.split("/")[-1]
            ):
                new_filename = make_filename_pre_2012(
                    "Tipo", year, filename, year_dir_name, month
                )
            elif re.search(r"Mun\w*", filename, re.IGNORECASE):
                new_filename = make_filename_pre_2012(
                    "Munic", year, filename, year_dir_name, month
                )
            if new_filename:
                g.extract(file, path=year_dir_name)
                os.rename(f"{year_dir_name}/{file.filename}", new_filename)
    print("Bom dia")


def make_dir_when_not_exists(dir_name: str):
    """Auxiliary function to create a subdirectory when it is not present.

    Args:
        dir_name (str): Name of the subdirectory to be created.
    """
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def call_r_to_read_file(file: str) -> pd.DataFrame:
    # Install and load the required R packages
    if not os.path.isfile(file):
        raise ValueError('Invalid file')
    packages = ("readxl",)
    r_utils = rpackages.importr("utils", suppress_messages=True)
    r_utils.chooseCRANmirror(ind=1)
    r_utils.install_packages(StrVector(packages))
    rpackages.importr("readxl", suppress_messages=True)

    # Read the Excel file
    robjects.r(
        f"""
        library(readxl)

        sheets <- excel_sheets('{file}')
        correct_sheet <- sheets[sheets != "Glossário"][1]
        df <- read_excel('{file}', sheet = correct_sheet)

    """
    )

    # Convert the R dataframe to a pandas dataframe
    df = robjects.r["df"]
    df = pd.DataFrame(dict(zip(df.names, list(df))))
    return df
