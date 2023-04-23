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
import requests
from pipelines.datasets.br_denatran_frota.constants import constants

DICT_UFS = constants.DICT_UFS.value
SUBSTITUTIONS = constants.SUBSTITUTIONS.value
HEADERS = constants.HEADERS.value


def guess_header(df: pd.DataFrame, max_header_guess: int = 4) -> int:
    header_guess = 0
    while header_guess < max_header_guess:
        # Iffy logic, but essentially: if all rows of the column are strings, then this is a good candidate for a header.
        if all(df.iloc[header_guess].apply(lambda x: isinstance(x, str))):
            return header_guess

        header_guess += 1
    return 0  # If nothing is ever found until the max, let's just assume it's the first row as per usual.


def change_df_header(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    new_header = df.iloc[header_row]
    new_df = df[(header_row + 1) :].reset_index(drop=True)
    new_df.rename(columns=new_header, inplace=True)
    return new_df


def get_year_month_from_filename(filename: str) -> tuple[int, int]:
    match = re.search(r"(\w+)_(\d{1,2})-(\d{4})\.(xls|xlsx)$", filename)
    if match:
        month = match.group(2)
        year = match.group(3)
        return month, year
    else:
        raise ValueError("No match found")


def verify_total(df: pl.DataFrame) -> None:
    columns_for_total = df.select(pl.exclude("TOTAL")).select(pl.exclude([pl.Utf8]))
    calculated_total = columns_for_total.select(
        pl.fold(
            acc=pl.lit(0), function=lambda acc, x: acc + x, exprs=pl.col("*")
        ).alias("calculated_total")
    )["calculated_total"]

    mask = df["TOTAL"] == calculated_total
    if pl.sum(~mask) != 0:
        raise ValueError(
            "A coluna de TOTAL da base original tem inconsistências e não soma tudo das demais colunas."
        )


def fix_suggested_nome_ibge(row) -> str:
    key = (row[0], row[1])
    if key in SUBSTITUTIONS:
        return SUBSTITUTIONS[key]
    else:
        return row[-1]


def match_ibge(denatran_uf: pl.DataFrame, ibge_uf: pl.DataFrame) -> None:
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


def make_dir_when_not_exists(dir_name: str):
    """Auxiliary function to create a subdirectory when it is not present.

    Args:
        dir_name (str): Name of the subdirectory to be created.
    """
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
