"""
General purpose functions for the br_denatran_frota project.
"""

import datetime
import difflib
import os
import re
from enum import Enum
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

import basedosdados as bd
import pandas as pd
import polars as pl
import requests
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from bs4 import BeautifulSoup
from rarfile import RarFile
from rpy2.robjects.vectors import StrVector
from string_utils import asciify

from pipelines.datasets.br_denatran_frota.constants import (
    constants as denatran_constants,
)
from pipelines.utils.utils import log


class DenatranType(Enum):
    Municipio = "Municipio"
    UF = "UF"


def update_yearmonth(year: str | int, month: str | int):
    if isinstance(year, str):
        year = int(year)
    if isinstance(month, str):
        month = int(month)

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    return year, month


def guess_header(
    df: pd.DataFrame, type_of_file: DenatranType, max_header_guess: int = 10
) -> int:
    """Search for expected header in dataframe.

    Args:
        df (pd.DataFrame): Dataframe whose header we don't know
        type_of_file (str): _description_
        max_header_guess (int, optional): _description_. Defaults to 10.

    Raises:
        ValueError: _description_

    Returns:
        int: _description_
    """
    possible_headers = []
    if type_of_file == DenatranType.UF:
        expected_header = denatran_constants.UF_TIPO_HEADER.value
    elif type_of_file == DenatranType.Municipio:
        expected_header = denatran_constants.MUNICIPIO_TIPO_HEADER.value
    else:
        raise ValueError("Unrecognized type of dataframe.")
    current_header = [c for c in df.columns]
    equal_column_names = [
        (x, y)
        for x, y in zip(expected_header, current_header, strict=False)
        if x == y
    ]
    if len(equal_column_names) / len(expected_header) >= 0.6:
        return -1
    header_guess = 0
    while header_guess < max_header_guess:
        if len(df) - 1 < header_guess:
            break
        current_row = df.iloc[header_guess].to_list()
        equal_column_names = [
            (x, y)
            for x, y in zip(expected_header, current_row, strict=False)
            if x == y
        ]
        if (
            len(equal_column_names) / len(expected_header) >= 0.6
        ):  # If 60% of the columns match, this is the header.
            possible_headers.append(header_guess)
        header_guess += 1
    if possible_headers:
        return max(possible_headers)
    return 0  # If nothing is ever found until the max, let's just assume it's the first row as per usual.


def change_df_header(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """Change the dataframe's header to a row inside of it and returns the corrected df.

    Ideally, to be used in conjunction with guess_header().
    Args:
        df (pd.DataFrame): Dataframe whose header we want changed.
        header_row (int): Index of the row where the header is located.

    Returns:
        pd.DataFrame: Returns the same dataframe but with the corrected header
    """
    if header_row == -1:
        return df
    new_header = df.iloc[header_row]
    new_df = df[(header_row + 1) :].reset_index(drop=True)
    new_df = new_df.rename(columns=new_header)
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
    """Extract month and year information from files named indicator_month-year.xls.

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
        log(f"{year} && {month}")
        return int(year), int(month)
    else:
        raise ValueError("No match found")


def verify_total(df: pl.DataFrame) -> pl.DataFrame:
    """
    Verify that we can pivot from wide to long.

    Essentially, gets a Wide dataframe, excludes all string columns and the TOTAL column and sums it all row wise.
    Some historical data from Denatran seems to have a rounding error.
    Thus, we need to verify if the calculated total column is greater or equal than the original TOTAL column.
    If not, raises an error.

    Args:
        df (pl.DataFrame): Wide format dataframe to verify.

    Raises:
        ValueError: Errors out if the sum of the columns is actually different than the total.

    """

    def calculate_total(df):
        columns_for_total = df.select(
            [col for col in df.columns if df[col].dtype != pl.Utf8]
        ).select(pl.exclude("TOTAL"))
        calculated_total = columns_for_total.select(
            pl.fold(
                acc=pl.lit(0),
                function=lambda acc, x: acc + x,
                exprs=pl.col("*"),
            ).alias("calculated_total")
        )["calculated_total"]
        return calculated_total

    calculated_total = calculate_total(df)
    mask = df["TOTAL"] != calculated_total

    if mask.sum() != 0:
        # In some cases, one of the columsn in the data is multiplied by 10, and this will correct it before you
        columns_to_try = df.select(
            [col for col in df.columns if df[col].dtype != pl.Utf8]
        ).select(pl.exclude("TOTAL"))

        for col in columns_to_try.columns:
            new_df = clean_dirty_data(df=df, column_name=col)
            new_calculated_total = calculate_total(new_df)
            mask = new_df["TOTAL"] != new_calculated_total
            if mask.sum() == 0:
                return new_df
        if mask.sum() != 0:
            raise ValueError(
                "A coluna de TOTAL da base original tem inconsistências e é diferente da soma das demais colunas."
            )
    else:
        return df


def clean_dirty_data(
    df: pl.DataFrame, column_name: str = "QUADRICICLO"
) -> pl.DataFrame:
    """
    Divide the column_name column by 10 when the data is dirty.

    Args:
        df (pl.DataFrame): Denatran data to be cleaned

    Returns:
        pl.DataFrame: Cleaned Denatran data with the column_name column altered.
    """
    old_column = df[column_name]
    new_column = old_column / 10
    return df.with_columns(new_column.cast(pl.Int32).alias(column_name))


def fix_suggested_nome_ibge(row: tuple[str, ...]) -> str:
    """
    Get a row from a dataframe and applies the SUBSTITUTIONS constant ruleset where applicable.

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
    if key in denatran_constants.SUBSTITUTIONS.value:
        return denatran_constants.SUBSTITUTIONS.value[key]
    else:
        return row[-1]


def verify_match_ibge(
    denatran_uf: pl.DataFrame, ibge_uf: pl.DataFrame
) -> None:
    """
    Take a dataframe of the Denatran data and an IBGE dataframe of municipalities.

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
    return joined_df


def get_city_name_ibge(denatran_name: str, ibge_uf: pl.DataFrame) -> str:
    """
    Get the closest match to the denatran name of the municipality in the IBGE dataframe of the same state.

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


def download_file(url: str, filename: str) -> None:
    """
    Download a file from a url using a simple open + get request.

    Only necessary because some problematic URLs report a 404 despite the files being there.

    Args:
        url (str): URL where the desired file is located.
        filename (str): Filename to write the contents of the HTTP request to.
    """
    # Send a GET request to the URL

    new_url = url.replace("arquivos-denatran", "arquivos-senatran")
    log(new_url)
    response = requests.get(new_url, headers=denatran_constants.HEADERS.value)
    response.raise_for_status()
    # Save the contents of the response to a file
    with open(filename, "wb") as f:
        f.write(response.content)

    log(f"Download of {filename} complete")


def verify_file(
    url: str,
) -> bool:
    """
    Task to verify if a file from a url exists.

    Only necessary because some problematic URLs report a 404 despite the files being there.

    Args:
        url (str): URL where the desired file is located.
    """
    # Send a GET request to the URL
    log(f"Verify {url}")
    new_url = url.replace("arquivos-denatran", "arquivos-senatran")
    log(new_url)
    response = requests.get(new_url, headers=denatran_constants.HEADERS.value)
    return response.status_code == 200


def generic_extractor(dest_path_file: str):
    """
    Extracts the desired DENATRAN compressed files from .rar or .zip file.

    Args:
        dest_path_file (str): Filepath of the compressed file whose contents should be extracted.

    Raises:
        ValueError: If the passed argument is not a .rar or .zip file.
    """
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
            log(file)
            if (
                re.search("UF|munic", file.filename, re.IGNORECASE)
                and not file.is_dir()
            ):
                new_extension = file.filename.split(".")[-1]
                new_filename = f"{f.filename.split('.')[0]}.{new_extension}"
                f.extract(file.filename, path=directory)
                os.rename(f"{directory}/{file.filename}", new_filename)


def make_file_path(file_info: dict, ext: bool = True) -> str:
    """Create the filename using the sent dictionary.

    Args:
        file_info (dict): Dictionary with all the file's info.
        ext (bool, optional): Specifies if the generated file name needs the file
            extension at the end. Defaults to True.

    Returns:
        str: The full filename.
    """
    if "ano" in file_info:
        year = int(file_info["ano"])
        ## Make file path post 2012
        if year > 2012:
            raw_file_name = asciify(file_info["raw_file_name"])
            if year == 2013:
                # Need to treat this specific difference because name is misleading. This should solve
                raw_file_name = raw_file_name.replace(
                    "tipo", "uf e tipo"
                ).replace("Municipio", "municipio e tipo")
            file_name = (
                re.sub(r"\d+|\-", "", raw_file_name, flags=re.UNICODE)
                .lower()
                .strip()
            )
            file_name = re.sub(
                "\\s+", "_", raw_file_name, flags=re.UNICODE
            ).lower()
            file_path = (
                file_info["destination_dir"]
                + f"/{file_name}"
                + f"_{file_info['mes']}"
                + f"-{year}"
            )
            if ext:
                file_path += f".{file_info['file_extension']}"
            log(f"FILEPATH:{file_path}")
            return file_path

        ## Make file path pre 2012
        else:
            if file_info["type_of_file"] == DenatranType.UF:
                raw_filename = denatran_constants.UF_TIPO_BASIC_FILENAME.value
                if year > 2005:
                    regex_to_search = r"UF\s+([^\s\d]+\s*)*([12]\d{3})"
                else:
                    regex_to_search = (
                        rf"UF[_\s]?([^\s\d]+\s*)_{str(year)[2:4]}"
                    )
            elif file_info["type_of_file"] == DenatranType.Municipio:
                raw_filename = (
                    denatran_constants.MUNIC_TIPO_BASIC_FILENAME.value
                )
                if year > 2003:
                    regex_to_search = rf"Munic\.?\s*(.*?)\s*\.?{year}"
                else:
                    regex_to_search = rf"Mun\w*_(.*?)_{str(year)[2:4]}"
            else:
                log(file_info)
                raise ValueError
            match = re.search(regex_to_search, file_info["raw_file_name"])

            if match:
                if (year == 2004 or year == 2005) and file_info[
                    "type_of_file"
                ] == DenatranType.Municipio:
                    month_value = int(match.group(1))
                else:
                    month_in_file = match.group(1).lower().replace(".", "")
                    month_value = denatran_constants.MONTHS.value.get(
                        str(month_in_file).lower()
                    ) or denatran_constants.MONTHS_SHORT.value.get(
                        str(month_in_file).lower()
                    )
                if file_info["file_extension"] == "":
                    extension = str(file_info["raw_file_name"]).split(".")[-1]
                else:
                    extension = file_info["file_extension"]
                file_path = (
                    file_info["destination_dir"]
                    + f"/{raw_filename}_{month_value}-{year}.{extension}"
                )
                if month_value == int(file_info["mes"]):
                    log(f"FILEPATH:{file_path}")
                    return file_path


def call_downloader(file_info: dict):
    """
    Call other download functions in the correct order according to file extension.

    Args:
        file_info (dict): Dictionary with the file's information.
    """
    file_path = make_file_path(file_info)
    log(f"File path: {file_path}")
    if file_info["file_extension"] in ["xlsx", "xls"]:
        download_file(file_info["file_url"], file_path)
    elif (
        file_info["file_extension"] == "zip"
        or file_info["file_extension"] == "rar"
    ):
        download_file(file_info["file_url"], file_path)
        generic_extractor(file_path)


def extract_links_post_2012(
    month: int, year: int, directory: str | Path
) -> list[dict]:
    """Extract links of the Denatran files post 2012.

    Args:
        year (int): A year starting from 2013 onwards.
        month (int): A month from 1 to 12.
    """
    url = f"{denatran_constants.BASE_URL_POST_2012.value}/frota-de-veiculos-{year}"
    soup = BeautifulSoup(urlopen(url), "html.parser")

    ## First level of html elements
    # Searching for html elements with text 'frota'
    tags = soup.find_all(
        lambda tag: (tag.name == "div" and "pageBreak" in tag.get("class", []))
        or tag.name == "p"
    )
    tags_of_interest = [
        tag
        for tag in tags
        if (
            tag.a
            and not re.search(
                re.compile(
                    r"Q*uantidade\s*.+",
                    re.I,
                ),
                tag.a.text,
            )
            and re.search(
                re.compile(
                    r"(F*rota\s*(por)*\s*(UF\s*e*\s*Tipo\s*(de)*\s*Veículo)|(Município\s*e*\s*Tipo))",
                    re.I,
                ),
                tag.a.text,
            )
        )
        or (
            tag.find("strong")
            and re.search(
                re.compile(r"F*rota\s*.+", re.I), tag.find("strong").text
            )
            and not re.search(
                re.compile(r"Q*uantidade\s*.+", re.I), tag.find("strong").text
            )
        )
    ]
    tags_of_interest.reverse()
    valid_links = []
    matched_year = None
    matched_month = None
    matched_date = datetime.date(year=year, month=month, day=1)
    ref_date = datetime.date(year=year, month=month, day=1)
    i = 0
    while i < len(tags_of_interest) and matched_date <= ref_date:
        current_tag = tags_of_interest[i]
        title_index = i + 1
        parent_txt = tags_of_interest[
            title_index if title_index < len(tags_of_interest) else i
        ].get_text(" ", strip=True)

        # Parent node text contains month and year info
        parent_match = re.search(
            r"(?i)\(\s*([\w]+)(?:\s\w*\s*)(\d{4})\s*\)", parent_txt
        )

        # Try again for subsection title
        if not parent_match and i < len(tags_of_interest) + 2:
            title_index += 1
            parent_txt = tags_of_interest[title_index].get_text(
                " ", strip=True
            )
            # Parent node text contains month and year info
            parent_match = re.search(
                r"(?i)\(\s*([\w]+)(?:\s\w*\s*)(\d{4})\s*\)", parent_txt
            )
        if parent_match:
            matched_month = parent_match.group(1)
            matched_year = parent_match.group(2)
            matched_date = datetime.date(
                year=int(matched_year),
                month=int(
                    denatran_constants.MONTHS.value.get(
                        str(matched_month).lower()
                    )
                ),
                day=1,
            )

        if current_tag.find("a"):
            ## Second level of html elements
            for child_node in current_tag.select("a"):
                txt = child_node.text
                if "quantidade" not in txt.lower():
                    file_url = child_node.get("href")
                    match = re.search(
                        r"(?i)([\w\-\d\/]+)\.(xls|xlsx|rar|zip)$",
                        file_url,
                    )
                    # 'Municipio' type of file contains 'município' in its html element's text
                    search_municipio = re.search(
                        "município", txt, flags=re.IGNORECASE
                    )
                    # 'UF' type of file contains 'UF' or 'tipo' in its html element's text
                    search_uf = re.search("tipo|uf", txt, flags=re.IGNORECASE)
                    if (
                        match
                        and (search_uf or search_municipio)
                        and denatran_constants.MONTHS.value.get(
                            str(matched_month).lower()
                        )
                        == int(month)
                        and matched_year == str(year)
                    ):
                        # All file metadata aggregated at the info dict
                        info = {
                            "raw_file_name": txt,
                            "file_url": file_url,
                            "mes": month,
                            "ano": year,
                            "file_extension": match.group(2).lower(),
                            "type_of_file": DenatranType.Municipio
                            if search_municipio
                            else DenatranType.UF,
                            "destination_dir": str(directory),
                        }
                        valid_links.append(info)
        i += 1
    log(f"There are {len(valid_links)} valid links.")
    return valid_links


def extraction_pre_2012(
    month: int, year: int, year_dir_name: str, zip_file: str
):
    """
    Args:
        month (int): _description_
        year (int): _description_
        year_dir_name (str): _description_
        zip_file (str): _description_
    """
    # Aí depois eu preciso andar pelo zip:
    if not zip_file.endswith(".zip"):
        return
    with ZipFile(zip_file, "r") as g:
        compressed_files = [file for file in g.infolist() if not file.is_dir()]
        for _, file in enumerate(compressed_files):
            file_path = None
            split_file_name = file.filename.split("/")[-1]
            split_file_name = split_file_name.split(".")
            raw_file_name = ".".join([split for split in split_file_name[:-1]])
            file_info = {
                "raw_file_name": raw_file_name,
                "file_url": None,
                "mes": month,
                "ano": year,
                "file_extension": split_file_name[-1],
                "type_of_file": "",
                "destination_dir": str(year_dir_name),
            }

            if re.search("Tipo", raw_file_name, re.IGNORECASE) or re.search(
                r"Tipo[-\s]UF", zip_file.split("/")[-1]
            ):
                file_info["type_of_file"] = DenatranType.UF
            elif re.search(r"Mun\w*", raw_file_name, re.IGNORECASE):
                file_info["type_of_file"] = DenatranType.Municipio

            try:
                file_path = make_file_path(file_info=file_info, ext=True)
            except Exception as e:
                log(e)
            if file_path:
                log(file_path)
                g.extract(file, path=year_dir_name)
                os.rename(f"{year_dir_name}/{file.filename}", file_path)


def call_r_to_read_excel(file: str) -> pd.DataFrame:
    """Use rpy2 to call R's readxl for problematic Excel files and then keep reading them as dataframes.

    Args:
        file (str): The full filepath that needs to be opened.

    Raises:
        ValueError: If the desired file is not an actual file.

    Returns:
        pd.DataFrame: Returns a pandas dataframe with the excel file's content.
    """
    if not os.path.isfile(file):
        raise ValueError("Invalid file")
    packages = "readxl"
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

    df = pd.DataFrame(dict(zip(df.names, list(df), strict=False)))
    return df


def treat_uf(
    denatran_df: pl.DataFrame, ibge_df: pl.DataFrame, uf: str
) -> None:
    """Take the DENATRAN data at municipality level and compare it to the IBGE data.

    This will filter by the uf argument and do all comparisons to ensure consistency.

    Args:
        denatran_df (pl.DataFrame): Dataframe with the DENATRAN data at municipality level.
        ibge_df (pl.DataFrame): Dataframe with the IBGE data of municipalities.
        uf (str): Desired municipality to filter the DF for.

    Raises:
        ValueError: If there are somehow municipalities in the DENATRAN data that do not exist in the IBGE data. Very unlikely.
        ValueError: If there are two municipalities in the DENATRAN data with the same IBGE name in the same state.
    """
    denatran_uf = denatran_df.filter(pl.col("sigla_uf") == uf)
    ibge_uf = ibge_df.filter(pl.col("sigla_uf") == uf)
    ibge_uf = ibge_uf.with_columns(
        pl.col("nome").apply(asciify).str.to_lowercase()
    )
    municipios_na_bd = ibge_uf["nome"].to_list()
    suggested_name_ibge = denatran_uf["nome_denatran"].apply(
        lambda x: get_city_name_ibge(x, ibge_uf)
    )
    denatran_uf = denatran_uf.with_columns(
        suggested_name_ibge.alias("suggested_nome_ibge")
    )
    denatran_uf = denatran_uf.with_columns(
        denatran_uf.apply(fix_suggested_nome_ibge)["map"].alias(
            "suggested_nome_ibge"
        )
    )
    municipios_no_denatran = denatran_uf["suggested_nome_ibge"].to_list()
    d = set(municipios_no_denatran) - set(municipios_na_bd)
    municipios_duplicados = (
        denatran_uf.groupby("suggested_nome_ibge")
        .count()
        .filter(pl.col("count") > 1)
    )
    if not municipios_duplicados.is_empty():
        raise ValueError(
            f"Existem municípios com mesmo nome do IBGE em {uf}! São eles {municipios_duplicados['suggested_nome_ibge'].to_list()}"
        )
    if d:
        # This here is probably impossible and shouldn't happen due to the matching coming from the BD data.
        # The set difference might occur the other way around, but still, better safe.
        raise ValueError(f"Existem municípios em {uf} que não estão na BD.")
    return verify_match_ibge(denatran_uf, ibge_uf)


def get_data_from_prod(dataset_id: str, table_id: str) -> list:
    """Get data from a table from basedosdados.

    Args:
        dataset_id (str): Name of the dataset.
        table_id (str): _description_
        columns (list): _description_

    Returns:
        list: _description_
    """

    storage = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    blobs = list(
        storage.client["storage_staging"]
        .bucket("basedosdados-dev")
        .list_blobs(prefix=f"staging/{storage.dataset_id}/{storage.table_id}/")
    )

    dfs = []

    for blob in blobs:
        partitions = re.findall(r"\w+(?==)", blob.name)
        if len(set(partitions)) == 0:
            df = pd.read_csv(blob.public_url)
            dfs.append(df)
        else:
            columns2add = list(set(partitions))
            df = pd.read_csv(blob.public_url)
            for column in columns2add:
                df[column] = blob.name.split(column + "=")[1].split("/")[0]
            dfs.append(df)
    if dfs:
        df = pd.concat(dfs)

        return df
