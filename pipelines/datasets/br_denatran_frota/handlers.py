# -*- coding: utf-8 -*-
"""
Handlers for br_denatran_frota.

This is merely a way to have functions that are called by tasks but can be tested with unit tests and debugging.
"""

import os
import re
from zipfile import ZipFile

import basedosdados as bd
import pandas as pd
import polars as pl
from string_utils import asciify

from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.utils import (
    DenatranType,
    call_downloader,
    call_r_to_read_excel,
    change_df_header,
    download_file,
    extract_links_post_2012,
    extraction_pre_2012,
    get_data_from_prod,
    get_year_month_from_filename,
    guess_header,
    make_dir_when_not_exists,
    treat_uf,
    verify_total,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log, to_partitions

MONTHS = constants.MONTHS.value
DATASET = constants.DATASET.value
DICT_UFS = constants.DICT_UFS.value
OUTPUT_PATH = constants.OUTPUT_PATH.value
MONTHS_SHORT = constants.MONTHS_SHORT.value
UF_TIPO_BASIC_FILENAME = constants.UF_TIPO_BASIC_FILENAME.value
MUNIC_TIPO_BASIC_FILENAME = constants.MUNIC_TIPO_BASIC_FILENAME.value


def crawl(month: int, year: int, temp_dir: str = "") -> bool:
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")
    log("Downloading file")
    files_dir = os.path.join(temp_dir, "files")
    make_dir_when_not_exists(files_dir)
    year_dir_name = os.path.join(files_dir, f"{year}")
    make_dir_when_not_exists(year_dir_name)
    if year > 2012:
        try:
            files_to_download = extract_links_post_2012(month, year, year_dir_name)
            for file_dict in files_to_download:
                call_downloader(file_dict)
        except Exception as e:
            log(e)
            log(
                "The above error indicates that the next month denatran file was not released yet."
            )

            return False

    else:
        url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"
        filename = f"{year_dir_name}/dados_anuais.zip"
        download_file(url, filename)
        if year < 2010:
            with ZipFile(filename, "r") as f:
                f.extractall(path=f"{year_dir_name}")
            for aggregate_file in os.listdir(f"{year_dir_name}"):
                if aggregate_file != "dados_anuais.zip":
                    extraction_pre_2012(
                        month,
                        year,
                        year_dir_name,
                        os.path.join(year_dir_name, aggregate_file),
                    )
        else:
            extraction_pre_2012(month, year, year_dir_name, filename)

    return True


def treat_uf_tipo(file: str) -> pl.DataFrame:
    log(f"------- Cleaning {file}")
    valid_ufs = list(DICT_UFS.keys()) + list(DICT_UFS.values())
    filename = os.path.split(file)[1]
    try:
        correct_sheet = [
            sheet for sheet in pd.ExcelFile(file).sheet_names if sheet != "Glossário"
        ][0]
        df = pd.read_excel(file, sheet_name=correct_sheet)
    except UnicodeDecodeError:
        df = call_r_to_read_excel(file)

    new_df = change_df_header(df, guess_header(df=df, type_of_file=DenatranType.UF))
    # This is ad hoc for UF_tipo.

    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf"}, inplace=True
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    clean_df = new_df[new_df.sigla_uf.isin(valid_ufs)].reset_index(
        drop=True
    )  # Now we get all the actual RELEVANT uf data.
    year, month = get_year_month_from_filename(filename)
    # If the df is all strings, try to get numbers where it makes sense.
    clean_df.replace(" -   ", 0, inplace=True)

    # Create a reverse dictionary to replace uf names with uf sigla
    reverse_dict = {v: k for k, v in DICT_UFS.items()}
    clean_df["sigla_uf"] = clean_df["sigla_uf"].map(reverse_dict)

    # clean_df.replace()
    if all(clean_df.dtypes == "object"):
        clean_df = clean_df.apply(pd.to_numeric, errors="ignore")

    clean_pl_df = pl.from_pandas(clean_df).lazy()
    clean_pl_df = verify_total(clean_pl_df.collect())
    # Add year and month
    clean_pl_df = clean_pl_df.with_columns(
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    clean_pl_df = clean_pl_df.select(pl.exclude("TOTAL"))
    clean_pl_df = clean_pl_df.melt(
        id_vars=["ano", "mes", "sigla_uf"],
        variable_name="tipo_veiculo",
        value_name="quantidade",
    )  # Long format.

    return clean_pl_df


def output_file_to_parquet(df: pl.DataFrame, filename: str) -> None:
    make_dir_when_not_exists(OUTPUT_PATH)

    pd_df = df.to_pandas()

    to_partitions(
        pd_df,
        partition_columns=["ano", "mes"],
        savepath=OUTPUT_PATH,
        file_type="parquet",
    )
    return OUTPUT_PATH


def get_desired_file(year: int, download_directory: str, filetype: str) -> str:
    log(f"-------- Accessing download directory {download_directory}")
    directory_to_search = os.path.join(download_directory, "files", f"{year}")

    for file in os.listdir(directory_to_search):
        if re.search(filetype, file) and file.split(".")[-1] in [
            "xls",
            "xlsx",
        ]:
            log(f"-------- The file {file} was selected")
            return os.path.join(directory_to_search, file)
    raise ValueError("No files found buckaroo")


def get_latest_data(table_id: str, dataset_id: str):
    denatran_data = get_api_most_recent_date(
        table_id=table_id, dataset_id=dataset_id, date_format="%Y-%m"
    )

    log(f"{denatran_data}")
    year = denatran_data.year
    month = denatran_data.month

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    log(f"Ano: {year}, mês: {month}")
    # return year, month

    return 2023, 12


def treat_municipio_tipo(file: str) -> pl.DataFrame:
    log(f"------- Cleaning {file}")

    bd_municipios = bd.read_sql(
        "select nome, id_municipio, sigla_uf from `basedosdados.br_bd_diretorios_brasil.municipio`",
        from_file=True,
    )

    bd_municipios = pl.from_pandas(bd_municipios)

    filename = os.path.split(file)[1]
    year, month = get_year_month_from_filename(filename)
    correct_sheet = [
        sheet for sheet in pd.ExcelFile(file).sheet_names if sheet != "Glossário"
    ][0]
    df = pd.read_excel(file, sheet_name=correct_sheet)
    # Some very janky historical files have an entire first empty column that will break EVERYTHING
    # This checks if they exist and drops them
    if df[df.columns[0]].isnull().sum() == len(df):
        df.drop(columns=df.columns[0], inplace=True)
    new_df = change_df_header(df, guess_header(df, DenatranType.Municipio))
    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf", new_df.columns[1]: "nome_denatran"},
        inplace=True,
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    new_pl_df = pl.from_pandas(new_df)
    new_pl_df = verify_total(new_pl_df)
    new_pl_df = new_pl_df.with_columns(
        pl.col("nome_denatran").apply(asciify).str.to_lowercase(),
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    new_pl_df = new_pl_df.filter(pl.col("nome_denatran") != "municipio nao informado")
    if new_pl_df.shape[0] > bd_municipios.shape[0]:
        raise ValueError(
            f"Atenção: a base do Denatran tem {new_pl_df.shape[0]} linhas e isso é mais municípios do que a BD com {bd_municipios.shape[0]}"
        )
    dfs = []
    for uf in DICT_UFS:
        dfs.append(treat_uf(new_pl_df, bd_municipios, uf))
    full_pl_df = pl.concat(dfs)
    full_pl_df = full_pl_df.select(
        pl.exclude("TOTAL", "suggested_nome_ibge", "nome_denatran")
    )
    full_pl_df = full_pl_df.melt(
        id_vars=["ano", "mes", "sigla_uf", "id_municipio"],
        variable_name="tipo_veiculo",
        value_name="quantidade",
    )  # Long format.
    return full_pl_df


def should_process_data(bq_year: int, bq_month: int, filename: str) -> bool:
    """Verify if the crawled data is new enough to be uploaded.

    Args:
        bq_year (int): Year gotten from get_latest_data, comes from BQ data.
        bq_month (int): Month gotten from get_latest_data, comes from BQ data.
        filename (str): Name of the DENATRAN data file downloaded.

    Returns:
        bool: Whether or not the data obtained from the website is recent enough to be updated.
    """
    crawled_year, crawled_month = get_year_month_from_filename(filename)
    if crawled_year > bq_year or (
        crawled_year == bq_year and crawled_month >= bq_month
    ):
        return True
    else:
        return False
