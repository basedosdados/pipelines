# -*- coding: utf-8 -*-
"""
Handlers for br_denatran_frota.

This is merely a way to have functions that are called by tasks but can be tested with unit tests and debugging.
"""

import os
import re
import basedosdados as bd
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.utils import (
    make_dir_when_not_exists,
    extract_links_post_2012,
    verify_total,
    change_df_header,
    guess_header,
    get_year_month_from_filename,
    call_downloader,
    download_file,
    extraction_pre_2012,
    call_r_to_read_excel,
    treat_uf,
    DenatranType,
)
import pandas as pd
import polars as pl
from string_utils import asciify
from zipfile import ZipFile
from pipelines.utils.utils import (
    clean_dataframe,
    to_partitions,
    log,
)

MONTHS = constants.MONTHS.value
DATASET = constants.DATASET.value
DICT_UFS = constants.DICT_UFS.value
OUTPUT_PATH = constants.OUTPUT_PATH.value
MONTHS_SHORT = constants.MONTHS_SHORT.value
UF_TIPO_BASIC_FILENAME = constants.UF_TIPO_BASIC_FILENAME.value
MUNIC_TIPO_BASIC_FILENAME = constants.MUNIC_TIPO_BASIC_FILENAME.value


def crawl(month: int, year: int, temp_dir: str = "") -> None:
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")
    files_dir = os.path.join(temp_dir, "files")
    make_dir_when_not_exists(files_dir)
    year_dir_name = os.path.join(files_dir, f"{year}")
    make_dir_when_not_exists(year_dir_name)
    if year > 2012:
        files_to_download = extract_links_post_2012(month, year, year_dir_name)
        for file_dict in files_to_download:
            call_downloader(file_dict)
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


def treat_uf_tipo(file: str) -> pl.DataFrame:
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
    month, year = get_year_month_from_filename(filename)
    # If the df is all strings, try to get numbers where it makes sense.
    if all(clean_df.dtypes == "object"):
        clean_df = clean_df.apply(pd.to_numeric, errors="ignore")
    clean_pl_df = pl.from_pandas(clean_df).lazy()
    verify_total(clean_pl_df.collect())
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
    clean_pl_df = clean_pl_df.collect()
    return clean_pl_df


def output_file_to_csv(df: pl.DataFrame, filename: str) -> None:
    make_dir_when_not_exists(OUTPUT_PATH)
    df.write_csv(file=f"{OUTPUT_PATH}/{filename}.csv", has_header=True)
    return OUTPUT_PATH


def get_desired_file(year: int, download_directory: str, filetype: str) -> str:
    directory_to_search = os.path.join(download_directory, "files", f"{year}")
    log(f"Directory: {directory_to_search}")
    for file in os.listdir(directory_to_search):
        if re.search(filetype, file) and file.split(".")[-1] in [
            "xls",
            "xlsx",
        ]:
            log(f"File: {file}")
            return os.path.join(directory_to_search, file)
    raise ValueError("No files found buckaroo")


def treat_municipio_tipo(file: str) -> pl.DataFrame:
    municipios_query = """SELECT nome, id_municipio, sigla_uf FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    bd_municipios = bd.read_sql(
        municipios_query, "tamir-pipelines"
    )  # Hardcoded, not good.
    bd_municipios = pl.from_pandas(bd_municipios)

    filename = os.path.split(file)[1]
    month, year = get_year_month_from_filename(filename)
    df = pd.read_excel(file)
    new_df = change_df_header(df, guess_header(df, DenatranType.Municipio))
    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf", new_df.columns[1]: "nome_denatran"},
        inplace=True,
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    new_pl_df = pl.from_pandas(new_df)
    verify_total(new_pl_df)
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
    return pl.concat(dfs)
