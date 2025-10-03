import datetime
import os
import re
from pathlib import Path
from zipfile import ZipFile

import basedosdados as bd
import pandas as pd
import polars as pl
from prefect import task
from prefect.triggers import all_finished
from string_utils import asciify

from pipelines.datasets.br_denatran_frota.constants import (
    constants as denatran_constants,
)
from pipelines.datasets.br_denatran_frota.utils import (
    DenatranType,
    call_downloader,
    call_r_to_read_excel,
    change_df_header,
    download_file,
    extract_links_post_2012,
    extraction_pre_2012,
    get_year_month_from_filename,
    guess_header,
    output_file_to_parquet,
    treat_uf,
    update_yearmonth,
    verify_file,
    verify_total,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.utils import log


@task()
def crawl_task(
    source_max_date: datetime,
    temp_dir: str | Path = denatran_constants.DOWNLOAD_PATH.value,
) -> bool:
    """
    Main task to extract data from *frota por município e tipo* and *frota por UF e tipo*.

    Args:
        month (int): Desired month
        year (int): Desired year

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    year = source_max_date.year
    month = source_max_date.month

    if month not in denatran_constants.MONTHS.value.values():
        raise ValueError("Mês inválido.")
    log("Downloading file")
    files_dir = os.path.join(str(temp_dir), "files")
    year_dir_name = os.path.join(str(files_dir), f"{year}")
    Path(year_dir_name).mkdir(exist_ok=True, parents=True)
    if year > 2012:
        try:
            files_to_download = extract_links_post_2012(
                month, year, year_dir_name
            )
            for file_dict in files_to_download:
                call_downloader(file_dict)
        except Exception as e:
            log(e)
            log(
                "The above error indicates that the next month denatran file was not released yet."
            )

            return False

    else:
        url = f"{denatran_constants.BASE_URL_PRE_2012.value}/{year}/frota{'_' if year > 2008 else ''}{year}.zip"
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


@task(trigger=all_finished)
def get_desired_file_task(
    source_max_date: datetime, download_directory: str, filetype: str
) -> str:
    """
    Task to search for the desired file at a specific folder, being it uf_tipo or municipio_tipo

    Args:
        year (int): file year
        download_directory (str | Path): donwload directory
        filetype (str): filetype

    Raises:
        ValueError: No files found

    Returns:
        str: File path
    """
    year = source_max_date.year
    month = source_max_date.month
    log(f"-------- Accessing download directory {download_directory}")
    directory_to_search = os.path.join(
        str(download_directory), "files", f"{year}"
    )
    log(f"-------- Directory to search {directory_to_search}")

    for file in os.listdir(directory_to_search):
        if (
            re.search(filetype, file)
            and file.split(".")[-1]
            in [
                "xls",
                "xlsx",
            ]
        ) and (str(month) in file):
            log(f"-------- The file {file} was selected")
            return os.path.join(directory_to_search, file)
    raise ValueError("No files found!")


@task(trigger=all_finished)
def treat_uf_tipo_task(file) -> pl.DataFrame:
    """Task to treat data from  frota por UF e tipo.


    Args:
        file (str): path to the file to be treated

    Returns:
        pl.DataFrame: final file
    """

    log(f"------- Cleaning {file}")
    valid_ufs = list(denatran_constants.DICT_UFS.value.keys()) + list(
        denatran_constants.DICT_UFS.value.values()
    )
    filename = os.path.split(file)[1]

    try:
        correct_sheet = [  # noqa: RUF015
            sheet
            for sheet in pd.ExcelFile(file).sheet_names
            if sheet != "Glossário"
        ][0]
        df = pd.read_excel(file, sheet_name=correct_sheet)

    except UnicodeDecodeError:
        df = call_r_to_read_excel(file)

    new_df = change_df_header(
        df, guess_header(df=df, type_of_file=DenatranType.UF)
    )
    # This is ad hoc for UF_tipo.

    new_df = new_df.rename(
        columns={new_df.columns[0]: "sigla_uf"}
    )  # Rename for ease of use.

    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    clean_df = new_df[new_df.sigla_uf.isin(valid_ufs)].reset_index(
        drop=True
    )  # Now we get all the actual RELEVANT uf data.
    year, month = get_year_month_from_filename(filename)
    # If the df is all strings, try to get numbers where it makes sense.
    clean_df = clean_df.replace(" -   ", 0)

    # Create a reverse dictionary to replace uf names with uf sigla
    reverse_dict = {v: k for k, v in denatran_constants.DICT_UFS.value.items()}
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

    log("-------- Data Wrangling finished")
    output_path = output_file_to_parquet(clean_pl_df)
    return output_path


@task()
def treat_municipio_tipo_task(file: str) -> pl.DataFrame:
    """Task to treat data from  frota por UF e tipo.


    Args:
        file (str): path to the file to be treated

    Returns:
        pl.DataFrame: final file
    """

    log(f"------- Cleaning {file}")

    bd_municipios = bd.read_sql(
        "select nome, id_municipio, sigla_uf from `basedosdados.br_bd_diretorios_brasil.municipio`",
        from_file=True,
    )

    bd_municipios = pl.from_pandas(bd_municipios)

    filename = os.path.split(file)[1]

    year, month = get_year_month_from_filename(filename)
    correct_sheet = [  # noqa: RUF015
        sheet
        for sheet in pd.ExcelFile(file).sheet_names
        if sheet != "Glossário"
    ][0]

    df = pd.read_excel(file, sheet_name=correct_sheet)
    # Some very janky historical files have an entire first empty column that will break EVERYTHING
    # This checks if they exist and drops them
    if df[df.columns[0]].isna().sum() == len(df):
        df = df.drop(columns=df.columns[0])

    new_df = change_df_header(df, guess_header(df, DenatranType.Municipio))
    new_df = new_df.rename(
        columns={
            new_df.columns[0]: "sigla_uf",
            new_df.columns[1]: "nome_denatran",
        },
    )  # Rename for ease of use.

    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    new_pl_df = pl.from_pandas(new_df)
    new_pl_df = verify_total(new_pl_df)
    new_pl_df = new_pl_df.with_columns(
        pl.col("nome_denatran").apply(asciify).str.to_lowercase(),
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    new_pl_df = new_pl_df.filter(
        pl.col("nome_denatran") != "municipio nao informado"
    )
    if new_pl_df.shape[0] > bd_municipios.shape[0]:
        raise ValueError(
            f"Atenção: a base do Denatran tem {new_pl_df.shape[0]} linhas e isso é mais municípios do que a BD com {bd_municipios.shape[0]}"
        )
    dfs = []
    for uf in denatran_constants.DICT_UFS.value:
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

    log("-------- Data Wrangling finished")

    output_path = output_file_to_parquet(full_pl_df)
    return output_path


@task()
def get_latest_date_task(
    table_id: str, dataset_id: str
) -> tuple[list, list, int | None, str | None]:
    """Task to extract the latest data from available on the data source
    Args:
        table_id (str): table_id from BQ
        dataset_id (str): table_id from BQ

    Returns:
        [year, month]: most recente date
    """
    backend = bd.Backend(graphql_url=get_url("prod"))

    denatran_data = get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m",
        backend=backend,
    )

    log(f"{denatran_data}")
    year = denatran_data.year
    month = denatran_data.month
    today = datetime.datetime.now().date()

    dates = []
    dates_str = []
    year, month = update_yearmonth(year, month)
    while datetime.date(int(year), int(month), 1) <= today:
        if year > 2012:
            files_dir = os.path.join(
                str(denatran_constants.DOWNLOAD_PATH.value), "files"
            )
            year_dir_name = os.path.join(str(files_dir), f"{year}")
            try:
                files_to_download = extract_links_post_2012(
                    month, year, year_dir_name
                )
                log(f"files_to_download:{files_to_download}")
                if len(files_to_download) > 0:
                    files_to_download.sort(
                        key=lambda x: x["mes"], reverse=True
                    )
                    file_dict = files_to_download[0]
                    if verify_file(file_dict["file_url"]):
                        date_return = datetime.datetime(year, month, 1)
                        str_return = date_return.strftime("%Y-%m")
                        dates.append(date_return)
                        dates_str.append(str_return)
            except Exception as e:
                log(e, "error")
                raise
        else:
            url = f"{denatran_constants.BASE_URL_PRE_2012.value}/{year}/frota{'_' if year > 2008 else ''}{year}.zip"
            if verify_file(url):
                date_return = datetime.datetime(year, month, 1)
                str_return = date_return.strftime("%Y-%m")
                dates.append(date_return)
                dates_str.append(str_return)

        year, month = update_yearmonth(year, month)
    log(f"Ano: {year}, mês: {month}")
    return dates, dates_str, dates[0], dates_str[0]


@task()
def get_denatran_date(filename: str) -> datetime.date:
    year, month = get_year_month_from_filename(filename)
    return datetime.date(year, month, 1)
