"""
Tasks for br_me_caged
"""

import datetime
import ftplib

# pylint: disable=invalid-name
import re
from pathlib import Path

import basedosdados as bd
import pandas as pd
from dateutil.relativedelta import relativedelta
from prefect import task
from tqdm import tqdm
from unidecode import unidecode

from pipelines.constants import constants
from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_caged.utils import (
    count_competence_violations,
    count_data_rows,
    count_unknown_municipalities,
    download_file,
    month_partition_paths,
    verify_year_month,
    year_month_from_filename,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log

utils_constants = constants


@task
def build_table_paths(
    table_id: str, parent_dir: str | Path = caged_constants.DATASET_DIR.value
) -> tuple[Path, Path]:
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)

    table_dir = parent_dir / table_id
    table_dir.mkdir(exist_ok=True)
    table_input_dir = table_dir / "input"
    table_output_dir = table_dir / "output"

    table_input_dir.mkdir(exist_ok=True)
    table_output_dir.mkdir(exist_ok=True)

    return table_input_dir, table_output_dir


@task
def get_source_last_date(
    ftp_host: str = caged_constants.FTP_HOST.value,
    # pyrefly: ignore [bad-return]
) -> datetime.date:
    """
    This task reaches 'ftp.mtps.gov.br' subfolders looking for most recent year and month

    Parameters:
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")

    Returns:
        last_date (datetime): most recent date with updated content
    """
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.encoding = "latin-1"
    ftp.cwd(caged_constants.REMOTE_DIR.value)
    try:
        folder_items = ftp.nlst()
        year_folders = [
            # pyrefly: ignore [missing-attribute]
            int(re.search(r"\d{4}", item).group(0))
            for item in folder_items
            if re.search(r"\d{4}", item)
        ]

        year_folders.sort(reverse=True)
        ftp.cwd(str(year_folders[0]))
        folder_items = ftp.nlst()
        month_folders = [
            # pyrefly: ignore [missing-attribute]
            int(re.search(r"^(?:\d{4})(\d{2})$", item).group(1))
            for item in folder_items
            if re.search(r"^(?:\d{4})(\d{2})$", item)
        ]
        month_folders.sort(reverse=True)
        last_date = datetime.datetime(
            year=year_folders[0], month=month_folders[0], day=1
        )
        return last_date.date()
    except Exception as ErrorNlst:
        log(f"Unable to access CAGED subfolders due to {ErrorNlst}")


@task
def get_table_last_date(
    dataset_id: str,
    table_id: str,
) -> datetime.date:
    """
    This task gets most recent date content for a given table

    Args:
        dataset_id e table_id(string): table and dataset identification to fecth correspondent last date

    Returns:
        str: table most recent date
    """

    backend = bd.Backend(graphql_url=utils_constants.API_URL.value["prod"])

    data_api = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        backend=backend,
        date_format="%Y-%m",
    )
    log(f"Table Last Date: {data_api}")
    return data_api


def generate_year_month_range(
    start_date: str | datetime.date, end_date: str | datetime.date
) -> list:
    """
    Generate a list of year_month strings between start_date and end_date (inclusive).

    Parameters:
    start_date (str): Start date in format 'YYYYMM'
    end_date (str): End date in format 'YYYYMM'

    Returns:
    list: List of year_month strings in chronological order

    Raises:
    ValueError: If date format is incorrect or start_date is after end_date
    """
    if isinstance(start_date, datetime.date):
        start_date = start_date.strftime("%Y%m")

    if isinstance(end_date, datetime.date):
        end_date = end_date.strftime("%Y%m")

    if not (
        len(start_date) == 6
        and len(end_date) == 6
        and start_date.isdigit()
        and end_date.isdigit()
    ):
        raise ValueError("Dates must be in 'YYYYMM' format")

    start = datetime.datetime.strptime(start_date, "%Y%m")
    end = datetime.datetime.strptime(end_date, "%Y%m")

    if start > end:
        raise ValueError("Start date must be before or equal to end date")

    year_months = []
    current = start
    current += relativedelta(months=1)
    while current <= end:
        year_months.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    log(f"Yearnonths to be searched: {year_months}")

    return year_months


@task
def resolve_year_months(
    backfill_year_months: list[str] | None,
    table_last_date: datetime.date,
    source_last_date: datetime.date,
) -> list[str]:
    """Decide quais meses a execução vai carregar.

    Sem backfill, são os meses posteriores ao último coberto em produção, até o
    último publicado. Com backfill, são os meses pedidos, em ordem, e a
    cobertura não entra na conta.

    Args:
        backfill_year_months: Meses a recarregar, no formato `AAAAMM`, ou None.
        table_last_date: Último mês coberto em produção.
        source_last_date: Último mês publicado na fonte.

    Returns:
        Os meses a carregar, no formato `AAAAMM`, em ordem crescente.

    Raises:
        ValueError: Se algum mês pedido não estiver no formato `AAAAMM` ou
            ficar fora do que a fonte publica, de 202001 ao último mês
            publicado.
    """
    if not backfill_year_months:
        return generate_year_month_range(table_last_date, source_last_date)

    year_months = sorted(set(backfill_year_months))
    for year_month in year_months:
        datetime.datetime.strptime(year_month, "%Y%m")

    first_year_month = "202001"
    last_year_month = source_last_date.strftime("%Y%m")
    out_of_range = [
        year_month
        for year_month in year_months
        if not (first_year_month <= year_month <= last_year_month)
    ]
    if out_of_range:
        raise ValueError(
            f"A fonte publica de {first_year_month} a {last_year_month}, "
            f"e estes meses estão fora: {out_of_range}"
        )

    log(f"Meses do backfill: {year_months}")
    return year_months


@task
def download_caged_ftp(
    year_month: str,
    table_id: str,
    ftp_host: str = caged_constants.FTP_HOST.value,
) -> list:
    """
    Downloads specified .7z files from a CAGED dataset FTP server.

    Parameters:
        year_month (str): the month to download data from (e.g., '202301' for January 2023)
        ftp_host (str): the FTP host to connect to (default: "ftp.mtps.gov.br")

    Returns:
        List: Lists of unsuccessfully downloaded files
    """
    log(f"Verificando dados do mês: {year_month}")
    verify_year_month(year_month)
    ftp = ftplib.FTP(ftp_host)
    ftp.login()
    ftp.cwd(f"{caged_constants.REMOTE_DIR.value}/{int(year_month[0:4])}/")

    available_months = ftp.nlst()
    if year_month not in available_months:
        raise ValueError(
            f"Month {year_month} is not available in the directory for the year {year_month[0:4]}"
        )

    ftp.cwd(year_month)
    log(f"Baixando para o mês: {year_month}")

    filenames = [f for f in ftp.nlst() if f.endswith(".7z")]

    successful_downloads = []
    failed_downloads = []

    for file in filenames:
        if "CAGEDMOV" in file and table_id == "microdados_movimentacao":
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                year_month,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

        elif (
            "CAGEDFOR" in file
            and table_id == "microdados_movimentacao_fora_prazo"
        ):
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                year_month,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_fora_prazo"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

        elif (
            "CAGEDEXC" in file
            and table_id == "microdados_movimentacao_excluida"
        ):
            log(f"Baixando o arquivo: {file}")
            success, corrupt_file = download_file(
                ftp,
                year_month,
                file,
                caged_constants.DATASET_DIR.value
                / "microdados_movimentacao_excluida"
                / "input",
            )
            if success:
                successful_downloads.append(file)
            else:
                failed_downloads.append(corrupt_file)

    ftp.quit()

    log("\nDownload Summary:")
    log(f"Successfully downloaded: {successful_downloads}")
    log(f"Failed downloads: {failed_downloads}")

    if len(successful_downloads) == 0:
        log(failed_downloads)
        raise Exception("No successful downloads!")
    return failed_downloads


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def build_partitions(table_id: str, table_output_dir: str | Path) -> str:
    """
    build partitions from gtup files

    table_id: microdados_movimentacao | microdados_movimentacao_fora_prazo | microdados_movimentacao_excluida
    """
    input_files = Path(
        caged_constants.DATASET_DIR.value / table_id / "input"
    ).glob("*txt")
    for filepath in tqdm(input_files):
        log(f"Reading: {filepath}")
        filename = filepath.name
        try:
            df = pd.read_csv(filepath, sep=";", dtype={"uf": str})
            # pyrefly: ignore [missing-attribute]
            date = re.search(r"\d+", filename).group()
            ano = date[:4]
            mes = int(date[-2:])
            df.columns = [unidecode(col) for col in df.columns]
            df = df.rename(columns=caged_constants.RENAME_DICT.value)

            log(f"Renaming dataframe columns to: {df.columns}")
            df["sigla_uf"] = df["sigla_uf"].map(caged_constants.UF_DICT.value)

            for state in caged_constants.UF_DICT.value.values():
                data = df[df["sigla_uf"] == state]
                data = data.drop(
                    caged_constants.COLUMNS_TO_DROP.value[table_id],
                    axis=1,
                )
                output_dir = (
                    Path(table_output_dir)
                    / f"ano={ano}"
                    / f"mes={mes}"
                    / f"sigla_uf={state}"
                )
                columns_to_select = [
                    col
                    for col in caged_constants.COLUMNS_TO_SELECT.value
                    if col in data.columns
                ]
                output_dir.mkdir(exist_ok=True, parents=True)
                output_path = str(output_dir / "data.csv")
                data[columns_to_select].to_csv(
                    output_path,
                    index=False,
                )
                del data
            del df
        except Exception as e:
            log(f"Failed to read: {filepath} due to: {e}", "error")
    # pyrefly: ignore [bad-return]
    return table_output_dir


@task
def check_rows_written(
    table_input_dir: str | Path, table_output_dir: str | Path
) -> None:
    """Confere se as linhas de cada `.txt` foram gravadas nas partições do mês.

    Args:
        table_input_dir: Pasta com os `.txt` baixados, um por mês.
        table_output_dir: Pasta com as partições `ano=/mes=/sigla_uf=/data.csv`.

    Raises:
        ValueError: Se, em algum mês, as linhas gravadas diferirem das lidas.
    """
    files = Path(table_input_dir).glob("*txt")

    months_list = []

    for f in files:
        file_date = year_month_from_filename(f)
        partition_paths = month_partition_paths(table_output_dir, file_date)

        rows_read = count_data_rows(f)

        rows_written = 0

        for path in partition_paths:
            rows_written += count_data_rows(path)

        log(f"{file_date}: {rows_read} linhas lidas, {rows_written} gravadas")

        if rows_read != rows_written:
            months_list.append(
                f"Mês: {file_date}, "
                f"com {rows_read} linhas lidas, "
                f"mas com {rows_written} linhas escritas"
            )

    if months_list:
        raise ValueError(
            f"Contagem de linhas não bate nos seguintes meses: \n{months_list}"
        )


@task
def check_competence_month(
    table_id: str,
    table_input_dir: str | Path,
    table_output_dir: str | Path,
) -> None:
    """Confere se a competência das linhas casa com o mês do arquivo.

    Na `movimentacao`, a competência é o próprio mês do arquivo; na
    `_fora_prazo`, um mês anterior; na `_excluida`, nem a competência nem o
    mês da declaração passam do mês do arquivo.

    Args:
        table_id: Tabela do conjunto.
        table_input_dir: Pasta com os `.txt` baixados, um por mês.
        table_output_dir: Pasta com as partições `ano=/mes=/sigla_uf=/data.csv`.

    Raises:
        ValueError: Se alguma linha quebrar a regra da tabela.
    """
    files = Path(table_input_dir).glob("*txt")

    months_list = []

    for f in files:
        file_date = year_month_from_filename(f)

        violations = 0

        for path in month_partition_paths(table_output_dir, file_date):
            competences = pd.read_csv(
                path,
                usecols=lambda column: column.startswith("competencia"),
                dtype=str,
            )
            violations += count_competence_violations(
                competences, table_id, file_date
            )

        log(f"{file_date}: {violations} linhas com competência fora da regra")

        if violations:
            months_list.append(f"Mês: {file_date}, com {violations} linhas")

    if months_list:
        raise ValueError(
            "Competência fora da regra da tabela nos seguintes meses: \n"
            f"{months_list}"
        )


@task
def check_states_present(
    table_input_dir: str | Path, table_output_dir: str | Path
) -> None:
    """Confere se as 27 UFs têm linhas em todo mês.

    Args:
        table_input_dir: Pasta com os `.txt` baixados, um por mês.
        table_output_dir: Pasta com as partições `ano=/mes=/sigla_uf=/data.csv`.

    Raises:
        ValueError: Se, em algum mês, faltar alguma UF.
    """
    files_list = Path(table_input_dir).glob("*txt")

    months_list = []

    expected_ufs = set(caged_constants.UF_DICT.value.values())

    expected_ufs.remove("UF não identificada")

    for f in files_list:
        date_in_name = year_month_from_filename(f)

        partition_paths = month_partition_paths(
            table_output_dir=table_output_dir, year_month=date_in_name
        )

        partitions_ufs = set()

        for partition in partition_paths:
            if count_data_rows(partition) > 0:
                partitions_ufs.add(partition.parent.name.split("=")[1])

        missing_ufs = expected_ufs - partitions_ufs

        if missing_ufs:
            months_list.append(
                f"Para o mês {date_in_name}, "
                f"as UFs não estão presentes {sorted(missing_ufs)}"
            )

    if months_list:
        raise ValueError(f"UFs ausentes nos seguintes meses: \n{months_list}")


@task
def get_directory_municipality_codes() -> set[str]:
    """Busca os códigos de 6 dígitos do diretório de municípios.

    Returns:
        Os valores de `id_municipio_6` do `br_bd_diretorios_brasil.municipio`.
    """
    directory = bd.read_sql(
        "select id_municipio_6 "
        "from `basedosdados.br_bd_diretorios_brasil.municipio`",
        from_file=True,
    )
    return set(directory["id_municipio_6"].dropna().astype(str))


@task
def check_municipality_codes(
    table_input_dir: str | Path,
    table_output_dir: str | Path,
    directory_codes: set[str],
) -> None:
    """Confere se os códigos de município das partições existem no diretório.

    Até `MAX_UNKNOWN_MUNICIPALITY_SHARE` das linhas do mês com código fora do
    diretório, registra só um aviso no log.

    Args:
        table_input_dir: Pasta com os `.txt` baixados, um por mês.
        table_output_dir: Pasta com as partições `ano=/mes=/sigla_uf=/data.csv`.
        directory_codes: Códigos de 6 dígitos do diretório de municípios.

    Raises:
        ValueError: Se, em algum mês, os códigos fora do diretório passarem
            do limite.
    """
    files = Path(table_input_dir).glob("*txt")
    max_share = caged_constants.MAX_UNKNOWN_MUNICIPALITY_SHARE.value

    months_list = []

    for f in files:
        file_date = year_month_from_filename(f)

        rows = 0
        unknown = 0

        for path in month_partition_paths(table_output_dir, file_date):
            municipalities = pd.read_csv(
                path, usecols=["id_municipio"], dtype=str
            )
            rows += len(municipalities)
            unknown += count_unknown_municipalities(
                municipalities["id_municipio"], directory_codes
            )

        log(
            f"{file_date}: {unknown} de {rows} linhas com município fora do "
            "diretório"
        )

        if rows and unknown / rows > max_share:
            months_list.append(f"Mês: {file_date}, com {unknown} de {rows}")
        elif unknown:
            log(
                f"{file_date}: municípios fora do diretório abaixo do limite "
                f"de {max_share:.1%}",
                "warning",
            )

    if months_list:
        raise ValueError(
            f"Municípios fora do diretório acima de {max_share:.1%} das "
            f"linhas nos seguintes meses: \n{months_list}"
        )
