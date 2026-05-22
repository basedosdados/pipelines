import datetime
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    constants,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.utils import (
    check_duplicates,
    count_nulls,
    download_xlsx,
    extract_cnae_hierarchy,
    get_xlsx_metadata,
)
from pipelines.utils.utils import log


@task(nout=2)
def create_folders(
    table_id: str,
) -> tuple[Path, Path]:

    table_input_dir = constants.INPUT_DIR.value / table_id
    table_input_dir.mkdir(parents=True, exist_ok=True)
    table_output_dir = constants.OUTPUT_DIR.value / table_id
    table_output_dir.mkdir(parents=True, exist_ok=True)

    return table_input_dir, table_output_dir


@task
def get_source_last_date(
    table_id: str,
    input_folder: str | Path,
    input_file: str | Path | None = None,
):
    """
    Gets the last update date of the source metadata.

    Args:
        table_id (str): The ID of the table and also the filename.
        input_folder (str|Path): The folder where the input files are stored.
        input_file (str|Path|None): The path to the input file. If None, it will be downloaded.

    Returns:
        datetime.date: The last update date of the source data.
    """
    if input_file is None or not Path(input_file).exists():
        input_file = download_xlsx(filename=table_id, input_path=input_folder)

    source_coverage_dates, last_source_update = get_xlsx_metadata(input_file)
    log(
        f"Cobertura temporal: {source_coverage_dates[0].strftime('%Y-%m-%d')} a {source_coverage_dates[1].strftime('%Y-%m-%d')}\nÚltima atualização da fonte: {last_source_update.strftime('%Y-%m-%d')}"
    )
    return last_source_update.date()


@task
def process_data(
    input_folder: str | Path,
    output_folder: str | Path,
    data_apuracao: str | datetime.datetime,
    table_id: str = "operacoes_nao_automaticas",
):
    """
    Processes the data (operacoes_nao_automaticas) for the given table.

    Args:
        input_folder (str|Path): The folder where the input files are stored.
        output_folder (str|Path): The folder where the output files will be stored.
        data_apuracao (str|datetime.datetime): The date of data apuracao.
        table_id (str): The ID of the table.

    Returns:
        Path: The path to the processed CSV file.
    """
    df_operacoes = pd.read_excel(
        input_folder / f"{table_id}.xlsx",
        sheet_name=constants.DATA_SHEET_NAME.value,
        skiprows=constants.DATA_SKIPROWS.value,
        usecols=constants.DATA_USECOLS.value,
        dtype=constants.DATA_DTYPES_MAPPING.value,
    )

    # Check duplicates
    check_duplicates(df_operacoes, df_operacoes.columns.tolist())

    df_operacoes = df_operacoes.rename(
        columns=constants.DATA_RENAMMING_MAPPING.value
    )

    # CNPJ
    df_operacoes["cnpj_cliente"] = (
        df_operacoes["cnpj_cliente"]
        .str.replace(".0", "", regex=False)
        .str.zfill(14)
    )
    df_operacoes.loc[
        df_operacoes["cnpj_cliente"] == "00000000000000", "cnpj_cliente"
    ] = None

    # Cnaes
    df_operacoes = extract_cnae_hierarchy(df_operacoes, "codigo_cnae_2")

    # Null values for id_municipio
    df_operacoes.loc[
        (df_operacoes["id_municipio"] == "9999999")
        | (df_operacoes["id_municipio"] == "0000000"),
        "id_municipio",
    ] = None
    df_operacoes["id_municipio"] = (
        df_operacoes["id_municipio"]
        .astype("str")
        .str.replace(".0", "", regex=False)
    )

    # String columns with null values
    string_columns = df_operacoes.select_dtypes(
        include=["string", "object"]
    ).columns.tolist()

    null_placeholders = [
        "None",
        "NaN",
        "----------",
        "-",
        "nan",
        "null",
        "none",
    ]
    for col in string_columns:
        df_operacoes[col] = df_operacoes[col].str.strip()
        df_operacoes[col] = df_operacoes[col].replace(
            {placeholder: None for placeholder in null_placeholders}
        )

    # indicador_inovacao mapping to 1, 0
    df_operacoes["indicador_inovacao"] = df_operacoes[
        "indicador_inovacao"
    ].map({"SIM": 1, "NÃO": 0, "Sim": 1, "Não": 0})

    count_nulls(df_operacoes, df_operacoes.columns.tolist())

    # Data apuração
    if isinstance(data_apuracao, datetime.datetime):
        data_apuracao = data_apuracao.date()
    elif isinstance(data_apuracao, str):
        try:
            log("Tentando converter data_apuracao com formato %d/%m/%Y")
            df_operacoes["data_apuracao"] = datetime.datetime.strptime(
                data_apuracao, "%d/%m/%Y"
            )
        except ValueError:
            log(
                "Formato %d/%m/%Y falhou, tentando converter data_apuracao com formato %d/%m/%y"
            )
            df_operacoes["data_apuracao"] = datetime.datetime.strptime(
                data_apuracao, "%d/%m/%y"
            )

    else:
        log(f"data_apuracao está no formato {type(data_apuracao)}")
        df_operacoes["data_apuracao"] = data_apuracao

    # Check null values after transformations
    count_nulls(df_operacoes, df_operacoes.columns.tolist())
    df_operacoes.to_csv(output_folder / "data.csv", index=False)
    return output_folder / "data.csv"


@task
def true_task():
    return True


@task
def false_task():
    return False


@task
def switch_check_for_updates(
    check_for_updates: bool | None,
    value_if_true: bool | None,
    value_if_false: bool | None,
):
    if check_for_updates:
        log("Considerando data update dos dados.")
        return value_if_true
    else:
        log("Desconsiderando data update dos dados.")
        return value_if_false
