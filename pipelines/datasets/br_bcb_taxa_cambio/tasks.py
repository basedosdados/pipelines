"""
Tasks for br_bcb_taxa_cambio.
"""

from datetime import date

import pandas as pd
from prefect import task

from pipelines.datasets.br_bcb_taxa_cambio.constants import (
    COVERAGE,
    TAXA_CAMBIO_TABLE_ID,
)
from pipelines.datasets.br_bcb_taxa_cambio.utils import (
    available_currencies,
    get_currency_data,
    read_input_csv,
    save_input,
    treat_currency_df,
)
from pipelines.datasets.br_bcb_taxa_cambio.utils import (
    get_source_max_date as _get_source_max_date,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection
from pipelines.utils.utils import log, to_partitions


@task
def get_source_max_date() -> str:
    """Devolve a última data de cotação publicada pelo PTAX, em %Y-%m-%d.

    Returns:
        str: Data da cotação mais recente, no formato %Y-%m-%d.
    """
    return _get_source_max_date()


@task
def get_data_taxa_cambio(table_id: str, ano: int | None = None) -> str:
    """
    Retrieves data from an API for multiple currencies, concatenates the resulting dataframes,
    saves the final dataframe to a file, and returns the full file path.

    Args:
        table_id (str): The identifier for the table.
        ano (int | None): Year to download. None means the current year.

    Returns:
        str: The full file path where the data is saved.
    """

    # List to store individual currency dataframes
    list_dfs = []

    # Iterate over available currencies
    for currency in available_currencies():
        log(f"downloading data for {currency['simbolo']}")
        # Retrieve data for each currency
        df = get_currency_data(currency, ano=ano)
        log("download task successfully !")

        # Append the dataframe to the list
        list_dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    df_final = pd.concat(list_dfs)
    log("all dfs concated")
    # Save the final dataframe to a file and obtain the full file path
    full_filepath = save_input(df_final, table_id)
    log(f"input saved in {full_filepath}")

    # Return the full file path
    return full_filepath


@task
def treat_data_taxa_cambio(table_id: str) -> dict:
    """
    Reads input data from a CSV file, performs data treatment on the dataframe,
    and saves the treated dataframe partitioned by `ano`.

    Args:
        table_id (str): The identifier for the table.

    Returns:
        dict: `save_output_path` (directory the partitioned data was written
            to) and `max_date` (most recent `data_cotacao`, as `%Y-%m-%d`).
    """

    # Read input data from a CSV file
    df = read_input_csv(table_id)

    # Perform data treatment on the dataframe
    df = treat_currency_df(df, table_id)
    max_date = df["data_cotacao"].max()
    log(max_date)

    save_output_path = f"tmp/{table_id}/output/"

    to_partitions(
        data=df, partition_columns=["ano"], savepath=save_output_path
    )

    file_info = {
        "save_output_path": save_output_path,
        "max_date": max_date.strftime("%Y-%m-%d"),
    }

    return file_info


# ──────────────────────────────────────────────────────────────────────────────
# taxa_cambio — ver constants.py
#
# O check consulta só o dólar numa janela de quinze dias (`get_source_max_date`)
# — as dez moedas saem no mesmo boletim do PTAX, então a data do dólar vale
# pra tabela inteira. Leve: não baixa o ano todo, só os últimos boletins.
#
# `extract_load_data` baixa, pra cada moeda, o ano corrente inteiro (mesmo
# comportamento do flow monolítico antigo) — sem download incremental. Aceita
# opcionalmente `anos` (lista de anos) em `download_params`, pra recarregar
# anos específicos do passado num disparo manual direto do deployment de
# extract_and_load (corrige partição incompleta/duplicada) — nesse modo
# `reference_date` é ignorada; esse caminho nunca é usado pelo check_update,
# só por quem dispara a etapa manualmente.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    reference_date = date.fromisoformat(get_source_max_date())
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    anos = download_params.get("anos")
    anos_alvo: list[int | None] = list(anos) if anos else [None]

    output_paths = []
    for ano in anos_alvo:
        get_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID, ano=ano)
        file_info = treat_data_taxa_cambio(table_id=TAXA_CAMBIO_TABLE_ID)
        # pyrefly: ignore [bad-index]
        output_paths.append(file_info["save_output_path"])

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=output_paths[-1],
    )
