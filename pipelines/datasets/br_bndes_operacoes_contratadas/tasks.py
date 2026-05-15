import datetime
from pathlib import Path

import basedosdados as bd
import pandas as pd
from prefect import task

from pipelines.datasets.br_bndes_operacoes_contratadas.constants import (
    constants,
)
from pipelines.datasets.br_bndes_operacoes_contratadas.utils import (
    download_xlsx,
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
    table_id: str = "operacoes_contratadas_forma_direta_e_indireta_nao_automatica",
):
    """
    Processes the data (operacoes_contratadas_forma_direta_e_indireta_nao_automatica) for the given table.

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
    )

    # Cnaes
    df_operacoes = df_operacoes.rename(
        columns=constants.DATA_RENAMMING_MAPPING.value
    )
    df_operacoes["secao_cnae"] = df_operacoes["cnae_2"].str.extract(
        r"(^[A-Z]{1})"
    )
    df_operacoes["subclasse_cnae"] = df_operacoes["cnae_2"].str.extract(
        r"(\d+$)"
    )
    df_operacoes = df_operacoes.drop(columns=["cnae_2"])

    # String columns with null values
    df_types = df_operacoes.dtypes
    string_columns = (
        df_types[df_types == pd.StringDtype].index.to_numpy().tolist()
    )

    for col in string_columns:
        df_operacoes[col] = (
            df_operacoes[col].str.strip().replace("----------", "None")
        )
        df_operacoes[col] = df_operacoes[col].str.strip().replace("-", "None")
        df_operacoes.loc[df_operacoes[col] == "None", col] = None

    # Null values for id_municipio
    df_operacoes.loc[
        (df_operacoes["id_municipio"] == 9999999)
        | (df_operacoes["id_municipio"] == 0),
        "id_municipio",
    ] = None
    df_operacoes["id_municipio"] = (
        df_operacoes["id_municipio"].astype("str").str.replace(".0", "")
    )

    # indicador_inovacao mapping to 1, 0
    df_operacoes["indicador_inovacao"] = df_operacoes[
        "indicador_inovacao"
    ].map({"SIM": 1, "NÃO": 0, "Sim": 1, "Não": 0})

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
    df_operacoes.to_csv(output_folder / "data.csv", index=False)
    return output_folder / "data.csv"


@task
def process_de_para_cnae(
    input_folder: str | Path,
    output_folder: str | Path,
    table_id: str = "cnaes_agrupados_bndes",
):
    """
    Processes the de_para_cnae (metadata) data for the given table.
    Args:
        input_folder (str|Path): The folder where the input files are stored.
        output_folder (str|Path): The folder where the output files will be stored.
        table_id (str): The ID of the table.
    Returns:
        Path: The path to the processed CSV file.
    """

    df_cnae = pd.read_excel(
        input_folder / f"{table_id}.xlsx",
        sheet_name=constants.DE_PARA_CNAE_NAME.value,
        skiprows=constants.DE_PARA_CNAE_SKIPROWS.value,
        usecols=constants.DE_PARA_CNAE_USECOLS.value,
    )
    df_cnae = df_cnae.rename(
        columns=constants.DE_PARA_CNAE_RENAME_MAPPING.value
    )

    df_diretorios = bd.read_sql(
        """
        SELECT DISTINCT secao,divisao FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
        """,
        from_file=True,
    )
    df_diretorios = df_diretorios.astype({"secao": "str", "divisao": "int64"})

    # Cnaes by limits and by lists
    df_cnae[["limite_inferior", "limite_superior"]] = (
        df_cnae["lista_cnaes_2"]
        .str.split(r"\(restante\)| a ")
        .apply(pd.Series)
    )
    df_cnae_limites = df_cnae.loc[df_cnae["limite_superior"].notna()].copy()
    df_cnae_listas = df_cnae.loc[df_cnae["limite_superior"].isna()].copy()

    # CNAEs by lists transformation:
    # split cnaes list into rows, extract section and convert to int
    df_cnae_listas[["limite_inferior", "limite_superior"]] = None
    df_cnae_listas["lista_cnaes_2"] = df_cnae_listas[
        "lista_cnaes_2"
    ].str.split(r"[\n\s]*e{1}[\n\s]*|[\n\s]*,{1}[\n\s]*")
    df_cnae_listas = df_cnae_listas.explode("lista_cnaes_2")
    df_cnae_listas["secao_cnae"] = df_cnae_listas["lista_cnaes_2"].str.extract(
        r"(^[A-Z]{1})"
    )
    df_cnae_listas["lista_cnaes_2"] = (
        df_cnae_listas["lista_cnaes_2"]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("Int64")
    )

    # CNAEs by limits transformation:
    # create cnaes list from limits, extract section and convert to int
    df_cnae_limites["lista_cnaes_2"] = None
    # Eztract section
    df_cnae_limites["secao_cnae"] = df_cnae_limites[
        "limite_inferior"
    ].str.extract(r"(^[A-Z]{1})")
    # Extract limits numbers
    df_cnae_limites["limite_inferior"] = (
        df_cnae_limites["limite_inferior"]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("int")
    )
    df_cnae_limites["limite_superior"] = (
        df_cnae_limites["limite_superior"]
        .str.extract(r"(\d+)")
        .fillna("-1")
        .astype("int")
    )
    df_cnae_limites.loc[
        df_cnae_limites["limite_superior"] == -1, "limite_superior"
    ] = None
    df_cnae_limites.loc[
        df_cnae_limites["limite_inferior"] == -1, "limite_inferior"
    ] = None
    df_cnae_limites["limite_inferior"] = df_cnae_limites[
        "limite_inferior"
    ].astype("Int64")
    df_cnae_limites["limite_superior"] = df_cnae_limites[
        "limite_superior"
    ].astype("Int64")

    # For rows with null upper limit, fill the upper limit with the max division of the section
    df_cnae_limites.loc[
        df_cnae_limites["limite_superior"].isna(), "limite_superior"
    ] = df_cnae_limites.loc[
        df_cnae_limites["limite_superior"].isna(), "secao_cnae"
    ].apply(
        lambda x: df_diretorios.loc[
            df_diretorios["secao"] == x, "divisao"
        ].max()
    )
    for i, row in df_cnae_limites.iterrows():
        df_cnae_limites.loc[i, "lista_cnaes_2"] = str(
            list(range(row["limite_inferior"], row["limite_superior"] + 1))
        )
    # Explode cnaes list into rows and convert to string with leading zeros
    df_cnae_limites["lista_cnaes_2"] = df_cnae_limites["lista_cnaes_2"].apply(
        lambda x: eval(x)
    )
    df_cnae_limites = df_cnae_limites.explode("lista_cnaes_2")
    df_cnae_limites["lista_cnaes_2"] = df_cnae_limites["lista_cnaes_2"].apply(
        lambda x: str(x).zfill(2)
    )

    # Concatenate cnaes by limits and by lists, extract section, division, class and subclass from cnae code and drop unnecessary columns
    df_cnaes_expandidos = pd.concat(
        [df_cnae_limites, df_cnae_listas], ignore_index=True
    )
    df_cnaes_expandidos["divisao_cnae"] = df_cnaes_expandidos[
        "lista_cnaes_2"
    ].str.extract(r"(^\d{2})")
    df_cnaes_expandidos["grupo_cnae"] = df_cnaes_expandidos[
        "lista_cnaes_2"
    ].str.extract(r"(^\d{3})")
    df_cnaes_expandidos["classe_cnae"] = df_cnaes_expandidos[
        "lista_cnaes_2"
    ].str.extract(r"(^\d{5})")
    df_cnaes_expandidos["subclasse_cnae"] = df_cnaes_expandidos[
        "lista_cnaes_2"
    ].str.extract(r"(^\d{7})")
    df_cnaes_expandidos = df_cnaes_expandidos.drop(
        columns=["limite_inferior", "limite_superior", "lista_cnaes_2"]
    )

    df_cnaes_expandidos.to_csv(output_folder / "data.csv", index=False)
    return output_folder / "data.csv"


@task
def true_task():
    return True


@task
def switch_check_for_updates(
    check_for_updates: bool, value_if_true: bool, value_if_false: bool
):
    if check_for_updates:
        log("Considerando data update dos dados.")
        return value_if_true
    else:
        log("Desconsiderando data update dos dados.")
        return value_if_false
