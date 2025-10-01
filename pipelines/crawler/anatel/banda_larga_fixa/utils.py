import gc  # noqa: I001
import os
from zipfile import ZipFile
import numpy as np
import pandas as pd
import requests

from pipelines.crawler.anatel.banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.utils.utils import log, to_partitions


def download_zip_file():
    response = requests.get(
        "https://dados.gov.br/api/publico/conjuntos-dados/acessos---banda-larga-fixa",
        cookies=anatel_constants.COOKIES.value,
        headers=anatel_constants.HEADERS.value,
    )

    r = response.json()
    for recurso in r["resources"]:
        if recurso["format"] == "ZIP":
            download_url = recurso["url"]
            log(
                f"Baixando {download_url} em {anatel_constants.INPUT_PATH.value}"
            )

    with open(
        os.path.join(
            anatel_constants.INPUT_PATH.value, "acessos_banda_larga_fixa.zip"
        ),
        "wb",
    ) as file:
        response = requests.get(
            download_url,
            cookies=anatel_constants.COOKIES.value,
            headers=anatel_constants.HEADERS.value,
        )
        file.write(response.content)

    log("Download concluído com sucesso!")


def unzip_file():
    os.makedirs(anatel_constants.INPUT_PATH.value, exist_ok=True)
    download_zip_file()
    zip_file_path = os.path.join(
        anatel_constants.INPUT_PATH.value, "acessos_banda_larga_fixa.zip"
    )
    try:
        with ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(anatel_constants.INPUT_PATH.value)
    except Exception as e:
        print(f"Erro ao baixar ou extrair o arquivo ZIP: {e!s}")

    os.remove(zip_file_path)
    gc.collect()


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    # ! Verifique se existe uma coluna em um Pandas DataFrame. Caso contrário, crie uma nova coluna com o nome fornecido
    # ! e preenchê-lo com valores NaN. Se existir, não faça nada.

    # * Parâmetros:
    # ! df (Pandas DataFrame): O DataFrame a ser verificado.
    # ! col_name (str): O nome da coluna a ser verificada ou criada.

    # * Retorna:
    # ! Pandas DataFrame: O DataFrame modificado.
    """

    if col_name not in df.columns:
        df[col_name] = ""
    return df


def treatment(table_id: str, ano: int):
    log("Iniciando o tratamento do arquivo microdados da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Acessos_Banda_Larga_Fixa_{ano}.csv",
        sep=";",
        encoding="utf-8",
    )
    df = check_and_create_column(df, "Tipo de Produto")
    df = df.rename(columns=anatel_constants.RENAME_MICRODADOS.value)
    df = df.drop(anatel_constants.DROP_COLUMNS_MICRODADOS.value, axis=1)
    df = df[anatel_constants.ORDER_COLUMNS_MICRODADOS.value]
    df = df.sort_values(
        anatel_constants.SORT_VALUES_MICRODADOS.value,
    )
    df = df.replace(np.nan, "")
    df["transmissao"] = df["transmissao"].apply(
        lambda x: x.replace("Cabo Metálico", "Cabo Metalico")
        .replace("Satélite", "Satelite")
        .replace("Híbrido", "Hibrido")
        .replace("Fibra Óptica", "Fibra Optica")
        .replace("Rádio", "Radio")
    )
    df["acessos"] = df["acessos"].apply(lambda x: str(x).replace(".0", ""))
    df["produto"] = df["produto"].apply(
        lambda x: x.replace("LINHA_DEDICADA", "linha dedicada").lower()
    )
    log("Salvando o arquivo microdados da Anatel")
    to_partitions(
        df,
        partition_columns=["ano", "mes", "sigla_uf"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


def treatment_br(table_id: str):
    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(
        anatel_constants.DROP_COLUMNS_BRASIL.value, axis=1
    )
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_brasil = df_brasil.rename(
        columns=anatel_constants.RENAME_COLUMNS_BRASIL.value,
    )
    log("Salvando o arquivo densidade brasil da Anatel")
    df_brasil.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_brasil.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_uf(table_id: str):
    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_uf = df[df["Geografia"] == "UF"]
    df_uf = df_uf.drop(anatel_constants.DROP_COLUMNS_UF.value, axis=1)
    df_uf["Densidade"] = df_uf["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_uf = df_uf.rename(
        columns=anatel_constants.RENAME_COLUMNS_UF.value,
    )
    log("Iniciando o particionado do arquivo densidade uf da Anatel")
    df_uf.to_csv(
        f"{anatel_constants.TABLES_OUTPUT_PATH.value[table_id]}densidade_uf.csv",
        index=False,
        sep=",",
        encoding="utf-8",
        na_rep="",
    )


def treatment_municipio(table_id: str):
    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        f"{anatel_constants.INPUT_PATH.value}Densidade_Banda_Larga_Fixa.csv",
        sep=";",
        encoding="utf-8",
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio = df_municipio.drop(["Município", "Geografia"], axis=1)
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        lambda x: float(x.replace(",", "."))
    )
    df_municipio = df_municipio.rename(
        columns=anatel_constants.RENAME_COLUMNS_MUNICIPIO.value,
    )
    log("Salvando o arquivo densidade municipio da Anatel")
    to_partitions(
        df_municipio,
        partition_columns=["ano"],
        savepath=anatel_constants.TABLES_OUTPUT_PATH.value[table_id],
    )


def get_year():
    lista = []
    for x in os.listdir(anatel_constants.INPUT_PATH.value):
        parts = x.split("_")
        if len(parts) > 4:
            x = parts[4]
            if len(x) == 4:
                lista.append(x)

    max_year = max(lista)
    log(f"Ano máximo: {max_year}")
    return max_year
