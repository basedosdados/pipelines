import re
from io import BufferedReader
from zipfile import ZipFile

import numpy as np
import pandas as pd
from httpx import Client

from pipelines.datasets.br_anatel_banda_larga_fixa.constants import (
    constants as anatel_constants,
)
from pipelines.shared.anatel.utils import HttpRangeFile
from pipelines.utils.utils import log, to_partitions


def get_zip_url() -> str:
    """Busca na API do dados.gov.br a URL do ZIP com os dados de acessos.

    Returns:
        A URL do primeiro recurso em formato ZIP do conjunto.

    Raises:
        httpx.HTTPStatusError: Se a API responder com erro.
        ValueError: Se a API não listar nenhum recurso em formato ZIP.
    """

    with Client(
        cookies=anatel_constants.COOKIES.value,
        headers=anatel_constants.HEADERS.value,
        timeout=10,
        follow_redirects=True,
    ) as client:
        response = (
            client.get(
                "https://dados.gov.br/api/publico/conjuntos-dados/acessos---banda-larga-fixa"
            )
            .raise_for_status()
            .json()
        )

        for resource in response["resources"]:
            if resource["format"] == "ZIP":
                return resource["url"]

        raise ValueError("A API não trouxe um ZIP.")


def get_csv_file_from_zip(names: list[str], table_id: str) -> str:
    """Escolhe, entre os arquivos do ZIP, o CSV que a tabela lê.

    As tabelas de densidade leem o `Densidade_Banda_Larga_Fixa.csv`. O
    microdados lê o ano mais recente, no formato
    `Acessos_Banda_Larga_Fixa_2026.csv`. Como o formato do nome é fixo, a
    ordem alfabética coincide com a do tempo.

    Args:
        names: Nomes dos arquivos do ZIP, como devolve `ZipFile.namelist()`.
        table_id: Tabela que vai ler o arquivo.

    Returns:
        O nome do arquivo dentro do ZIP.

    Raises:
        ValueError: Se o ZIP não tiver o arquivo que a tabela lê.
    """

    if table_id != "microdados":
        if "Densidade_Banda_Larga_Fixa.csv" in names:
            return "Densidade_Banda_Larga_Fixa.csv"

        raise ValueError(f"Não há CSVs para a tabela: {table_id}")

    microdados_names = [
        name
        for name in names
        if re.fullmatch(r"Acessos_Banda_Larga_Fixa_\d{4}\.csv", name)
    ]

    if microdados_names == []:
        raise ValueError(f"Não há CSVs para a tabela: {table_id}")

    return max(microdados_names)


def download_csv_from_zip(table_id: str) -> str:
    """Baixa do ZIP da Anatel só o CSV que a tabela lê.

    O ZIP é lido direto no servidor, por faixas de bytes, sem ser baixado
    inteiro. Cada requisição traz até 8 MiB, e o arquivo escolhido é gravado em
    `INPUT_PATH` com o mesmo nome que tem dentro do ZIP.

    Args:
        table_id: Tabela que vai ler o arquivo.

    Returns:
        O caminho do CSV gravado.

    Raises:
        httpx.HTTPStatusError: Se a API ou o servidor da Anatel responderem com
            erro.
        OSError: Se o servidor da Anatel não informar o tamanho do ZIP ou
            ignorar o cabeçalho `Range`.
        ValueError: Se a API não listar o ZIP ou se o ZIP não tiver o arquivo
            que a tabela lê.
    """

    url = get_zip_url()

    with Client(
        cookies=anatel_constants.COOKIES.value,
        headers=anatel_constants.HEADERS.value,
        timeout=10,
        follow_redirects=True,
    ) as client:
        buffer_size = 8 * 2**20

        anatel_file = BufferedReader(HttpRangeFile(client, url), buffer_size)

        with ZipFile(anatel_file) as zip_file:
            csv_file = get_csv_file_from_zip(zip_file.namelist(), table_id)

            log(
                f"Baixando {csv_file} de {url} em {anatel_constants.INPUT_PATH.value}"
            )

            csv_path = zip_file.extract(
                csv_file, path=anatel_constants.INPUT_PATH.value
            )

            return csv_path


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


def treatment(csv_path: str, table_id: str):
    log("Iniciando o tratamento do arquivo microdados da Anatel")
    df = pd.read_csv(
        csv_path,
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
        lambda x: (
            x.replace("Cabo Metálico", "Cabo Metalico")
            .replace("Satélite", "Satelite")
            .replace("Híbrido", "Hibrido")
            .replace("Fibra Óptica", "Fibra Optica")
            .replace("Rádio", "Radio")
        )
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


def _parse_densidade(x: str | float | None) -> float | None:
    """Converte a densidade da fonte, com vírgula decimal, para float.

    Args:
        x: Valor da célula: texto como `"12,34"`, ou NaN/None quando a
            densidade está vazia.

    Returns:
        A densidade como float, ou o próprio valor quando a célula está vazia.
    """

    if isinstance(x, str):
        return float(x.replace(",", "."))

    return x


def treatment_br(csv_path: str, table_id: str):
    log("Iniciando o tratamento do arquivo densidade brasil da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_brasil = df[df["Geografia"] == "Brasil"]
    df_brasil = df_brasil.drop(
        anatel_constants.DROP_COLUMNS_BRASIL.value, axis=1
    )
    df_brasil["Densidade"] = df_brasil["Densidade"].apply(_parse_densidade)
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


def treatment_uf(csv_path: str, table_id: str):
    log("Iniciando o tratamento do arquivo densidade uf da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_uf = df[df["Geografia"] == "UF"]
    df_uf = df_uf.drop(anatel_constants.DROP_COLUMNS_UF.value, axis=1)
    df_uf["Densidade"] = df_uf["Densidade"].apply(_parse_densidade)
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


def treatment_municipio(csv_path: str, table_id: str):
    log("Iniciando o tratamento do arquivo densidade municipio da Anatel")
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        dtype=str,
    )
    df = df.rename(columns={"Nível Geográfico Densidade": "Geografia"})
    df_municipio = df[df["Geografia"] == "Municipio"]
    df_municipio = df_municipio.drop(["Município", "Geografia"], axis=1)
    df_municipio = df_municipio.dropna(subset=["Código IBGE"])
    df_municipio["Densidade"] = df_municipio["Densidade"].apply(
        _parse_densidade
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
