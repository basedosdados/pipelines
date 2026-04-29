"""
General purpose functions for the br_inmet_bdmep project
"""

import re
import zipfile
from datetime import datetime, time
from pathlib import Path

import basedosdados as bd
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from shapely import Point
from unidecode import unidecode

from pipelines.datasets.br_inmet_bdmep.constants import (
    constants as constants_microdados,
)
from pipelines.utils.utils import log


def get_latest_dowload_link() -> str:
    try:
        reponse = requests.get(constants_microdados.URL.value)

        soup = BeautifulSoup(reponse.text, "html.parser")
        latest_dowload_link = soup.select("article.post-preview a:last-child")[
            -1
        ].get("href")
        return latest_dowload_link

    except IndexError:
        raise Exception(
            "Nenhum link para download foi encontrado. Verifique o site e o select usado"
        ) from None


def get_date_from_path(path: str) -> datetime:
    try:
        match_data = re.compile(r"(\d{2}-\d{2}-\d{4})\.CSV$").search(path)
        date = datetime.strptime(match_data.group(1), "%d-%m-%Y")
        return date

    except AttributeError:
        raise Exception(
            "O 'path' que foi passado não respeita mais o padrão estabecelido. Data não encontrada!"
        ) from None


def new_names(base: pd.DataFrame, oldname: str, newname: str):
    """
    Esta função renomeia a coluna oldname do DataFrame base para newname.

    Args:

    `base` : DataFrame do Pandas
    O DataFrame no qual a coluna deve ser renomeada.

    `oldname` : string
    O nome atual da coluna a ser renomeada.

    `newname` : string
    O novo nome que será atribuído à coluna.
    Retorno:

    `names(base)` : lista de strings
    Retorna uma lista contendo os nomes das colunas do DataFrame base após a renomeação. Se mais de uma coluna com o nome oldname for encontrada, a função retorna a lista de todos os nomes das colunas em base.

    """
    # x = [i for i, name in enumerate(base.columns) if name == oldname]
    x = re.search(oldname, base.columns)

    if len(x) > 1:
        return base.columns.tolist()

    else:
        base = base.rename(columns={oldname: newname})
        return base.columns.tolist()


def lowercase_columns(df):
    df = df.rename(columns=lambda x: unidecode(x.lower()))
    return df


def change_names(base: pd.DataFrame):
    """
    Altera os nomes das colunas de um DataFrame baseado em um conjunto
    de regras pré-definidas.

    Args:
        base (pandas.DataFrame): DataFrame com as colunas que serão
            renomeadas.

    Returns:
        List[str]: Lista com os novos nomes das colunas.

    Regras:
        - Torna os nomes das colunas em letras minúsculas.
        - Remove caracteres acentuados e substitui por caracteres ASCII
          equivalentes.
        - Renomeia as colunas baseado em padrões específicos
    """
    base = lowercase_columns(base)
    # base.columns = base.columns.map(lambda x: x.translate(x, string.ascii_letters, 'ASCII'))
    base = rename_cols_with_regex(base, "data", "data")
    base = rename_cols_with_regex(base, "^hora", "hora")
    base = rename_cols_with_regex(
        base, "precipitacao.*total", "precipitacao_total"
    )
    base = rename_cols_with_regex(base, "pressao.*nivel", "pressao_atm_hora")
    base = rename_cols_with_regex(base, "pressao.*max", "pressao_atm_max")
    base = rename_cols_with_regex(base, "pressao.*min", "pressao_atm_min")
    base = rename_cols_with_regex(base, "radiacao", "radiacao_global")
    base = rename_cols_with_regex(
        base, "temperatura.*bulbo.*horaria", "temperatura_bulbo_hora"
    )
    base = rename_cols_with_regex(
        base, "temperatura maxima", "temperatura_max"
    )
    base = rename_cols_with_regex(
        base, "temperatura minima", "temperatura_min"
    )
    base = rename_cols_with_regex(
        base, "temperatura do ponto de orvalho", "temperatura_orvalho_hora"
    )
    base = rename_cols_with_regex(
        base, "temperatura orvalho min", "temperatura_orvalho_min"
    )
    base = rename_cols_with_regex(
        base, "temperatura orvalho max", "temperatura_orvalho_max"
    )
    base = rename_cols_with_regex(
        base, "umidade relativa.*horaria", "umidade_rel_hora"
    )
    base = rename_cols_with_regex(base, "umidade rel. max", "umidade_rel_max")
    base = rename_cols_with_regex(base, "umidade rel. min", "umidade_rel_min")
    base = rename_cols_with_regex(base, "vento.*direcao", "vento_direcao")
    base = rename_cols_with_regex(
        base, "vento.*rajada.* maxima", "vento_rajada_max"
    )
    base = rename_cols_with_regex(
        base, "vento.*velocidade", "vento_velocidade"
    )

    return base


def rename_cols_with_regex(df, regex, new_name):
    """
    Renomeia as colunas de um dataframe que correspondem a um regex.

    Parameters:
    df (pandas.DataFrame): O dataframe para renomear as colunas.
    regex (str): O regex para procurar nas colunas do dataframe.
    new_name (str): O novo nome para atribuir às colunas que correspondem ao regex.

    Returns:
    pandas.DataFrame: O dataframe com as colunas renomeadas.
    """
    pattern = re.compile(regex)
    col_names = df.columns.tolist()
    renamed_cols = [
        new_name if pattern.search(col) else col for col in col_names
    ]
    df.columns = renamed_cols
    return df


def convert_to_time(hora: str):
    # hora_str = "0100 UTC"
    hora_parts = hora.split()[0]  # extrai "0100" da string original
    hora_obj = time(
        hour=int(hora_parts[:2]), minute=0, second=0
    )  # cria um objeto time com a hora

    return hora_obj.strftime("%H:%M:%S")


def get_clima_info(file: str) -> pd.DataFrame:
    """
    Extrai informações climáticas de um arquivo em formato .txt e retorna um dataframe com as informações.

    Args:
        file (str): O caminho e nome do arquivo a ser lido.

    Returns:
        pd.DataFrame: Um dataframe com as informações climáticas.
    """

    # lê o arquivo de clima
    clima = pd.read_csv(
        file, sep=";", skiprows=8, decimal=",", encoding="ISO-8859-1"
    )

    # lê as informações de cabeçalho
    caract = pd.read_csv(
        file,
        sep=";",
        nrows=8,
        header=None,
        names=["caract", "value"],
        encoding="ISO-8859-1",
    )

    # remove a coluna V20 do dataframe clima
    clima = clima.drop(columns=["Unnamed: 19"])

    # renomeia as colunas do dataframe clima
    clima = change_names(clima)

    # adiciona as informações da estação no dataframe clima
    clima["id_estacao"] = caract.loc[3, "value"]

    # substitui valores -9999 por NaN
    clima = clima.replace(to_replace=-9999, value=np.nan)

    # converte a coluna data para datetime
    if all(clima["data"].str.contains("/")):
        clima["data"] = clima["data"].apply(
            lambda x: datetime.strptime(str(x), "%Y/%m/%d")
        )
    else:
        clima["data"] = clima["data"].apply(
            lambda x: datetime.strptime(str(x), "%Y-%m-%d")
        )

    # converte as colunas de 3 a 19 para float
    clima.iloc[:, 3:19] = clima.iloc[:, 3:19].astype(float)

    # converte a coluna hora para o formato "HH:00:00"
    clima["hora"] = clima["hora"].apply(lambda x: convert_to_time(x))

    return clima


def get_station_id_municipio(
    sigla_uf: str,
    latitude: float,
    longitude: float,
    data_municipios: pd.DataFrame = None,
) -> int:
    valid_uf_codes = {
        "AC",
        "AL",
        "AP",
        "AM",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MT",
        "MS",
        "MG",
        "PA",
        "PB",
        "PR",
        "PE",
        "PI",
        "RJ",
        "RN",
        "RS",
        "RO",
        "RR",
        "SC",
        "SP",
        "SE",
        "TO",
    }

    sigla_uf = str(sigla_uf).strip().upper()
    if sigla_uf not in valid_uf_codes:
        raise ValueError(
            f"UF inválida: {sigla_uf!r}. Deve ser uma sigla de estado brasileiro de duas letras."
        )

    if data_municipios is None:
        df_municipios = bd.read_sql(
            query="""
            SELECT id_municipio, centroide
            FROM `basedosdados.br_bd_diretorios_brasil.municipio`
            WHERE sigla_uf = '{}'
            """.format(sigla_uf.replace("'", "\\'")),
            from_file=True,
        )
    else:
        df_municipios = data_municipios.loc[
            data_municipios["sigla_uf"] == sigla_uf,
            ["id_municipio", "centroide"],
        ].copy()
    df_municipios = gpd.GeoDataFrame(
        df_municipios,
        geometry=gpd.GeoSeries.from_wkt(df_municipios["centroide"]),
    )

    # Distância entre o centroide do município e a estação meteorológica
    df_municipios["distancia"] = gpd.GeoSeries.from_wkt(
        df_municipios["centroide"]
    ).apply(lambda x: Point(longitude, latitude).distance(x))

    return df_municipios.sort_values("distancia").iloc[0].id_municipio


def get_estacao_info(
    file: str | Path, data_municipios: pd.DataFrame = None
) -> dict:
    """
    Args:
        file (str|Path): O caminho e nome do arquivo a ser lido.

    Returns:
        dict: Um dicionário com dados de todas as estações.
    """

    df_estacao = pd.read_csv(
        file,
        sep=";",
        nrows=8,
        header=None,
        names=["caract", "value"],
        encoding="ISO-8859-1",
    )

    dados_estacao = {
        "id_estacao": str(df_estacao.loc[3, "value"])
        if str(df_estacao.loc[3, "value"]) != ""
        else "",
        "nome_estacao": str(df_estacao.loc[2, "value"])
        if str(df_estacao.loc[2, "value"]) != ""
        else "",
        "sigla_uf": str(df_estacao.loc[1, "value"]).strip()
        if str(df_estacao.loc[1, "value"]) != ""
        else "",
        "latitude": float(str(df_estacao.loc[4, "value"]).replace(",", "."))
        if str(df_estacao.loc[4, "value"]) != ""
        else "",
        "longitude": float(str(df_estacao.loc[5, "value"]).replace(",", "."))
        if str(df_estacao.loc[5, "value"]) != ""
        else "",
        "altitude": float(str(df_estacao.loc[6, "value"]).replace(",", "."))
        if str(df_estacao.loc[6, "value"]) != ""
        else "",
        "data_fundacao": "",
    }

    if "/" in df_estacao.loc[7, "value"]:
        try:
            dados_estacao["data_fundacao"] = datetime.strptime(
                str(df_estacao.loc[7, "value"]), "%Y/%m/%d"
            )
        except Exception:
            dados_estacao["data_fundacao"] = datetime.strptime(
                str(df_estacao.loc[7, "value"]), "%d/%m/%y"
            )
    elif "-" in df_estacao.loc[7, "value"]:
        try:
            dados_estacao["data_fundacao"] = datetime.strptime(
                str(df_estacao.loc[7, "value"]), "%Y-%m-%d"
            )
        except Exception:
            dados_estacao["data_fundacao"] = datetime.strptime(
                str(df_estacao.loc[7, "value"]), "%d-%m-%y"
            )
    else:
        dados_estacao["data_fundacao"] = df_estacao.loc[7, "value"]

    if all(
        [
            dados_estacao["sigla_uf"] != "",
            dados_estacao["latitude"] != "",
            dados_estacao["longitude"] != "",
        ]
    ):
        dados_estacao["id_municipio"] = get_station_id_municipio(
            sigla_uf=dados_estacao["sigla_uf"],
            latitude=dados_estacao["latitude"],
            longitude=dados_estacao["longitude"],
            data_municipios=data_municipios,
        )
    else:
        dados_estacao["id_municipio"] = None
    return dados_estacao


def download_inmet(latest_dowload_link: str) -> None:
    """
    Realiza o download dos dados históricos de uma determinado ano do INMET (Instituto Nacional de Meteorologia)
    e descompacta o arquivo em um diretório local.

    Args:
        latest_dowload_link (str): O link para o qual deseja-se baixar os dados históricos.

    Returns:
        None
    """

    response = requests.get(latest_dowload_link, stream=True)

    response.raise_for_status()

    save_path = (
        constants_microdados.PATH_INPUT.value
        / latest_dowload_link.split("/")[-1]
    )

    constants_microdados.PATH_INPUT.value.mkdir(parents=True, exist_ok=True)

    log("Iniciando Download...")

    with open(save_path, "wb") as fd:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall(constants_microdados.PATH_INPUT.value)

    log("Dados baixados.")


def verify_inmet_duplicates(
    dataframe: pd.DataFrame, subset: list[str] | None = None
):
    """
    Verifica se existem linhas duplicadas em um DataFrame com base em um subconjunto de colunas.
    Args:
        dataframe (pd.DataFrame): O DataFrame a ser verificado.
        subset (list[str], optional): Lista de colunas a serem consideradas para identificar duplicatas.
            Padrão é ["data", "hora", "id_estacao"].
    Returns:
        bool: True se existirem linhas duplicadas, False caso contrário.
    """
    if subset is None:
        subset = ["data", "hora", "id_estacao"]
    return dataframe.duplicated(subset=subset).any()
