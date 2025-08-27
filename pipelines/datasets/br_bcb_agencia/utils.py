import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from pipelines.utils.utils import log


# ==== Funções para download de dados ==== #
def fetch_bcb_documents(
    url: str, headers: dict, params: dict
) -> dict[str, Any] | None:
    """
    Consulta documentos na API do Banco Central.

    Args:
        pasta (str): Caminho da pasta na API (ex: "/postos", "/agencias")

    Returns:
        dict | None: Dados retornados pela API ou None em caso de erro.
    """

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log(f"Erro ao consultar API do BCB: {e}")
        return None


def download_file(
    url: str,
    download_dir: Path,
    session: requests.Session = None,
    filename: str = None,
) -> Path | None:
    """
    Downloads a file from the specified URL and saves it to a given directory.

    Args:
        url (str): The URL of the file to download.
        download_dir (Path): Directory where the file should be saved.
        session (requests.Session, optional): Optional requests session for reusing connections.
        filename (str, optional): Custom name to save the file as. Defaults to the name in the URL.

    Returns:
        Path | None: Path to the downloaded file, or None if download failed.
    """
    log(f"Downloading {url}")
    local_path = (
        Path(download_dir) / filename.lower()
        if filename is not None
        else Path(download_dir) / url.split("/")[-1].lower()
    )

    try:
        response = session.get(url) if session else requests.get(url)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(response.content)

        return local_path
    except FileNotFoundError:
        log("Failed to locate directory or create file.")
    except Exception as e:
        log(f"Error downloading file: {e}")
    return None


# ==== Funções para leitura e tratamento inicial dos arquivos ==== #
def find_cnpj_row_number(file_path: str) -> int:
    """
    Localiza a linha em que aparece a primeira ocorrência do valor 'CNPJ'
    (sempre na primeira coluna) e retorna o número da linha.

    Args:
        file_path (str): Caminho do arquivo.

    Returns:
        int: Número da linha correspondente ao cabeçalho.
    """
    df = pd.read_excel(file_path, nrows=20)
    first_col = df.columns[0]
    log(f"Primeira coluna identificada: {first_col}")

    df[first_col] = df[first_col].str.strip()
    match = df[df[first_col] == "CNPJ"]

    if not match.empty:  # noqa: SIM108
        cnpj_row_number = match.index.tolist()[0]
    else:
        cnpj_row_number = 9  # valor padrão quando não encontrado

    return cnpj_row_number


def get_conv_names(file_path: str, skiprows: int) -> dict:
    """
    Obtém nomes de colunas de um arquivo para uso como conversores (string).

    Args:
        file_path (str): Caminho do arquivo.
        skiprows (int): Número de linhas a serem ignoradas.

    Returns:
        dict: Dicionário mapeando nomes de colunas para `str`.
    """
    df = pd.read_excel(file_path, nrows=20, skiprows=skiprows)
    cols = df.columns
    conv = dict(zip(cols, [str] * len(cols), strict=False))
    return conv


def read_file(file_path: str, file_name: str) -> pd.DataFrame:
    """
    Lê um arquivo Excel de agências e retorna um DataFrame tratado.

    Args:
        file_path (str): Caminho do arquivo.
        file_name (str): Nome do arquivo, utilizado para extrair ano e mês.

    Returns:
        pd.DataFrame: DataFrame contendo os dados da agência.
    """
    try:
        skiprows = find_cnpj_row_number(file_path=file_path) + 1
        conv = get_conv_names(file_path=file_path, skiprows=skiprows)
        log(f"Conversores de colunas gerados: {list(conv.keys())}")
        df = pd.read_excel(
            file_path, skiprows=skiprows, converters=conv, skipfooter=2
        )
        df = create_year_month_cols(df=df, file=file_name)

    except Exception as e:
        log(
            f"Erro ao capturar linha de cabeçalho (skiprows): {e}. Usando valor padrão (9)."
        )
        conv = get_conv_names(file_path=file_path, skiprows=9)
        df = pd.read_excel(
            file_path, skiprows=9, converters=conv, skipfooter=2
        )
        df = create_year_month_cols(df=df, file=file_name)

    return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza limpeza padronizada nos nomes das colunas de um DataFrame.

    Operações aplicadas:
        - Remove espaços em branco iniciais e finais.
        - Remove acentuação.
        - Substitui caracteres especiais por "_".
        - Converte para letras minúsculas.

    Args:
        df (pd.DataFrame): DataFrame de entrada.

    Returns:
        pd.DataFrame: DataFrame com nomes de colunas limpos.
    """
    log("Limpando nomes de colunas...")
    df.columns = df.columns.str.strip()
    df.columns = df.columns.map(
        lambda x: unicodedata.normalize("NFKD", str(x))
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    df.columns = df.columns.str.replace(r"[^\w\s]+", "_", regex=True)
    df.columns = df.columns.str.lower()
    return df


# ==== Funções de transformação e padronização ==== #
def create_year_month_cols(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """
    Cria colunas de ano e mês a partir do nome do arquivo.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        file (str): Nome do arquivo (primeiros 6 dígitos são ano e mês).

    Returns:
        pd.DataFrame: DataFrame com colunas 'ano' e 'mes' adicionadas.
    """
    df["ano"] = file[0:4]
    df["mes"] = file[4:6]
    log(f"Colunas 'ano'={file[0:4]} e 'mes'={file[4:6]} criadas.")
    return df


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Verifica se uma coluna existe no DataFrame.
    Caso não exista, cria a coluna preenchida com strings vazias.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        col_name (str): Nome da coluna a verificar/criar.

    Returns:
        pd.DataFrame: DataFrame atualizado.
    """
    if col_name not in df.columns:
        df[col_name] = ""
        log(f"Coluna '{col_name}' criada (não existia no DataFrame).")
    return df


def rename_cols() -> dict:
    """
    Dicionário de padronização de nomes de colunas.

    Returns:
        dict: Dicionário com mapeamento de nomes antigos para novos.
    """
    rename_dict = {
        "cnpj": "cnpj",
        "sequencial do cnpj": "sequencial_cnpj",
        "dv do cnpj": "dv_do_cnpj",
        "nome instituicao": "instituicao",
        "nome da instituicao": "instituicao",
        "segmento": "segmento",
        "segmentos": "segmento",
        "cod compe ag": "id_compe_bcb_agencia",
        "cod compe bco": "id_compe_bcb_instituicao",
        "nome da agencia": "nome_agencia",
        "nome agencia": "nome_agencia",
        "endereco": "endereco",
        "numero": "numero",
        "complemento": "complemento",
        "bairro": "bairro",
        "cep": "cep",
        "municipio": "nome",
        "estado": "sigla_uf",
        "uf": "sigla_uf",
        "data inicio": "data_inicio",
        "ddd": "ddd",
        "fone": "fone",
        "id instalacao": "id_instalacao",
        "municipio ibge": "id_municipio",
    }
    return rename_dict


def order_cols() -> list:
    """
    Ordem padrão das colunas no DataFrame.

    Returns:
        list: Lista ordenada de nomes de colunas.
    """
    return [
        "ano",
        "mes",
        "sigla_uf",
        "nome",
        "id_municipio",
        "data_inicio",
        "cnpj",
        "nome_agencia",
        "instituicao",
        "segmento",
        "id_compe_bcb_agencia",
        "id_compe_bcb_instituicao",
        "cep",
        "endereco",
        "complemento",
        "bairro",
        "ddd",
        "fone",
        "id_instalacao",
    ]


def clean_nome_municipio(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Realiza limpeza da coluna de município.

    Operações aplicadas:
        - Remove acentos.
        - Remove caracteres especiais.
        - Converte para minúsculas.
        - Remove espaços extras.

    Args:
        df (pd.DataFrame): DataFrame de entrada.
        col_name (str): Nome da coluna de município.

    Returns:
        pd.DataFrame: DataFrame atualizado.
    """
    df[col_name] = df[col_name].apply(
        lambda x: unicodedata.normalize("NFKD", str(x))
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    df[col_name] = df[col_name].apply(lambda x: re.sub(r"[^\w\s]", "", x))
    df[col_name] = df[col_name].str.lower().str.strip()
    return df


def remove_latin1_accents_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove acentuação de todos os valores de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame de entrada.

    Returns:
        pd.DataFrame: DataFrame atualizado.
    """
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: "".join(
                c
                for c in unicodedata.normalize("NFD", str(x))
                if unicodedata.category(c) != "Mn"
            )
        )
    return df


def remove_non_numeric_chars(s: str) -> str:
    """
    Remove caracteres não numéricos de uma string.

    Args:
        s (str): String de entrada.

    Returns:
        str: String contendo apenas números.
    """
    return re.sub(r"\D", "", s)


def remove_empty_spaces(s: str) -> str:
    """
    Remove espaços em branco de uma string.

    Args:
        s (str): String de entrada.

    Returns:
        str: String sem espaços.
    """
    return re.sub(r"\s", "", s)


def create_cnpj_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói a coluna `cnpj` concatenando suas partes.

    Args:
        df (pd.DataFrame): DataFrame contendo colunas
            `cnpj`, `sequencial_cnpj` e `dv_do_cnpj`.

    Returns:
        pd.DataFrame: DataFrame com a coluna `cnpj` final.
    """
    df["sequencial_cnpj"] = df["sequencial_cnpj"].astype(str)
    df["dv_do_cnpj"] = df["dv_do_cnpj"].astype(str)
    df["cnpj"] = df["cnpj"].astype(str)

    df["cnpj"] = df["cnpj"] + df["sequencial_cnpj"] + df["dv_do_cnpj"]
    log("Coluna 'cnpj' construída.")
    return df


def str_to_title(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Transforma os valores de uma coluna em formato título (Title Case).
    """
    df[column_name] = df[column_name].str.title()
    return df


def strip_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove espaços extras de todos os valores string de um DataFrame.

    Args:
        df (pd.DataFrame): DataFrame de entrada.

    Returns:
        pd.DataFrame: DataFrame atualizado.
    """
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].str.strip()
    return df


def format_date(date_str: str) -> str:
    """
    Converte uma string de data para o formato padrão YYYY-MM-DD.

    Caso não seja possível converter, retorna string vazia.

    Args:
        date_str (str): Data em formato variável.

    Returns:
        str: Data formatada ou string vazia.
    """
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        return ""
