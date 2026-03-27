import io
import os
from io import StringIO

import numpy as np
import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from unidecode import unidecode


def download_file(real_file_id: str, sheet_name: str) -> pd.DataFrame:
    """Downloads a file
    Args:
        real_file_id: ID of the file to download
    Returns : IO object with location.

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    filename = "/home/tricktx/.service-account/service-account-sou-da-paz.json"  # ! Path para o Json da service account
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        filename=filename, scopes=scopes
    )

    try:
        # create drive api client
        service = build("drive", "v3", credentials=creds)

        file_id = real_file_id

        # pylint: disable=maybe-no-member
        request = service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}.")

            df = pd.read_excel(file, sheet_name, dtype=str)
            df.columns = df.columns.str.strip()

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return df


def change_columns_name(url_architecture: str) -> dict[str, str]:
    """Essa função recebe como input uma string com link para uma tabela de arquitetura
    e retorna um dicionário com os nomes das colunas originais e os nomes das colunas
    padronizados

    Returns:
        dict: com chaves sendo os nomes originais e valores sendo os nomes padronizados
    """

    rename_columns = []

    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    df_architecture.columns = df_architecture.columns.str.strip()

    column_name_dict = dict(
        zip(
            df_architecture["original_name"],
            df_architecture["name"],
            strict=False,
        )
    )

    for x in column_name_dict.items():
        if x[1] != "(deletado)":
            rename_columns.append(x)

    rename_columns = dict(rename_columns)

    orderning_columns = list(rename_columns.values())

    return rename_columns, orderning_columns


def capitalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize and capitalize text values in DataFrame columns related to weapons and ammunition categories.

    This function applies several transformations to the input DataFrame:
    1. Converts specified columns to lowercase and strips whitespace
    2. Replaces predefined text mappings to standardize category names
    3. Capitalizes the first letter of each word in relevant columns
    4. Cleans military region identifiers by removing "ª RM" and "RM" suffixes

    Args:
      df (pd.DataFrame): Input DataFrame containing columns with text values to be standardized.
        Expected columns include: 'unidade', 'categoria_principal', 'categoria_informada',
        'macrocategoria', 'microcategoria_1', 'macrocategoria_1', 'microcategoria_2',
        and optionally 'id_regiao_militar'.

    Returns:
      pd.DataFrame: DataFrame with standardized and capitalized text values in the processed columns.

    Notes:
      - Only processes columns that exist in the input DataFrame
      - The 'categoria_informada' column is not transformed to lowercase before mapping
      - Military region IDs are converted to string type and whitespace is stripped
      - NaN values in 'id_regiao_militar' are preserved as np.nan
    """

    mapping_columns = {
        "cacs, clubes e federações": "Cacs, clubes e federações",
        "Integ": "Integrantes da",
        "uso institucional": "Uso institucional",
        "Indústria": "Indústria",
        "industria oem": "Indústria oem",
        "integrantes órgãos públicos": "Integrantes Órgãos públicos",
        "integrantes de orgãos públicos": "Integrantes Órgãos públicos",
        "segurança privada": "Segurança privada",
        "varejo": "Varejo",
        "varejo/comércio": "Varejo",
        "pessoa física": "Pessoa física",
        "abin": "abin",
        "agente abin": "abin",
        "agente gsi": "gsi",
        "gsi": "gsi",
        "aeronautica": "aeronaútica",
        "aeronaútica": "aeronaútica",
        "bm": "bombeiro militar",
        "bombeiros militar": "bombeiro militar",
        "bombeiros militares": "bombeiro militar",
        "exercito": "exército brasileiro",
        "eb": "exército brasileiro",
        "exército": "exército brasileiro",
        "pm": "policial militar",
        "policiais militares": "policial militar",
        "Policial militar": "policial militar",
        "pertimido": "permitido",
        "munição marcada com lote": "munições marcadas com lote",
        "integ gcm": "integrante gcm",
        "instrut tiro pf-cbc": "instrutor de tiro pf-cbc",
        "integr policiais e rm": "integrante policial e rm",
        "uso institucional polícias": "uso institucional policial",
        "usos institucionais policias": "uso institucional policial",
        "uso institucional policias": "uso institucional policial",
        "orgão publico": "órgão público",
        "Atiradores ( cac)": "Atiradores (cac)",
    }

    colunas = [
        "unidade",
        "categoria_principal",
        "categoria_informada",
        "macrocategoria",
        "microcategoria_1",
        "macrocategoria_1",
        "microcategoria_2",
    ]

    cols_existentes = [c for c in colunas if c in df.columns]
    cols_sem_informada = [
        c for c in cols_existentes if c != "categoria_informada"
    ]

    df[cols_sem_informada] = (
        df[cols_sem_informada]
        .apply(lambda s: s.str.lower().str.strip())
        .replace(mapping_columns)
    )

    df[cols_existentes] = df[cols_existentes].apply(
        lambda s: s.astype("string").str.capitalize()
    )

    if "id_regiao_militar" in df.columns:
        df["id_regiao_militar"] = (
            df["id_regiao_militar"]
            .astype(str)
            .str.replace("ª RM", "", regex=False)
            .str.replace("RM", "", regex=False)
            .replace("nan", np.nan, regex=False)
            .str.strip()
        )

    return df


def consolidado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert and normalize the 'consolidado' column in the DataFrame.

    Transforms the 'consolidado' column by:
    1. Converting all values to strings
    2. Removing diacritical marks (accents) using unidecode
    3. Mapping string values to boolean: 'nao' -> False, 'sim' -> True
    4. Preserving NaN values as NaN

    Args:
      df (pd.DataFrame): Input DataFrame containing a 'consolidado' column.

    Returns:
      pd.DataFrame: DataFrame with the normalized 'consolidado' column.
    """

    df["consolidado"] = (
        df["consolidado"].apply(lambda x: str(x)).apply(unidecode)
    )
    df["consolidado"] = (
        df["consolidado"]
        .replace("nao", False)
        .replace("sim", True)
        .replace("nan", np.nan)
    )

    return df


def column_br(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the 'sigla_uf' column by cleaning and normalizing state abbreviations.

    This function processes the 'sigla_uf' column if it exists in the DataFrame by:
    - Stripping whitespace from all values
    - Removing internal spaces
    - Converting 'Z-BR' entries to 'BR'
    - Converting 'nan' string literals to actual NaN values

    Args:
      df (pd.DataFrame): Input DataFrame containing a 'sigla_uf' column.

    Returns:
      pd.DataFrame: DataFrame with standardized 'sigla_uf' values.
    """

    if "sigla_uf" in df.columns:  # noqa: SIM102
        if "Z - BR" in df["sigla_uf"].unique():
            df["sigla_uf"] = df["sigla_uf"].str.strip()

            df["sigla_uf"] = (
                df["sigla_uf"]
                .astype(str)
                .str.replace(" ", "", regex=False)
                .replace("Z-BR", "BR")
                .replace("nan", np.nan)
            )

    return df


def create_output(
    output: str = "models/br_sou_da_paz_armas_municoes/output/",
) -> None:
    """
    Create an output directory if it does not already exist.

    This function prints the full path of the output directory and creates it
    with all necessary parent directories. If the directory already exists,
    no error is raised.

    Args:
      output (str): The relative path to the output directory.
        Defaults to "models/br_sou_da_paz_armas_municoes/output/".

    Returns:
      None
    """

    print(f"{os.getcwd()}/{output}")
    os.makedirs(f"{os.getcwd()}/{output}", exist_ok=True)

    return None


def fix_quant(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the 'quantidade' column by replacing invalid values with NaN.

    Replaces the string "-" and "0*" with NaN (np.nan) to handle missing or invalid
    quantity values in the DataFrame.

    Args:
      df (pd.DataFrame): A DataFrame containing a 'quantidade' column with potentially
                invalid values like "-" or "0*".

    Returns:
      pd.DataFrame: The input DataFrame with the 'quantidade' column cleaned, where
            "-" and "0*" values have been replaced with NaN.
    """

    df["quantidade"] = (
        df["quantidade"].replace("-", str(np.nan)).replace("0*", (np.nan))
    )

    return df


def where_not_null(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with null values in key columns.

    Filters out rows where 'ano', 'periodo', or 'quantidade' columns
    contain missing (NaN) values.

    Args:
      df (pd.DataFrame): Input DataFrame to be filtered.

    Returns:
      pd.DataFrame: DataFrame with rows containing null values in 'ano',
              'periodo', or 'quantidade' columns removed.
    """

    df = df.dropna(subset=["ano", "periodo", "quantidade"])

    return df
