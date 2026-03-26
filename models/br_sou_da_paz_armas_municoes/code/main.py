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
    # Converte a URL de edição para um link de exportação em formato csv

    rename_columns = []

    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")
    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    df_architecture.columns = df_architecture.columns.str.strip()

    # Cria um dicionário de nomes de colunas e tipos de dados a partir do dataframe df_architecture
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


# df = change_columns_name(url_architecture='https://docs.google.com/spreadsheets/d/1awbiZiSUeyeG3j0wIuAT0m8iFBJ_YnffAnHLnN5qnmM/edit#gid=0')


def capitalize(df: pd.DataFrame) -> pd.DataFrame:

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
    print(f"{os.getcwd()}/{output}")
    os.makedirs(f"{os.getcwd()}/{output}", exist_ok=True)

    return None


def fix_quant(df: pd.DataFrame) -> pd.DataFrame:
    df["quantidade"] = (
        df["quantidade"].replace("-", str(np.nan)).replace("0*", (np.nan))
    )

    return df


def where_not_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["ano", "periodo", "quantidade"])

    return df
