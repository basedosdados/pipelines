"""Constants for the Censo 2022 public-microdata onboarding."""

import os
from pathlib import Path

GCP_PROJECT = "basedosdados-dev"
DATASET_ID = "br_ibge_censo_demografico"
YEAR = 2022

# Staging lands in the dev lake. The prod tables are materialised by the
# table-approve action on merge, never uploaded from here.
STAGING_BUCKET = "basedosdados-dev"

# Auxiliary-file bundles go to the prod bucket, one per table, under
# auxiliary_files/<gcp_dataset_id>/<table_slug>/auxiliary_files.zip.
AUX_BUCKET = "basedosdados"
AUX_PREFIX = f"auxiliary_files/{DATASET_ID}"

# Scratch data stays out of the repo tree. Override with CENSO_DATA_ROOT.
DATA_ROOT = Path(
    os.environ.get(
        "CENSO_DATA_ROOT",
        Path.home() / "Downloads" / f"{DATASET_ID}_data" / "data",
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"
DOCS_DIR = DATA_ROOT / "docs"

FTP_CSV = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Microdados_e_Areas_de_Ponderacao/Microdados_de_acesso_Publico/csv"
)
FTP_DOCS = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Microdados_e_Areas_de_Ponderacao/Documentacao/Layout%20e%20dicion%C3%A1rio"
)
LAYOUT_XLSX_NAME = "layout_acesso_publico.xlsx"

# Documents bundled with every table: bundle name → (local name, source URL).
# Renamed to something self-describing; the README records where each came from.
DOC_FILES = {
    "layout_microdados_acesso_publico.xlsx": (
        LAYOUT_XLSX_NAME,
        f"{FTP_DOCS}/Layout%20Microdados%20CD2022%20-%20acesso%20P%c3%bablico.xlsx",
    ),
    "dicionario_de_variaveis.pdf": (
        "dicionario_variaveis.pdf",
        f"{FTP_DOCS}/Dicion%c3%a1rio%20de%20Vari%c3%a1veis%20-%20Microdados%20CD2022.pdf",
    ),
}

# Long-form documents left at the publisher: title → URL. Listed in the bundle
# README instead of rehosted, per the auxiliary-files convention.
DOC_LINKS = {
    "Censo Demográfico 2022 — Microdados da amostra (página do IBGE)": (
        "https://www.ibge.gov.br/estatisticas/sociais/populacao/"
        "22827-censo-demografico-2022.html"
    ),
    "Diretório do FTP com a documentação completa": FTP_DOCS,
}

CITATION = (
    "IBGE — Instituto Brasileiro de Geografia e Estatística. "
    "Censo Demográfico 2022: microdados da amostra, arquivo de acesso público. "
    "Rio de Janeiro: IBGE, 2024."
)


def auxiliary_files_url(table_slug: str) -> str:
    """Public URL of a table's bundle, for the backend's auxiliaryFilesUrl.

    Args:
        table_slug: The table's slug, e.g. ``microdados_pessoa_2022``.

    Returns:
        The https URL of that table's ``auxiliary_files.zip``.
    """
    return (
        f"https://storage.googleapis.com/{AUX_BUCKET}/"
        f"{AUX_PREFIX}/{table_slug}/auxiliary_files.zip"
    )


# IBGE UF code (D0020 / P0020 / …) → sigla.
UF_CODE_TO_SIGLA = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}

# Zip name on the FTP: {code}_{sigla}.zip
UF_ZIPS = [
    (code, sigla, f"{code}_{sigla}.zip")
    for code, sigla in UF_CODE_TO_SIGLA.items()
]

# sheet → table slug and CSV prefix inside each UF zip.
TABLES = {
    "DOMI": {
        "slug": "microdados_domicilio_2022",
        "csv_prefix": "Domicilios",
        "description": (
            "Microdados da amostra do Censo Demográfico 2022 (acesso público): "
            "um registro por domicílio. Geografia máxima é a UF. "
            "Contém apenas registros com risco de revelação abaixo de 20%; "
            "idade em grupos quinquenais; variáveis quase-identificadoras "
            "omitidas; subamostra de 50% dos domicílios com fração amostral "
            "de 100%."
        ),
    },
    "PESS": {
        "slug": "microdados_pessoa_2022",
        "csv_prefix": "Pessoas",
        "description": (
            "Microdados da amostra do Censo Demográfico 2022 (acesso público): "
            "um registro por pessoa. Geografia máxima é a UF. "
            "Contém apenas registros com risco de revelação abaixo de 20%; "
            "idade em grupos quinquenais; variáveis quase-identificadoras "
            "omitidas; subamostra de 50% dos domicílios com fração amostral "
            "de 100%."
        ),
    },
    "FAMI": {
        "slug": "microdados_familia_2022",
        "csv_prefix": "Familia",
        "description": (
            "Microdados da amostra do Censo Demográfico 2022 (acesso público): "
            "um registro por família. Geografia máxima é a UF. "
            "Mesmas restrições de confidencialidade do arquivo público."
        ),
    },
    "MORT": {
        "slug": "microdados_mortalidade_2022",
        "csv_prefix": "Mortalidade",
        "description": (
            "Microdados da amostra do Censo Demográfico 2022 (acesso público): "
            "um registro por óbito de morador reportado no domicílio. "
            "Geografia máxima é a UF. Mesmas restrições de confidencialidade "
            "do arquivo público."
        ),
    },
}

# Source VAR → BD column name. Applied on every record type.
STANDARD_RENAME = {
    "0010": "id_regiao",
    "0020": "sigla_uf",
    "0100": "controle",
    "0110": "peso_amostral",  # DOMI/FAMI/MORT peso
    "0120": "situacao_setor",
}

# Pessoa uses P0110 for peso and P0120 for setor; P0101 is ordem.
PESS_RENAME = {
    "P0010": "id_regiao",
    "P0020": "sigla_uf",
    "P0100": "controle",
    "P0101": "numero_ordem",
    "P0110": "peso_amostral",
    "P0120": "situacao_setor",
    "P0140": "situacao_domicilio",
}
DOMI_RENAME = {
    "D0010": "id_regiao",
    "D0020": "sigla_uf",
    "D0100": "controle",
    "D0110": "peso_amostral",
    "D0120": "situacao_setor",
    "D0140": "situacao_domicilio",
}
FAMI_RENAME = {
    "F0010": "id_regiao",
    "F0020": "sigla_uf",
    "F0100": "controle",
    "F0101": "numero_ordem",
    "F0110": "peso_amostral",
    "F0120": "situacao_setor",
    "F0140": "situacao_domicilio",
}
MORT_RENAME = {
    "M0010": "id_regiao",
    "M0020": "sigla_uf",
    "M0100": "controle",
    "M0101": "numero_ordem",
    "M0110": "peso_amostral",
    "M0120": "situacao_setor",
    "M0140": "situacao_domicilio",
}
RENAMES = {
    "DOMI": DOMI_RENAME,
    "PESS": PESS_RENAME,
    "FAMI": FAMI_RENAME,
    "MORT": MORT_RENAME,
}

ARCHITECTURE_DIR = (
    Path("models") / "br_ibge_censo_demografico" / "code" / "architecture"
)
