"""Constants for the Censo 2022 public-microdata onboarding."""

from pathlib import Path

GCP_PROJECT = "sandbox-507414"
DATASET_ID = "br_ibge_censo_demografico"
YEAR = 2022
# Sandbox-only bucket for cleaned parquet + docs. Never basedosdados-dev.
GCS_BUCKET = "sandbox-507414-br-ibge-censo-demografico"
GCS_PREFIX = "br_ibge_censo_demografico"

DATA_ROOT = Path("tmp") / "br_ibge_censo_demografico_data" / "data"
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
