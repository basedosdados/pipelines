"""Constants for the cl_res_empresas pipeline."""

from enum import Enum
from pathlib import Path

# repo root, from pipelines/datasets/cl_res_empresas/constants.py
REPO_ROOT = Path(__file__).resolve().parents[3]
CODE_DIR = REPO_ROOT / "models" / "cl_res_empresas" / "code"


class constants(Enum):
    """Constants for the cl_res_empresas pipeline."""

    DATASET_ID = "cl_res_empresas"

    # datos.gob.cl CKAN package holding one CSV per year of constituciones
    CKAN_BASE = "https://datos.gob.cl/api/3/action"
    CKAN_PACKAGE_ID = "registro-de-empresas-y-sociedades"

    # The portal rejects requests without a browser-ish User-Agent.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    TABLES = ["sociedad", "dicionario"]

    ARCHITECTURE_DIR = CODE_DIR / "architecture"
    COMUNA_CUT_PATH = CODE_DIR / "comuna_cut.csv"

    # Source ships semicolon-separated, UTF-8 with BOM, CRLF.
    CSV_SEP = ";"
    CSV_ENCODING = "utf-8-sig"
    SOURCE_DATE_FORMAT = "%d-%m-%Y"

    MONTHS = {
        "Enero": 1,
        "Febrero": 2,
        "Marzo": 3,
        "Abril": 4,
        "Mayo": 5,
        "Junio": 6,
        "Julio": 7,
        "Agosto": 8,
        "Septiembre": 9,
        "Octubre": 10,
        "Noviembre": 11,
        "Diciembre": 12,
    }

    RENAMES = {
        "ID": "id_actuacion",
        "RUT": "rut",
        "Razon Social": "razon_social",
        "Fecha de actuacion (1era firma)": "fecha_actuacion",
        "Fecha de registro (ultima firma)": "fecha_registro",
        "Fecha de aprobacion x SII": "fecha_aprobacion_sii",
        "Anio": "ano",
        "Mes": "mes",
        "Comuna Tributaria": "comuna_tributaria",
        "Region Tributaria": "region_tributaria",
        "Codigo de sociedad": "tipo_sociedad",
        "Tipo de actuacion": "tipo_actuacion",
        "Capital": "capital",
        "Comuna Social": "comuna_social",
        "Region Social": "region_social",
    }

    # Source comuna spellings that no amount of accent/punctuation normalisation
    # reconciles with the official name in br_bd_diretorios_cl. Keys are the
    # normalised source value; values are the CUT code.
    COMUNA_OVERRIDES = {
        "EST CENTRAL": "13106",  # Estación Central
        "CON CON": "05103",  # Concón
        "SAN VICENTE T T": "06117",  # San Vicente de Tagua Tagua
        "PUERTO NATALES": "12401",  # Natales
        "LA CALERA": "05502",  # Calera
        "SAN FCO DE MOSTAZAL": "06110",  # Mostazal
        "SAN JOSE MAIPO": "13203",  # San José de Maipo
        "TIL TIL": "13303",  # Tiltil
        "LLAY LLAY": "05703",  # Llaillay
        "QUINTA TILCOCO": "06114",  # Quinta de Tilcoco
        "SAN PEDRO DE MELIPILLA": "13505",  # San Pedro
        "MARCHIGUE": "06204",  # Marchihue
        "ALTO BIO BIO": "08314",  # Alto Biobío
        "OHIGGINS": "11302",  # O'Higgins
        "TORRES DE PAINE": "12402",  # Torres del Paine
        "ANTARTIDA": "12202",  # Antártica
    }

    # Value -> label for the columns flagged covered_by_dictionary in the
    # architecture. Sourced from the Ley 20.659 Régimen Simplificado.
    DICTIONARY = {
        ("sociedad", "tipo_sociedad"): {
            "EIRL": "Empresa Individual de Responsabilidad Limitada",
            "SA": "Sociedad Anónima Cerrada",
            "SAGR": "Sociedad Anónima de Garantía Recíproca",
            "SCA": "Sociedad en Comandita por Acciones",
            "SCC": "Sociedad Colectiva Comercial",
            "SCS": "Sociedad en Comandita Simple",
            "SRL": "Sociedad de Responsabilidad Limitada",
            "SpA": "Sociedad por Acciones",
        }
    }
