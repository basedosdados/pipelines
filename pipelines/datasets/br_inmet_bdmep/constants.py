"""
Constant values for br_inmet_bdmep.
"""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import DateFormat, DateOnly, PartBdpro

DATASET_ID = "br_inmet_bdmep"

MICRODADOS_TABLE_ID = "microdados"

COVERAGE = PartBdpro(
    date_column=DateOnly(col="data"),
    date_format=DateFormat.YEAR_MD,
)


class ConstantsMicrodados(Enum):
    """
    Constant values for the br_inmet_bdmep project
    """

    DOWNLOAD_URL = (
        "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    )

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
        )
    }

    ENCODING = "ISO-8859-1"

    BASE_PATH = Path("tmp") / "data" / "microdados"

    PATH_INPUT = BASE_PATH / "input"

    PATH_OUTPUT = BASE_PATH / "output"

    RENAME_PATTERNS = [
        ("data", "data"),
        ("^hora", "hora"),
        ("precipitacao.*total", "precipitacao_total"),
        ("pressao.*nivel", "pressao_atm_hora"),
        ("pressao.*max", "pressao_atm_max"),
        ("pressao.*min", "pressao_atm_min"),
        ("radiacao", "radiacao_global"),
        ("temperatura.*bulbo.*horaria", "temperatura_bulbo_hora"),
        ("temperatura maxima", "temperatura_max"),
        ("temperatura minima", "temperatura_min"),
        ("temperatura do ponto de orvalho", "temperatura_orvalho_hora"),
        ("temperatura orvalho min", "temperatura_orvalho_min"),
        ("temperatura orvalho max", "temperatura_orvalho_max"),
        ("umidade relativa.*horaria", "umidade_rel_hora"),
        ("umidade rel. max", "umidade_rel_max"),
        ("umidade rel. min", "umidade_rel_min"),
        ("vento.*direcao", "vento_direcao"),
        ("vento.*rajada.* maxima", "vento_rajada_max"),
        ("vento.*velocidade", "vento_velocidade"),
    ]

    COLUMNS_ORDER = [
        "data",
        "hora",
        "id_estacao",
        "precipitacao_total",
        "pressao_atm_hora",
        "pressao_atm_max",
        "pressao_atm_min",
        "radiacao_global",
        "temperatura_bulbo_hora",
        "temperatura_orvalho_hora",
        "temperatura_max",
        "temperatura_min",
        "temperatura_orvalho_max",
        "temperatura_orvalho_min",
        "umidade_rel_max",
        "umidade_rel_min",
        "umidade_rel_hora",
        "vento_direcao",
        "vento_rajada_max",
        "vento_velocidade",
    ]
