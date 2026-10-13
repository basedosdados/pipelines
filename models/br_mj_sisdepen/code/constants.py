"""Constants for the br_mj_sisdepen dataset.

The SISDEPEN downloads page publishes one file per semiannual cycle. Cycle 1 is
2016/2 and cycle 19 is 2025/2. See architecture/README.md for the schema
generations and the source quirks these constants encode.
"""

from __future__ import annotations

import os
from pathlib import Path

BASE_URL = "https://www.gov.br/senappen/pt-br/servicos/sisdepen/bases-de-dados"

# cycle -> path fragment on the downloads page
CYCLE_FILES: dict[int, str] = {
    1: "2016/1o-ciclo-base-de-dados-2016-2-semestre.csv",
    2: "2017/2o-ciclo-base-de-dados-2017-1-semestre.csv",
    3: "2017/3o-ciclo-base-de-dados-2017-2-semestre.csv",
    4: "2018/4o-ciclo-base-de-dados-2018-1-semestre.csv",
    5: "2018/5o-ciclo-base-de-dados-2018-2-semestre.csv",
    6: "2019/6o-ciclo-base-de-dados-2019-1-semestre.csv",
    7: "2019/7o-ciclo-base-de-dados-2019-2-semestre.csv",
    8: "2020/8o-ciclo-base-de-dados-2020-1-semestre.csv",
    9: "2020/9o-ciclo-base-de-dados-2020-2-semestre.csv",
    10: "2021/10o-ciclo-base-de-dados-2021-1-semestre.csv",
    11: "2021/11o-ciclo-base-de-dados-2021-2-semestre.csv",
    12: "2022/12o-ciclo-base-de-dados-2022-1-semestre.csv",
    13: "2022/13o-ciclo-base-de-dados-2022-2-semestre.csv",
    14: "2023/14o-ciclo-base-de-dados-2023-1-semestre.csv",
    15: "2023/15o-ciclo-base-de-dados-2023-2-semestre.csv",
    16: "2024/16o-ciclo-base-de-dados-2024-1-semestre-retificado.csv",
    17: "2024/17o-ciclo-base-de-dados-2024-2-semestre.csv",
    18: "2025/18o-ciclo-base-de-dados-2025-1-semestre-retificado.csv",
    19: "2025/19o-ciclo-base-de-dados-2025-2-semestre.csv",
}

# Questionnaire generation. There is NO 2019 break: cycles 2-9 share a
# byte-identical 1,333-column header. The real discontinuity is at cycle 14.
SCHEMA_GENERATION: dict[int, str] = (
    {1: "A"}
    | {c: "B" for c in range(2, 10)}
    | {c: "C" for c in range(10, 14)}
    | {c: "D" for c in range(14, 20)}
)

GENERATION_LABELS = {
    "A": "Ciclo 1 (2016/2), 1.327 colunas",
    "B": "Ciclos 2 a 9 (2017/1 a 2020/2), 1.333 colunas",
    "C": "Ciclos 10 a 13 (2021/1 a 2022/2), 1.334 colunas",
    "D": "Ciclos 14 a 19 (2023/1 a 2025/2), 1.737 colunas",
}

# Cycles 16 and 18 (the "retificado" files) rename this column, same position.
COLUMN_ALIASES: dict[str, list[str]] = {
    "ano": ["Ano"],
    "referencia": ["Referência"],
    "tipo_recolhimento": ["Tipo do Estabelecimento", "Tipo de Recolhimento"],
    "preenchimento": ["Situação de Preenchimento"],
    "nome": ["Nome do Estabelecimento"],
    "situacao_estabelecimento": ["Situação do Estabelecimento"],
    "ambito": ["Âmbito"],
    "uf": ["UF"],
    "municipio": ["Município"],
    "id_municipio": ["Código IBGE"],
    "sexo_destinacao_original": [
        "1.1 Estabelecimento originalmente destinado a pessoa privadas de liberdade do sexo"
    ],
    "tipo_estabelecimento_original": [
        "1.2 Tipo de estabelecimento - originalmente destinado"
    ],
    "gestao": ["1.4 Gestão do estabelecimento"],
    "data_inauguracao": ["1.6 Data de inauguração do estabelecimento"],
    "descricao_outro_regime": [
        "1.3 Capacidade do estabelecimento | Outro(s). Qual(is)?"
    ],
}

# Block 1.3 -> tipo_regime. The "Masculino"/"Feminino" level-1 entries are sex
# margins over these seven, not an eighth regime: summing all nine double counts.
CAPACITY_REGIMES: dict[str, str] = {
    "Presos provisórios": "provisorio",
    "Regime fechado": "fechado",
    "Regime semiaberto": "semiaberto",
    "Regime aberto": "aberto",
    "Regime Disciplinar Diferenciado (RDD)": "rdd",
    "Medidas de segurança de internação": "medida_seguranca_internacao",
    "Outro(s). Qual(is)?": "outro",
}

# Block 4.1 -> (situacao_processual, regime)
POPULATION_STATUS: dict[str, tuple[str, str]] = {
    # pyrefly: ignore [bad-assignment]
    "Presos provisórios (sem condenação)": ("provisorio", None),
    "Presos sentenciados - regime fechado": ("sentenciado", "fechado"),
    "Presos sentenciados - regime semiaberto": ("sentenciado", "semiaberto"),
    "Presos sentenciados - regime aberto": ("sentenciado", "aberto"),
    "Medida de segurança - internação": ("medida_seguranca", "internacao"),
    "Medida de segurança - tratamento ambulatorial": (
        "medida_seguranca",
        "tratamento_ambulatorial",
    ),
}

POPULATION_COURT: dict[str, str] = {
    "Justiça Estadual": "estadual",
    "Justiça Federal": "federal",
    "Outros(Just. Trab., cível)": "outros",
}

RDD_COLUMN = (
    "4.1 População prisional | "
    "Quantas pessoas privadas de liberdade estão em Regime Disciplinar Diferenciado?"
)

# Blocks 5.x shipped as marginals in populacao_caracteristica. Each is crossed
# only with sex; the source publishes no joint distribution across them.
CHARACTERISTIC_BLOCKS: dict[str, str] = {
    "5.1": "faixa_etaria",
    "5.2": "raca_cor",
    "5.4": "estado_civil",
    "5.6": "escolaridade",
}

RECORD_CONDITION_FLAG = "O estabelecimento tem condições de obter estas informações em seus registros?"

RECORD_CONDITION: dict[str, str] = {
    "Sim, para todas as pessoas privadas de liberdade": "todas",
    "Sim, para parte das pessoas privadas de liberdade": "parte",
    "Não": "nao",
}

# Level-1 entries inside a 5.x block that are margins or free text, not categories.
NON_CATEGORY_LEVEL1 = {
    RECORD_CONDITION_FLAG,
    "Masculino",
    "Feminino",
    "Total",
    "Se houver indígenas, destacar povo indígena ao qual pertence e respectivo idioma (campos abertos)",
}

SEX_LEVELS: dict[str, str] = {"Masculino": "masculino", "Feminino": "feminino"}

DEACTIVATED_BLOCK = "Celas interditadas/ desativadas e respectivas vagas"

TABLES = [
    "unidade_prisional",
    "populacao_prisional",
    "populacao_caracteristica",
    "uf_semestre",
    "unidade_crosswalk",
    "cobertura",
    "dicionario",
]

# Record linkage. Tuned on all 19 cycles: 0 same-cycle collisions, 96.3% of
# successor slots linked, 0.70% of links ambiguous.
MATCH_NAME_WEIGHT = 0.80
MATCH_CAPACITY_WEIGHT = 0.20
MATCH_THRESHOLD = 0.55
MATCH_GAP_THRESHOLD = 0.60
MATCH_AMBIGUITY_MARGIN = 0.05

# Raw downloads and cleaned parquet never go in the repo or under Dropbox.
DATA_ROOT = Path(
    os.environ.get(
        "SISDEPEN_DATA_ROOT", Path.home() / "Downloads" / "br_mj_sisdepen_data"
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

ARCHITECTURE_DIR = Path(__file__).resolve().parent / "architecture"
