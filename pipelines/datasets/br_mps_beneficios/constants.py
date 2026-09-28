"""Constants for the br_mps_beneficios pipeline (Prefect 3).

Benefícios concedidos (flow) and benefícios mantidos (stock) do INSS, extraídos
do SUIBE e publicados pelo Ministério da Previdência Social / INSS no portal de
dados abertos.

Three reference tables live here because the source does not publish them in a
machine-readable form alongside the data:

1. ``SALARIO_MINIMO`` — the source expresses the renda mensal inicial of a
   granted benefit only as a *multiple of the minimum wage* (``Qt SM RMI``), so
   a nominal BRL figure has to be reconstructed from the legal minimum wage in
   force in the competência month.
2. ``ESPECIE`` / ``CATEGORIA`` — the espécie code is stable across the 2019
   reform but its *label* is not (31 and 32 were both renamed by EC 103/2019),
   so a grouping keyed on the code is the only one that survives the reform.
3. ``municipio_crosswalk.csv`` — ``Mun Resid`` carries no municipality code, so
   the join to br_bd_diretorios_brasil.municipio is by name and needs an
   explicit exception list.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "br_mps_beneficios" / "code"


class constants(Enum):
    """Constants for the br_mps_beneficios pipeline."""

    DATASET_ID = "br_mps_beneficios"

    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    MUNICIPIO_CROSSWALK = _CODE_DIR / "municipio_crosswalk.csv"

    MUNICIPIO_DIRECTORY = _CODE_DIR / "municipio_directory.csv"
    MUNICIPIO_GEX_LOOKUP = _CODE_DIR / "municipio_gex_lookup.csv"

    # SUIBE abbreviates espécie labels inconsistently across eras. Expanding
    # these before matching against ESPECIE resolves 34 of the 39 labels
    # observed across 2017, Dec/2018 and Dec/2025 by exact normalised match.
    ABREV_EXPANSION = {
        "aposent": "aposentadoria",
        "previdenc": "previdenciario",
        "trab": "trabalhador",
        "rur": "rural",
        "amp": "amparo",
        "esp": "especial",
        "sind": "sindrome",
    }

    # Labels that abbreviation expansion still does not resolve against
    # ESPECIE. The first three are authoritative: the current XLSX extracts
    # print the code and the label side by side, so the code is read off the
    # data itself. Note that 31 and 32 appear here because the data kept its
    # PRE-reform labels ("Auxílio Doenca Previdenciário", "Aposentadoria
    # Invalidez Previdenciária") while the official dictionary lists the
    # post-EC 103/2019 names for the same codes — which is exactly why the
    # code, not the label, is the stable key. The remaining five were matched
    # to the dictionary individually; the runner-up candidate was at least
    # 0.06 further away in every case.
    ESPECIE_LABEL_ALIAS = {
        "auxilio doenca previdenciario": 31,
        "aposentadoria invalidez previdenciaria": 32,
        "amparo social pessoa portadora deficiencia": 87,
        "amparo previdenc invalidez trab rural": 11,
        "aposent invalidez acidentaria trab rur": 5,
        "aposent invalidez empregador rural": 6,
        "aposent por tempo servico ex combatente": 43,
        "aposentadoria por invalidez trab rural": 4,
        "aposentadoria compulsoria ex sasse": 81,
        "pensao especial vitalicia lei 9793 99": 54,
    }

    # CKAN instance that indexes every monthly extract. Resources themselves are
    # served from an S3 bucket whose object keys are not stable month to month
    # (they carry ad-hoc suffixes like "_consulta59807528"), so the pipeline
    # resolves URLs through the API rather than templating them.
    CKAN_BASE = (
        "https://dadosabertos.inss.gov.br/api/3/action/package_show?id="
    )

    PKG_CONCEDIDO_HIST = (
        "beneficios-concedidos-dez-2012-a-nov-2018-plano-de-dados-abertos-"
        "jun-2023-a-jun-2025"
    )
    PKG_CONCEDIDO_MID = "inss-beneficios-concedidos"
    PKG_CONCEDIDO_CUR = (
        "beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025"
    )
    PKG_MANTIDO_MID = "inss-beneficios-mantidos"
    PKG_MANTIDO_CUR = (
        "beneficios-mantidos-plano-de-dados-abertos-jun-2023-a-jun-2025"
    )

    # Nominal monthly minimum wage in BRL, keyed by the first competência
    # (YYYYMM) in which it applied. Verified against DIEESE's série histórica
    # and, for the two values that are not simple January adjustments, against
    # the instrument itself: MP 1.172/2023 (R$ 1.320 from 2023-05-01) and
    # Decreto 12.797/2025 (R$ 1.621 from 2026-01-01).
    SALARIO_MINIMO = {
        201201: 622.00,
        201301: 678.00,
        201401: 724.00,
        201501: 788.00,
        201601: 880.00,
        201701: 937.00,
        201801: 954.00,
        201901: 998.00,
        202001: 1039.00,
        202101: 1100.00,
        202201: 1212.00,
        202301: 1302.00,
        202305: 1320.00,
        202401: 1412.00,
        202501: 1518.00,
        202601: 1621.00,
    }

    # Official espécie code table, from "Dicionário de Dados - Espécies de
    # Benefício" (PDA 2025/2027). Labels are as published; note that 31 and 32
    # carry their post-EC 103/2019 names here while the older monthly extracts
    # still print the pre-reform names for the same codes.
    ESPECIE = {
        1: "Pensão por Morte de Trabalhador Rural",
        2: "Pensão por Morte Acidentária - Trabalhador Rural",
        3: "Pensão por Morte de Empregador Rural",
        4: "Aposentadoria por Invalidez - Trabalhador Rural",
        5: "Aposentadoria Invalidez Acidentária - Trabalhador Rural",
        6: "Aposentadoria Invalidez Empregador Rural",
        7: "Aposentadoria por Velhice - Trabalhador Rural",
        8: "Aposentadoria por Idade - Empregador Rural",
        10: "Auxílio Doença Acidentário - Trabalhador Rural",
        11: "Amparo Previdenciário Invalidez - Trabalhador Rural",
        12: "Amparo Previdenciário Idade - Trabalhador Rural",
        13: "Auxílio Doença - Trabalhador Rural",
        16: "Auxílio União",
        18: "Auxílio Inclusão à Pessoa com Deficiência",
        21: "Pensão por Morte Previdenciária",
        22: "Pensão por Morte Estatutária",
        23: "Pensão por Morte de Ex-Combatente",
        25: "Auxílio Reclusão",
        26: "Pensão por Morte Especial",
        27: "Pensão Morte Servidor Público Federal",
        28: "Pensão por Morte Regime Geral",
        29: "Pensão por Morte Ex-Combatente Marítimo",
        30: "Renda Mensal Vitalícia por Incapacidade",
        31: "Auxílio por Incapacidade Temporária",
        32: "Aposentadoria por Incapacidade Permanente",
        33: "Aposentadoria Invalidez Aeronauta",
        34: "Aposentadoria Invalidez Ex-Combatente Marítimo",
        36: "Auxílio Acidente Previdenciário",
        37: "Aposentadoria Extranumerário Capin",
        38: "Aposentadoria Extranumerário Funcionário Público",
        40: "Renda Mensal Vitalícia por Idade",
        41: "Aposentadoria por Idade",
        42: "Aposentadoria por Tempo de Contribuição",
        43: "Aposentadoria por Tempo Serviço Ex-Combatente",
        44: "Aposentadoria Especial de Aeronauta",
        45: "Aposentadoria Tempo Serviço Jornalista",
        46: "Aposentadoria Especial",
        47: "Abono Permanência em Serviço - 35 Anos",
        48: "Abono Permanência em Serviço - 30 Anos",
        49: "Aposentadoria Ordinária",
        51: "Aposentadoria Invalidez Extinto Plano Básico",
        54: "Pensão Indenizatória a Cargo da União",
        55: "Pensão por Morte Extinto Plano Básico",
        56: "Pensão Vitalícia Síndrome Talidomida",
        57: "Aposentadoria Tempo de Serviço de Professor",
        58: "Aposentadoria de Anistiados",
        59: "Pensão por Morte de Anistiados",
        60: "Benefício Indenizatório a Cargo da União",
        72: "Aposentadoria Tempo Serviço - Lei de Guerra",
        79: "Vantagens de Servidor Aposentado",
        80: "Auxílio Salário Maternidade",
        81: "Aposentadoria por Idade Compulsória Ex-Sasse",
        82: "Aposentadoria Tempo de Serviço Ex-Sasse",
        83: "Aposentadoria por Invalidez Ex-Sasse",
        84: "Pensão por Morte Ex-Sasse",
        85: "Pensão Vitalícia Seringueiros",
        86: "Pensão Vitalícia Dependentes Seringueiro",
        87: "Amparo Social à Pessoa Portadora de Deficiência",
        88: "Amparo Social ao Idoso",
        89: "Pensão Especial Vítimas Hemodiálise - Caruaru",
        91: "Auxílio Doença por Acidente do Trabalho",
        92: "Aposentadoria Invalidez Acidente Trabalho",
        93: "Pensão por Morte Acidente do Trabalho",
        94: "Auxílio Acidente",
        95: "Auxílio Suplementar Acidente Trabalho",
        96: "Pensão Especial Hanseníase Lei 11520/07",
    }

    # Reform-stable functional grouping, keyed on the espécie CODE. It is also
    # truncation-stable: benefícios mantidos publishes only the first 20
    # characters of the label, which is ambiguous between espécies for 14 keys,
    # but every one of those keys resolves to a single categoria. Espécie 59
    # (Pensão por Morte de Anistiados) sits in pensao_morte for exactly that
    # reason — it shares the prefix "Pensão por Morte de " with 1, 3 and 23, and
    # its special-statute character is carried by natureza_beneficio instead.
    # Espécies 23 and 29 are grouped the same way for the same reason.
    # EC 103/2019
    # renamed espécies 31 and 32 without changing their codes and closed 42 to
    # new entrants, so a grouping built on labels would break at Nov/2019 while
    # this one does not. Every code in ESPECIE appears in exactly one group
    # (asserted by utils.validate_reference_tables).
    CATEGORIA = {
        "aposentadoria_idade": [7, 8, 41, 81],
        "aposentadoria_tempo_contribuicao": [
            37,
            38,
            42,
            43,
            45,
            49,
            57,
            72,
            82,
        ],
        "aposentadoria_invalidez": [4, 5, 6, 32, 33, 34, 51, 83, 92],
        "aposentadoria_especial": [44, 46],
        "pensao_morte": [1, 2, 3, 21, 22, 23, 26, 27, 28, 29, 55, 59, 84, 93],
        "auxilio_incapacidade_temporaria": [10, 13, 31, 91],
        "auxilio_acidente": [36, 94, 95],
        "salario_maternidade": [80],
        "auxilio_reclusao": [25],
        "bpc_loas": [87, 88],
        "renda_mensal_vitalicia": [11, 12, 30, 40],
        "auxilio_inclusao": [18],
        "beneficio_especial_legislacao": [
            16,
            54,
            56,
            58,
            60,
            85,
            86,
            89,
            96,
        ],
        "abono_vantagem": [47, 48, 79],
    }

    # Nature of the benefit. Everything not listed is contributory RGPS
    # ("previdenciaria"). The assistencial set is what must NOT be summed with
    # br_cgu_beneficios_cidadao: espécies 87 and 88 are the BPC/LOAS, which
    # that dataset also carries at person level.
    NATUREZA = {
        "assistencial": [11, 12, 18, 30, 40, 87, 88],
        "acidentaria": [2, 5, 10, 36, 91, 92, 93, 94, 95],
        "indenizatoria": [16, 54, 56, 58, 59, 60, 85, 86, 89, 96],
    }

    # Espécies that EC 103/2019 renamed without changing the code. The monthly
    # extracts still print these older labels, so both spellings must resolve.
    NOME_ANTERIOR = {
        31: "Auxílio Doença Previdenciário",
        32: "Aposentadoria por Invalidez Previdenciária",
    }

    OBSERVACAO = {
        59: "Pensão por morte concedida sob a legislação de anistia. Agrupada em pensao_morte, com a natureza indenizatória registrada em natureza_beneficio",
        54: "Pensão especial vitalícia da Lei 9.793/1999, paga pela União. O dicionário oficial do MPS a registra sob o rótulo genérico Pensão Indenizatória a Cargo da União; os extratos mensais imprimem o nome da lei",
        60: "Pensão especial mensal vitalícia da Lei 10.923/2004, paga pela União",
        81: "Aposentadoria por idade compulsória do extinto SASSE, extinta pela Lei 6.430/1977 e mantida apenas para o estoque. Ausente do Dicionário de Dados - Espécies de Benefício publicado em 2025, embora presente nos extratos de concessões até 2012",
        18: "Criado pela Lei 14.176/2021 para beneficiários do BPC que exercem atividade remunerada",
        30: "Renda mensal vitalícia, extinta pela Lei 8.213/1991 e substituída pelo BPC; estoque residual",
        40: "Renda mensal vitalícia, extinta pela Lei 8.213/1991 e substituída pelo BPC; estoque residual",
        31: "Renomeada de Auxílio Doença Previdenciário para Auxílio por Incapacidade Temporária pela EC 103/2019, sem mudança de código",
        32: "Renomeada de Aposentadoria por Invalidez Previdenciária para Aposentadoria por Incapacidade Permanente pela EC 103/2019, sem mudança de código",
        42: "Fechada a novos ingressos pela EC 103/2019, que manteve apenas as regras de transição",
        87: "Benefício de Prestação Continuada (BPC/LOAS) à pessoa com deficiência, assistencial e não contributivo. Também presente, em nível de pessoa, no conjunto br_cgu_beneficios_cidadao",
        88: "Benefício de Prestação Continuada (BPC/LOAS) ao idoso, assistencial e não contributivo. Também presente, em nível de pessoa, no conjunto br_cgu_beneficios_cidadao",
    }

    # Age bands follow the AEPS "faixas de idade" presentation so the table can
    # be compared against the published yearbook without rebanding.
    FAIXA_ETARIA_BINS = [
        0,
        16,
        20,
        25,
        30,
        35,
        40,
        45,
        50,
        55,
        60,
        65,
        70,
        999,
    ]
    FAIXA_ETARIA_LABELS = [
        "até 15 anos",
        "16 a 19 anos",
        "20 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
        "70 anos ou mais",
    ]

    # Sentinel written by SUIBE when the municipality of residence is absent.
    # 7.3% of the rows in the Dec/2025 extract carry it; those rows are kept
    # with a null id_municipio rather than dropped.
    MUN_SENTINELS = ("00000-Zerada", "Zerada", "{ñ class}", "")
