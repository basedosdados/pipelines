"""Constantes do br_sedec_desastres."""

from enum import Enum

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    PartBdpro,
)

_VIGENTES_PAINEL = (
    "//h3[contains(normalize-space(.), 'Reconhecimentos vigentes')]"
)
_VIGENTES_CONTEUDO = f"{_VIGENTES_PAINEL}/following-sibling::div[contains(@class,'ui-accordion-content')][1]"

_REALIZADOS_PAINEL = (
    "//h3[contains(normalize-space(.), 'Reconhecimentos realizados')]"
)
_REALIZADOS_CONTEUDO = f"{_REALIZADOS_PAINEL}/following-sibling::div[contains(@class,'ui-accordion-content')][1]"
_REALIZADOS_PERIODO = f"{_REALIZADOS_CONTEUDO}//td[starts-with(normalize-space(.), '* Período')]/following-sibling::td[1]"


class constants(Enum):
    """Constantes comuns às tabelas do br_sedec_desastres."""

    DATASET_ID = "br_sedec_desastres"

    DATE_FORMAT = "%Y-%m-%d"

    BASE_URL = "https://s2id.mi.gov.br/paginas/relatorios/"

    USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    ELEMENT_TIMEOUT = 120
    DOWNLOAD_TIMEOUT = 300


class reconhecimentos_vigentes(Enum):
    """Constantes da tabela reconhecimentos_vigentes."""

    TABLE_ID = "reconhecimentos_vigentes"

    XPATHS = {
        "painel": _VIGENTES_PAINEL,
        "todas_tipologias": (
            f"{_VIGENTES_CONTEUDO}"
            "//label[normalize-space(.)='Todas as tipologias de desastres']"
        ),
        "estado_select_oculto": (
            f"{_VIGENTES_CONTEUDO}"
            "//select[option[@value='AC'] and option[@value='SP']]"
        ),
        "estado_widget": "//*[@id='{base}']",
        "estado_item": "//div[@id='{base}_panel']//li[@data-label='{uf_nome}']",
        "exportar_csv": (
            f"{_VIGENTES_CONTEUDO}"
            "//button[span[normalize-space(.)='Exportar CSV']]"
        ),
    }

    COLUNAS = [
        {
            "name": "data_extracao",
            "bigquery_type": "DATE",
            "original_name": "",
        },
        {"name": "sigla_uf", "bigquery_type": "STRING", "original_name": "UF"},
        {
            "name": "id_municipio",
            "bigquery_type": "STRING",
            "original_name": "Município",
        },
        {
            "name": "id_cobrade",
            "bigquery_type": "STRING",
            "original_name": "COBRADE",
        },
        {
            "name": "nome_cobrade",
            "bigquery_type": "STRING",
            "original_name": "COBRADE",
        },
        {
            "name": "situacao",
            "bigquery_type": "STRING",
            "original_name": "Situação",
        },
        {
            "name": "data_ocorrencia",
            "bigquery_type": "DATE",
            "original_name": "Data de Ocorrência",
        },
        {
            "name": "data_vigencia",
            "bigquery_type": "DATE",
            "original_name": "Data da Vigência",
        },
    ]

    STAGING_SUBSTITUICOES = {"id_municipio": "nome_municipio"}

    COVERAGE = AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    )


class reconhecimentos_realizados(Enum):
    """Constantes da tabela reconhecimentos_realizados."""

    TABLE_ID = "reconhecimentos_realizados"

    PRIMEIRO_ANO = 2013

    COLUNA_DATA = "data_dou"

    XPATHS = {
        "painel": _REALIZADOS_PAINEL,
        "data_inicial": f"{_REALIZADOS_PERIODO}/span[1]/input",
        "data_final": f"{_REALIZADOS_PERIODO}/span[2]/input",
        "todas_tipologias": (
            f"{_REALIZADOS_CONTEUDO}"
            "//label[normalize-space(.)='Todas as tipologias de desastres']"
        ),
        "todos_estados": (
            f"{_REALIZADOS_CONTEUDO}"
            "//label[normalize-space(.)='Todas os estados']"
        ),
        "exportar_csv": (
            f"{_REALIZADOS_CONTEUDO}"
            "//button[span[normalize-space(.)='Exportar CSV']]"
        ),
    }

    COLUNAS = [
        {"name": "ano", "bigquery_type": "INT64", "original_name": ""},
        {"name": "sigla_uf", "bigquery_type": "STRING", "original_name": "UF"},
        {
            "name": "id_municipio",
            "bigquery_type": "STRING",
            "original_name": "Código IBGE",
        },
        {
            "name": "id_cobrade",
            "bigquery_type": "STRING",
            "original_name": "Desastre",
        },
        {
            "name": "nome_cobrade",
            "bigquery_type": "STRING",
            "original_name": "Desastre",
        },
        {
            "name": "situacao",
            "bigquery_type": "STRING",
            "original_name": "SE/ECP",
        },
        {"name": "rito", "bigquery_type": "STRING", "original_name": "Rito"},
        {
            "name": "numero_decreto",
            "bigquery_type": "STRING",
            "original_name": "Nº do Decreto",
        },
        {
            "name": "data_decreto",
            "bigquery_type": "DATE",
            "original_name": "Data do Decreto",
        },
        {
            "name": "numero_portaria",
            "bigquery_type": "STRING",
            "original_name": "Nº da Portaria",
        },
        {
            "name": "data_portaria",
            "bigquery_type": "DATE",
            "original_name": "Data da Portaria",
        },
        {
            "name": "numero_dou",
            "bigquery_type": "STRING",
            "original_name": "Nº do D.O.U.",
        },
        {
            "name": "data_dou",
            "bigquery_type": "DATE",
            "original_name": "Data do D.O.U.",
        },
        {
            "name": "numero_processo",
            "bigquery_type": "STRING",
            "original_name": "Processo",
        },
    ]

    COBRADE = {
        "11110": "Tremor de terra",
        "11120": "Tsunami",
        "11200": "Emanação vulcânica",
        "11311": "Quedas, Tombamentos e rolamentos - Blocos",
        "11312": "Quedas, Tombamentos e rolamentos - Lascas",
        "11313": "Quedas, Tombamentos e rolamentos - Matacões",
        "11314": "Quedas, Tombamentos e rolamentos - Lajes",
        "11321": "Deslizamentos",
        "11331": "Corridas de Massa - Solo/Lama",
        "11332": "Corridas de Massa - Rocha/detrito",
        "11340": "Subsidências e colapsos",
        "11410": "Erosão Costeira/Marinha",
        "11420": "Erosão de Margem Fluvial",
        "11431": "Erosão Continental - Laminar",
        "11432": "Erosão Continental - Ravinas",
        "11433": "Erosão Continental - Boçorocas",
        "12100": "Inundações",
        "12200": "Enxurradas",
        "12300": "Alagamentos",
        "13111": "Ciclones - Ventos Costeiros (Mobilidade de Dunas)",
        "13112": "Ciclones - Marés de Tempestade (Ressacas)",
        "13120": "Frentes Frias/Zonas de Convergência",
        "13211": "Tempestade Local/Convectiva - Tornados",
        "13212": "Tempestade Local/Convectiva - Tempestade de Raios",
        "13213": "Tempestade Local/Convectiva - Granizo",
        "13214": "Tempestade Local/Convectiva - Chuvas Intensas",
        "13215": "Tempestade Local/Convectiva - Vendaval",
        "13310": "Onda de Calor",
        "13321": "Onda de Frio - Friagem",
        "13322": "Onda de Frio - Geadas",
        "14110": "Estiagem",
        "14120": "Seca",
        "14131": "Incêndio Florestal - Incêndios em Parques, Áreas de Proteção Ambiental e Áreas de Preservação Permanente Nacionais, Estaduais ou Municipais",
        "14132": "Incêndio Florestal - Incêndios em áreas não protegidas, com reflexos na qualidade do ar",
        "14140": "Baixa Umidade do Ar",
        "15110": "Doenças infecciosas virais",
        "15120": "Doenças infecciosas bacterianas",
        "15130": "Doenças infecciosas parasíticas",
        "15140": "Doenças infecciosas fúngicas",
        "15210": "Infestações de animais",
        "15221": "Infestações de algas - Marés vermelhas",
        "15222": "Infestações de algas - Ciano bactérias em reservatórios",
        "15230": "Outras Infestações",
        "21110": "Queda de satélite (radionuclídeos)",
        "21210": "Fontes radioativas em processos de produção",
        "21310": "Outras fontes de liberação de radionuclídeos para o meio ambiente",
        "22110": "Liberação de produtos químicos para a atmosfera causada por explosão ou incêndio",
        "22210": "Liberação de produtos químicos nos sistemas de água potável",
        "22220": "Derramamento de produtos químicos em ambiente lacustre, fluvial e marinho",
        "22310": "Liberação produtos químicos e contaminação como conseqüência de ações militares.",
        "22410": "Transporte de produtos perigosos rodoviário",
        "22420": "Transporte de produtos perogosos ferroviário",
        "22430": "Transporte de produtos perigosos aéreo",
        "22440": "Transporte de produtos perigosos dutoviário",
        "22450": "Transporte de produtos perigosos marítimo",
        "22460": "Transporte de produtos perigosos aquaviário",
        "23110": "Incêndios em plantas e distritos industriais, parques e depósitos",
        "23120": "Incêndios em aglomerados residenciais",
        "24100": "Colapso de edificações",
        "24200": "Rompimento/colapso de barragens",
        "25100": "Transporte passageiros e cargas não perigosas rodoviário",
        "25200": "Transporte passageiros e cargas não perigosas ferroviário",
        "25300": "Transporte passageiros e cargas não perigosas aéreo",
        "25400": "Transporte de passageiros e cargas não perigosas marítimo",
        "25500": "Transporte passageiros e cargas não perigosas aquaviário",
    }

    COVERAGE = PartBdpro(
        date_column=DateOnly(col=COLUNA_DATA),
        date_format=DateFormat.YEAR_MD,
    )
