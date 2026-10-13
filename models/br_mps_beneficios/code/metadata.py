"""Dataset and table metadata for br_mps_beneficios, in PT/EN/ES.

Kept as code so the descriptions are reviewable in the PR and so the
registration step reads exactly what was approved, rather than being retyped
into API calls.

The dataset description carries an explicit warning about BPC. Espécies 87 and
88 are the Benefício de Prestação Continuada, and the dataset
``br_cgu_beneficios_cidadao`` publishes the same population at person level in
its ``bpc`` table. Measured against each other: 5,375,298 BPC benefits in this
dataset's stock for Jun/2023, against 5,832,383 distinct benefits in CGU for
2023. Adding the two together counts the same benefits twice.
"""

DATASET_SLUG = "beneficios_do_instituto_nacional_de_seguro_social_inss"

# The shell's organization is `me`; the publisher today is the recreated
# Ministério da Previdência Social, which exists as the `mps` organization.
ORGANIZATION_SLUG = "mps"
THEME_SLUGS = ("government", "economics")
# All verified against a prod tag listing, so nothing new has to be created.
# Note the vocabulary mixes separators -- "social_security" is underscored while
# "social-assistance" is hyphenated -- so the slugs are used exactly as they
# exist rather than normalised. No area, theme or organization is tagged here:
# those are separate metadata fields.
TAG_SLUGS = (
    "benefit",
    "social_security",
    "retirement",
    "pension",
    "social-assistance",
    "income",
    "elderly",
    "transfer",
)

DATASET = {
    "description_pt": (
        "Benefícios concedidos e mantidos pelo Instituto Nacional do Seguro "
        "Social (INSS), agregados por município de residência do titular, mês, "
        "espécie do benefício, clientela, sexo e faixa etária, a partir dos "
        "microdados do Sistema Único de Informações de Benefícios (SUIBE) "
        "publicados em dados abertos. Os valores são nominais, sem "
        "deflacionamento. Os benefícios assistenciais de prestação continuada "
        "(BPC/LOAS, espécies 87 e 88) também constam, em nível de pessoa, no "
        "conjunto br_cgu_beneficios_cidadao: somar os dois conjuntos conta os "
        "mesmos benefícios duas vezes."
    ),
    "description_en": (
        "Benefits granted and maintained by the National Social Security "
        "Institute (INSS), aggregated by the beneficiary's municipality of "
        "residence, month, benefit type, clientele, sex and age band, built "
        "from the SUIBE microdata published as open data. Values are nominal, "
        "with no deflation applied. The continuous-payment social assistance "
        "benefits (BPC/LOAS, benefit types 87 and 88) also appear at person "
        "level in the br_cgu_beneficios_cidadao dataset: adding the two "
        "datasets together counts the same benefits twice."
    ),
    "description_es": (
        "Beneficios concedidos y mantenidos por el Instituto Nacional del "
        "Seguro Social (INSS), agregados por municipio de residencia del "
        "titular, mes, especie del beneficio, clientela, sexo y grupo de edad, "
        "a partir de los microdatos del SUIBE publicados en datos abiertos. "
        "Los valores son nominales, sin deflactar. Los beneficios "
        "asistenciales de prestación continuada (BPC/LOAS, especies 87 y 88) "
        "también figuran, a nivel de persona, en el conjunto "
        "br_cgu_beneficios_cidadao: sumar ambos conjuntos cuenta los mismos "
        "beneficios dos veces."
    ),
}

TABLES = {
    "beneficio_concedido_municipio_mes": {
        "name_pt": "Benefícios concedidos por município e mês",
        "name_en": "Benefits granted by municipality and month",
        "name_es": "Beneficios concedidos por municipio y mes",
        "description_pt": (
            "Quantidade e valor dos benefícios concedidos pelo INSS a cada mês, "
            "por município de residência do titular, espécie, clientela, sexo e "
            "faixa etária. Mede o fluxo de novas concessões, e não o estoque. A "
            "fonte informa a renda mensal inicial apenas como múltiplo do "
            "salário mínimo, de modo que valor_total é a conversão para reais "
            "nominais pelo salário mínimo vigente na competência."
        ),
        "description_en": (
            "Count and value of benefits granted by INSS each month, by the "
            "beneficiary's municipality of residence, benefit type, clientele, "
            "sex and age band. It measures the flow of new grants, not the "
            "stock. The source reports the initial monthly income only as a "
            "multiple of the minimum wage, so valor_total is the conversion to "
            "nominal BRL at the minimum wage in force in that month."
        ),
        "description_es": (
            "Cantidad y valor de los beneficios concedidos por el INSS cada "
            "mes, por municipio de residencia del titular, especie, clientela, "
            "sexo y grupo de edad. Mide el flujo de nuevas concesiones, no el "
            "stock. La fuente informa la renta mensual inicial solo como "
            "múltiplo del salario mínimo, por lo que valor_total es la "
            "conversión a reales nominales según el salario mínimo vigente."
        ),
    },
    "beneficio_mantido_municipio_mes": {
        "name_pt": "Benefícios mantidos por município e mês",
        "name_en": "Benefits maintained by municipality and month",
        "name_es": "Beneficios mantenidos por municipio y mes",
        "description_pt": (
            "Quantidade e valor dos benefícios ativos mantidos pelo INSS a cada "
            "mês, por município de residência do titular, espécie, clientela, "
            "sexo e faixa etária. Mede o estoque de benefícios em manutenção, e "
            "não o fluxo de concessões. A fonte não publica o código da espécie "
            "e trunca o rótulo em 20 caracteres, de modo que especie_beneficio "
            "fica nulo quando o rótulo truncado é ambíguo entre espécies; "
            "categoria_beneficio está sempre preenchida e "
            "especie_beneficio_rotulo traz o rótulo como publicado. A série cobre 48 meses entre julho de 2021 e janeiro de 2026: a fonte não publicou abril de 2024, publicou setembro de 2023 com o cabeçalho trocado em relação às colunas (sem espécie, clientela e sexo), e reeditou o arquivo do mês anterior, byte a byte, sob os rótulos de junho a agosto de 2024, fevereiro de 2025, setembro de 2025 e fevereiro e março de 2026, que por isso não constam."
        ),
        "description_en": (
            "Count and value of active benefits maintained by INSS each month, "
            "by the beneficiary's municipality of residence, benefit type, "
            "clientele, sex and age band. It measures the stock of benefits in "
            "payment, not the flow of new grants. The source publishes no "
            "benefit-type code and truncates the label to 20 characters, so "
            "especie_beneficio is null wherever the truncated label is "
            "ambiguous between benefit types; categoria_beneficio is always "
            "populated and especie_beneficio_rotulo carries the label as "
            "published. The series covers 48 months between July 2021 and January 2026: the source did not publish April 2024, published September 2023 with a header that does not match its columns (benefit type, clientele and sex absent), and reissued the previous month's file byte for byte under the June-August 2024, February 2025, September 2025, and February and March 2026 labels, which are therefore absent."
        ),
        "description_es": (
            "Cantidad y valor de los beneficios activos mantenidos por el INSS "
            "cada mes, por municipio de residencia del titular, especie, "
            "clientela, sexo y grupo de edad. Mide el stock de beneficios en "
            "pago, no el flujo de concesiones. La fuente no publica el código "
            "de la especie y trunca la etiqueta en 20 caracteres, por lo que "
            "especie_beneficio queda nulo cuando la etiqueta truncada es "
            "ambigua; categoria_beneficio siempre está completa y "
            "especie_beneficio_rotulo trae la etiqueta tal como se publica. La serie cubre 48 meses entre julio de 2021 y enero de 2026: la fuente no publicó abril de 2024, publicó septiembre de 2023 con el encabezado desalineado respecto a las columnas (sin especie, clientela ni sexo), y reeditó el archivo del mes anterior, byte a byte, bajo las etiquetas de junio a agosto de 2024, febrero de 2025, septiembre de 2025, y febrero y marzo de 2026, que por ello no constan."
        ),
    },
    "dicionario_especie": {
        "name_pt": "Dicionário de espécies de benefício",
        "name_en": "Benefit type dictionary",
        "name_es": "Diccionario de especies de beneficio",
        "description_pt": (
            "Tabela de códigos das espécies de benefício do INSS, com o nome "
            "oficial, a natureza do benefício e um agrupamento funcional "
            "construído sobre o código. A Emenda Constitucional 103/2019 "
            "renomeou as espécies 31 e 32 sem alterar seus códigos, e os "
            "extratos mensais seguem imprimindo os nomes anteriores, "
            "registrados em nome_especie_anterior."
        ),
        "description_en": (
            "Code table for INSS benefit types, with the official name, the "
            "nature of the benefit and a functional grouping built on the code. "
            "Constitutional Amendment 103/2019 renamed benefit types 31 and 32 "
            "without changing their codes, and the monthly extracts still print "
            "the former names, recorded in nome_especie_anterior."
        ),
        "description_es": (
            "Tabla de códigos de las especies de beneficio del INSS, con el "
            "nombre oficial, la naturaleza del beneficio y una agrupación "
            "funcional construida sobre el código. La Enmienda Constitucional "
            "103/2019 renombró las especies 31 y 32 sin cambiar sus códigos, y "
            "los extractos mensuales siguen imprimiendo los nombres anteriores, "
            "registrados en nome_especie_anterior."
        ),
    },
}

# One raw data source per table: client._raw_source_id raises when a table has
# two or more, which breaks any future recurring pipeline at its first poll.
#
# Link the raw sources the dados.gov.br harvest already created on this dataset
# rather than creating new ones — run get_raw_data_sources first. Three
# duplicates were created here before that check was run, and the MCP has no
# delete for a RawDataSource, so they can only be cleaned up in Django admin.
# The IDs below are the harvested stubs; re-resolve them per environment by URL
# rather than trusting the ID to carry over.
#
# No stub covers the glossary, so dicionario_especie keeps its own raw source.
# The five harvested stubs also include "Benefícios Emitidos" and "Benefícios
# Indeferidos", which no table here uses, and a second concedido package
# covering Dec/2018-May/2023; only one may be linked, so the ongoing package is
# the one linked.
RAW_SOURCES = {
    "beneficio_concedido_municipio_mes": {
        "name": "Benefícios Concedidos (A partir de junho de 2023)",
        "url": "https://dados.gov.br/dados/conjuntos-dados/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025",
        "id_staging": "64954003-2f8f-4bde-89df-48142dee74bf",
    },
    "beneficio_mantido_municipio_mes": {
        "name": "Benefícios Mantidos",
        "url": "https://dados.gov.br/dados/conjuntos-dados/beneficios-mantidos-plano-de-dados-abertos-jun-2023-a-jun-2025",
        "id_staging": "46e32460-cbac-47f3-a1b8-f49205833c27",
    },
    "dicionario_especie": {
        "name": "Dicionário de Dados - Espécies de Benefício",
        "url": "https://dadosabertos.inss.gov.br/dataset/glossarios-dos-arquivos-de-beneficios-plano-de-dados-abertos-jun-2023-a-jun-2025",
        "id_staging": "7f6741c9-ac9d-4407-9bc9-402d48493c83",
    },
}

COVERAGE = {
    "beneficio_concedido_municipio_mes": (2012, 1, 2026, 8),
    "beneficio_mantido_municipio_mes": (2021, 7, 2026, 1),
    "dicionario_especie": None,
}
