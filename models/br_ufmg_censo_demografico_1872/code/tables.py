"""Table-level trilingual metadata for br_ufmg_censo_demografico_1872."""

from __future__ import annotations

DATASET_ID = "br_ufmg_censo_demografico_1872"
DATASET_SLUG = "censo_demografico_1872"

# What each published table stem contains, and the axis its id_categoria varies
# over. `sexo` is set where the sex is a property of the table rather than of
# the columns, which is what lets the origin tables carry gendered column names.
STEMS: dict[str, dict] = {
    "domicilio": dict(
        sexo=None,
        pt="Casas habitadas, casas desabitadas e fogos",
        en="Inhabited houses, uninhabited houses and fogos",
        es="Casas habitadas, casas deshabitadas y fogos",
        eixo_pt=None,
        eixo_en=None,
        eixo_es=None,
        unidade="dwelling",
    ),
    "populacao_geral": dict(
        sexo=None,
        pt="População por sexo e condição, livre ou escravizada",
        en="Population by sex and condition, free or enslaved",
        es="Población por sexo y condición, libre o esclavizada",
        eixo_pt="cor, estado civil, religião, nacionalidade, instrução, defeitos físicos, "
        "ausentes e transeuntes",
        eixo_en="colour, marital status, religion, nationality, literacy and schooling, "
        "physical disabilities, absent and transient population",
        eixo_es="color, estado civil, religión, nacionalidad, instrucción, discapacidades "
        "físicas, ausentes y transeúntes",
        unidade="person",
    ),
    "populacao_presente_idade": dict(
        sexo=None,
        pt="População presente por sexo, cor e condição, livre ou escravizada",
        en="Population present by sex, colour and condition, free or enslaved",
        es="Población presente por sexo, color y condición, libre o esclavizada",
        eixo_pt="faixa de idade, do primeiro mês de vida a mais de 100 anos",
        eixo_en="age band, from the first month of life to over 100 years",
        eixo_es="grupo de edad, desde el primer mes de vida hasta más de 100 años",
        unidade="person",
    ),
    "populacao_ausente_idade": dict(
        sexo=None,
        pt="População ausente da paróquia por sexo, cor e condição, livre ou escravizada",
        en="Population absent from the parish by sex, colour and condition, free or enslaved",
        es="Población ausente de la parroquia por sexo, color y condición, libre o esclavizada",
        eixo_pt="faixa de idade, do primeiro mês de vida a mais de 100 anos",
        eixo_en="age band, from the first month of life to over 100 years",
        eixo_es="grupo de edad, desde el primer mes de vida hasta más de 100 años",
        unidade="person",
    ),
    "populacao_total_idade": dict(
        sexo=None,
        pt="População presente e ausente somadas, por sexo, cor e condição",
        en="Population present and absent combined, by sex, colour and condition",
        es="Población presente y ausente sumadas, por sexo, color y condición",
        eixo_pt="faixa de idade, do primeiro mês de vida a mais de 100 anos",
        eixo_en="age band, from the first month of life to over 100 years",
        eixo_es="grupo de edad, desde el primer mes de vida hasta más de 100 años",
        unidade="person",
    ),
    "homem_origem_brasileira": dict(
        sexo="homens",
        pt="Homens de nacionalidade brasileira por estado civil, cor e condição",
        en="Men of Brazilian nationality by marital status, colour and condition",
        es="Hombres de nacionalidad brasileña por estado civil, color y condición",
        eixo_pt="província de nascimento, mais brasileiros adotivos e estrangeiros naturalizados",
        eixo_en="province of birth, plus adoptive Brazilians and naturalised foreigners",
        eixo_es="provincia de nacimiento, más brasileños adoptivos y extranjeros naturalizados",
        unidade="person",
    ),
    "mulher_origem_brasileira": dict(
        sexo="mulheres",
        pt="Mulheres de nacionalidade brasileira por estado civil, cor e condição",
        en="Women of Brazilian nationality by marital status, colour and condition",
        es="Mujeres de nacionalidad brasileña por estado civil, color y condición",
        eixo_pt="província de nascimento, mais brasileiras adotivas e estrangeiras naturalizadas",
        eixo_en="province of birth, plus adoptive Brazilians and naturalised foreigners",
        eixo_es="provincia de nacimiento, más brasileñas adoptivas y extranjeras naturalizadas",
        unidade="person",
    ),
    "estrangeiro_nacionalidade": dict(
        sexo=None,
        pt="Estrangeiros residentes por religião, estado civil e sexo",
        en="Resident foreigners by religion, marital status and sex",
        es="Extranjeros residentes por religión, estado civil y sexo",
        eixo_pt="nacionalidade de origem, incluindo africanos escravos e africanos livres",
        eixo_en="nationality of origin, including enslaved and free Africans",
        eixo_es="nacionalidad de origen, incluyendo africanos esclavos y africanos libres",
        unidade="person",
    ),
    "profissao": dict(
        sexo=None,
        pt="População por nacionalidade, estado civil e sexo",
        en="Population by nationality, marital status and sex",
        es="Población por nacionalidad, estado civil y sexo",
        eixo_pt="profissão, agrupada em profissões liberais, industriais e comerciais, "
        "manuais e mecânicas, agrícolas, e sem profissão",
        eixo_en="occupation, grouped into liberal, industrial and commercial, manual and "
        "mechanical, and agricultural professions, plus those without an occupation",
        eixo_es="profesión, agrupada en profesiones liberales, industriales y comerciales, "
        "manuales y mecánicas, agrícolas, y sin profesión",
        unidade="person",
    ),
    "resumo_geral": dict(
        sexo=None,
        pt="Quadro resumo da população por sexo e condição, livre ou escravizada",
        en="Summary table of the population by sex and condition, free or enslaved",
        es="Cuadro resumen de la población por sexo y condición, libre o esclavizada",
        eixo_pt="todos os eixos do recenseamento reunidos: categorias gerais, faixas de idade, "
        "origem brasileira, nacionalidade estrangeira e profissão",
        eixo_en="every axis of the census combined: general categories, age bands, Brazilian "
        "origin, foreign nationality and occupation",
        eixo_es="todos los ejes del censo reunidos: categorías generales, grupos de edad, "
        "origen brasileño, nacionalidad extranjera y profesión",
        unidade="person",
    ),
}

NIVEIS = {
    "paroquia": ("paróquia", "parish", "parroquia"),
    "municipio": (
        "município de 1872",
        "1872 municipality",
        "municipio de 1872",
    ),
    "provincia": ("província", "province", "provincia"),
}

VERSOES = {
    "original": (
        "Reproduz os quadros como publicados pela Diretoria Geral de Estatística em 1872",
        "Reproduces the tables as published by the Diretoria Geral de Estatistica in 1872",
        "Reproduce los cuadros tal como los publicó la Diretoria Geral de Estatística en 1872",
    ),
    "corrigido": (
        "Incorpora as correções aritméticas do NPHED/Cedeplar documentadas no "
        "Relatório crítico do censo de 1872",
        "Incorporates the arithmetic corrections by NPHED/Cedeplar documented in their "
        "critical report on the 1872 census",
        "Incorpora las correcciones aritméticas del NPHED/Cedeplar documentadas en el "
        "informe crítico del censo de 1872",
    ),
}


def table_description(
    stem: str, versao: str, level: str
) -> tuple[str, str, str]:
    """Trilingual description for one published table."""
    s = STEMS[stem]
    n_pt, n_en, n_es = NIVEIS[level]
    v_pt, v_en, v_es = VERSOES[versao]

    pt = f"{s['pt']}, por {n_pt}"
    en = f"{s['en']}, by {n_en}"
    es = f"{s['es']}, por {n_es}"
    if s["eixo_pt"]:
        pt += f" e por {s['eixo_pt']}"
        en += f" and by {s['eixo_en']}"
        es += f" y por {s['eixo_es']}"
    return f"{pt}. {v_pt}.", f"{en}. {v_en}.", f"{es}. {v_es}."


# The three geography lookup tables and the dictionary.
AUXILIARES: dict[str, dict] = {
    "provincia": dict(
        cols=["id_provincia", "nome_provincia"],
        key=["id_provincia"],
        pt="Províncias do Império do Brasil recenseadas em 1872, com os códigos usados no "
        "banco Pop-72 e os nomes na grafia da época.",
        en="Provinces of the Empire of Brazil enumerated in 1872, with the codes used in the "
        "Pop-72 database and names in their period spelling.",
        es="Provincias del Imperio del Brasil censadas en 1872, con los códigos usados en la "
        "base Pop-72 y los nombres en la grafía de la época.",
    ),
    "municipio": dict(
        cols=["id_provincia", "id_municipio_1872", "nome_municipio"],
        key=["id_municipio_1872"],
        pt="Municípios do Império do Brasil recenseados em 1872, com os códigos usados no "
        "banco Pop-72. Os códigos não correspondem aos do IBGE e não há tradutor oficial "
        "para os municípios atuais.",
        en="Municipalities of the Empire of Brazil enumerated in 1872, with the codes used in "
        "the Pop-72 database. The codes are not IBGE codes and no official crosswalk to "
        "present-day municipalities is published with the source.",
        es="Municipios del Imperio del Brasil censados en 1872, con los códigos usados en la "
        "base Pop-72. Los códigos no corresponden a los del IBGE y la fuente no publica "
        "una tabla de equivalencias con los municipios actuales.",
    ),
    "paroquia": dict(
        cols=[
            "id_provincia",
            "id_municipio_1872",
            "id_paroquia",
            "nome_paroquia",
        ],
        key=["id_paroquia"],
        pt="Paróquias do Império do Brasil recenseadas em 1872, a unidade territorial mais fina "
        "do recenseamento. Das 1.473 paróquias listadas, 1.440 têm dados nas tabelas.",
        en="Parishes of the Empire of Brazil enumerated in 1872, the finest territorial unit of "
        "the census. Of the 1,473 parishes listed, 1,440 carry data in the tables.",
        es="Parroquias del Imperio del Brasil censadas en 1872, la unidad territorial más fina "
        "del censo. De las 1.473 parroquias listadas, 1.440 tienen datos en las tablas.",
    ),
    "dicionario": dict(
        cols=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
        key=["id_tabela", "nome_coluna", "chave"],
        pt="Dicionário das colunas codificadas do conjunto, traduzindo cada valor de "
        "id_categoria no rótulo da categoria correspondente em cada tabela.",
        en="Dictionary of the coded columns in this dataset, translating each id_categoria "
        "value into the corresponding category label for each table.",
        es="Diccionario de las columnas codificadas del conjunto, que traduce cada valor de "
        "id_categoria en la etiqueta de la categoría correspondiente en cada tabla.",
    ),
}
