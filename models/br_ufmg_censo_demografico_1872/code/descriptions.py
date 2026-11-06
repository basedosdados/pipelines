"""Trilingual descriptions for br_ufmg_censo_demografico_1872.

The 72 distinct measure columns come from a closed vocabulary of six slots
(sex, marital status, colour, religion, nationality, condition), so their
descriptions are composed from tokens rather than written out one by one.
That keeps wording identical wherever the same token appears and makes a
vocabulary change a one-line edit.

Colour terms are the census's own (branco, pardo, preto, caboclo) and are kept
untranslated in English and Spanish, with a gloss, because they are technical
categories of the 1872 instrument rather than modern racial categories.
"""

from __future__ import annotations

# token -> (pt singular-plural as written, en, es)
_SEX = {
    "homens": ("homens", "men", "hombres"),
    "mulheres": ("mulheres", "women", "mujeres"),
}
_MARITAL = {
    "solteiros": ("solteiros", "single", "solteros"),
    "solteiras": ("solteiras", "single", "solteras"),
    "casados": ("casados", "married", "casados"),
    "casadas": ("casadas", "married", "casadas"),
    "viuvos": ("viúvos", "widowed", "viudos"),
    "viuvas": ("viúvas", "widowed", "viudas"),
}
_COLOUR = {
    "brancos": ("brancos", "branco (white)", "branco (blanco)"),
    "brancas": ("brancas", "branco (white)", "branco (blanca)"),
    "pardos": ("pardos", "pardo (mixed-race)", "pardo (mestizo)"),
    "pardas": ("pardas", "pardo (mixed-race)", "pardo (mestiza)"),
    "pretos": ("pretos", "preto (Black)", "preto (negro)"),
    "pretas": ("pretas", "preto (Black)", "preto (negra)"),
    "caboclos": (
        "caboclos",
        "caboclo (of Indigenous descent)",
        "caboclo (de ascendencia indígena)",
    ),
    "caboclas": (
        "caboclas",
        "caboclo (of Indigenous descent)",
        "caboclo (de ascendencia indígena)",
    ),
}
_RELIGION = {
    "catolicos": ("católicos", "Catholic", "católicos"),
    "catolicas": ("católicas", "Catholic", "católicas"),
    "acatolicos": ("acatólicos", "non-Catholic", "no católicos"),
    "acatolicas": ("acatólicas", "non-Catholic", "no católicas"),
}
_NATIONALITY = {
    "brasileiros": ("brasileiros", "Brazilian", "brasileños"),
    "brasileiras": ("brasileiras", "Brazilian", "brasileñas"),
    "estrangeiros": ("estrangeiros", "foreign", "extranjeros"),
    "estrangeiras": ("estrangeiras", "foreign", "extranjeras"),
}
_CONDITION = {
    "livres": ("livres", "free", "libres"),
    "escravizados": ("escravizados", "enslaved", "esclavizados"),
    "escravizadas": ("escravizadas", "enslaved", "esclavizadas"),
}

_SLOTS = [_MARITAL, _COLOUR, _RELIGION, _NATIONALITY, _CONDITION]

# Columns that are not compositional.
_FIXED: dict[str, tuple[str, str, str]] = {
    "casas_habitadas": (
        "Número de casas habitadas",
        "Number of inhabited houses",
        "Número de casas habitadas",
    ),
    "casas_desabitadas": (
        "Número de casas desabitadas",
        "Number of uninhabited houses",
        "Número de casas deshabitadas",
    ),
    "fogos": (
        "Número de fogos, a unidade domiciliar do recenseamento de 1872",
        "Number of fogos, the 1872 census household unit",
        "Número de fogos, la unidad doméstica del censo de 1872",
    ),
    "total": ("Total de pessoas", "Total persons", "Total de personas"),
    "total_livres": (
        "Total de pessoas livres",
        "Total free persons",
        "Total de personas libres",
    ),
    "total_escravizados": (
        "Total de pessoas escravizadas",
        "Total enslaved persons",
        "Total de personas esclavizadas",
    ),
    "livres_sem_informacao": (
        "Pessoas livres sem informação de estado civil ou cor",
        "Free persons with no marital status or colour reported",
        "Personas libres sin información de estado civil o color",
    ),
    "escravizados_sem_informacao": (
        "Homens escravizados sem informação de estado civil ou cor",
        "Enslaved men with no marital status or colour reported",
        "Hombres esclavizados sin información de estado civil o color",
    ),
    "escravizadas_sem_informacao": (
        "Mulheres escravizadas sem informação de estado civil ou cor",
        "Enslaved women with no marital status or colour reported",
        "Mujeres esclavizadas sin información de estado civil o color",
    ),
}

# `livres_sem_informacao` appears in both origin tables and takes the gender of
# the table, so it is resolved against `implied_sex` rather than fixed here.
_LIVRES_SEM_INFO = {
    "homens": (
        "Homens livres sem informação de estado civil ou cor",
        "Free men with no marital status or colour reported",
        "Hombres libres sin información de estado civil o color",
    ),
    "mulheres": (
        "Mulheres livres sem informação de estado civil ou cor",
        "Free women with no marital status or colour reported",
        "Mujeres libres sin información de estado civil o color",
    ),
}

# Geography and key columns.
KEY_COLUMNS: dict[str, tuple[str, str, str]] = {
    "ano": (
        "Ano de referência do recenseamento",
        "Reference year of the census",
        "Año de referencia del censo",
    ),
    "id_provincia": (
        "Código da província no banco Pop-72, de 1 a 21",
        "Province code in the Pop-72 database, 1 to 21",
        "Código de la provincia en la base Pop-72, de 1 a 21",
    ),
    "id_municipio_1872": (
        "Código do município de 1872 no banco Pop-72, com 3 dígitos, "
        "cujos dois primeiros são o código da província. Não corresponde ao código do IBGE",
        "1872 municipality code in the Pop-72 database, 3 digits, the first two being the "
        "province code. It is not the IBGE municipality code",
        "Código del municipio de 1872 en la base Pop-72, de 3 dígitos, cuyos dos primeros "
        "son el código de la provincia. No corresponde al código del IBGE",
    ),
    "id_paroquia": (
        "Código da paróquia no banco Pop-72, com 5 dígitos, "
        "cujos três primeiros são o código do município de 1872",
        "Parish code in the Pop-72 database, 5 digits, the first three being the 1872 "
        "municipality code",
        "Código de la parroquia en la base Pop-72, de 5 dígitos, cuyos tres primeros son "
        "el código del municipio de 1872",
    ),
    "id_categoria": (
        "Código da categoria de desagregação da tabela, decodificado em dicionario",
        "Code of the table's disaggregation category, decoded in dicionario",
        "Código de la categoría de desagregación de la tabla, decodificado en dicionario",
    ),
    "nome_provincia": (
        "Nome da província conforme grafia de 1872",
        "Province name in its 1872 spelling",
        "Nombre de la provincia según la grafía de 1872",
    ),
    "nome_municipio": (
        "Nome do município conforme grafia de 1872",
        "Municipality name in its 1872 spelling",
        "Nombre del municipio según la grafía de 1872",
    ),
    "nome_paroquia": (
        "Nome da paróquia conforme grafia de 1872",
        "Parish name in its 1872 spelling",
        "Nombre de la parroquia según la grafía de 1872",
    ),
    "id_tabela": (
        "Nome da tabela à qual a chave se aplica",
        "Name of the table the key applies to",
        "Nombre de la tabla a la que se aplica la clave",
    ),
    "nome_coluna": (
        "Nome da coluna à qual a chave se aplica",
        "Name of the column the key applies to",
        "Nombre de la columna a la que se aplica la clave",
    ),
    "chave": (
        "Chave, o valor armazenado na coluna codificada",
        "Key, the value stored in the coded column",
        "Clave, el valor almacenado en la columna codificada",
    ),
    "cobertura_temporal": (
        "Cobertura temporal da chave",
        "Temporal coverage of the key",
        "Cobertura temporal de la clave",
    ),
    "valor": (
        "Valor por extenso correspondente à chave, prefixado pelo grupo da categoria "
        "quando o rótulo é ambíguo entre grupos",
        "Full label corresponding to the key, prefixed by the category group where the "
        "label is ambiguous across groups",
        "Valor completo correspondiente a la clave, precedido por el grupo de la categoría "
        "cuando la etiqueta es ambigua entre grupos",
    ),
}


def _tokens(
    column: str,
) -> tuple[str | None, list[tuple[str, str, str]], bool]:
    """Split a measure column into (sex, ordered modifiers, sem_informacao)."""
    rest = column
    sem_inf = rest.endswith("sem_informacao")
    if sem_inf:
        rest = rest[: -len("_sem_informacao")]

    parts = rest.split("_")
    sex = parts[0] if parts and parts[0] in _SEX else None
    if sex:
        parts = parts[1:]

    mods = []
    for p in parts:
        for slot in _SLOTS:
            if p in slot:
                mods.append(slot[p])
                break
    return sex, mods, sem_inf


def measure_description(
    column: str, implied_sex: str | None = None
) -> tuple[str, str, str]:
    """Trilingual description for a measure column.

    ``implied_sex`` carries the sex for tables where it is a property of the
    table rather than of the column (the men's and women's origin tables).
    """
    if column == "livres_sem_informacao":
        if implied_sex is None:
            raise ValueError("livres_sem_informacao needs an implied sex")
        return _LIVRES_SEM_INFO[implied_sex]
    if column in _FIXED:
        return _FIXED[column]

    sex, mods, sem_inf = _tokens(column)
    sex_key = sex or implied_sex
    if sex_key is None:
        raise ValueError(f"cannot determine subject for column {column!r}")

    pt_sex, en_sex, es_sex = _SEX[sex_key]

    if sem_inf and not mods:
        return (
            f"Número de {pt_sex} sem informação de estado civil",
            f"Number of {en_sex} with no marital status reported",
            f"Número de {es_sex} sin información de estado civil",
        )

    pt = f"Número de {pt_sex} " + " ".join(m[0] for m in mods)
    en = "Number of " + " ".join(m[1] for m in mods) + f" {en_sex}"
    es = f"Número de {es_sex} " + " ".join(m[2] for m in mods)

    if sem_inf:
        pt += ", sem informação de estado civil"
        en += " with no marital status reported"
        es += ", sin información de estado civil"

    return pt.strip(), " ".join(en.split()), es.strip()
