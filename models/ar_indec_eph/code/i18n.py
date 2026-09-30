"""Trilingual text for ar_indec_eph: authored fragments and codebook translations.

Two different problems, handled two different ways, following cl_ine_ene:

* Observations are sentences THIS REPO writes -- the note that a value is a
  Ns./Nr. sentinel, that a weight is dimensionless, that a column ran only in
  some waves. They are GENERATED per language from the fragments below.
  Translating an already-assembled Spanish sentence back out would be both
  wasteful and lossy when the sentence was ours to begin with.

* Descriptions come from INDEC -- Stata variable labels and the record layout --
  so they exist only in Spanish and are translated once per distinct string in
  translations.json, keyed by the Spanish.

build_architecture.py raises on a description with no translation rather than
silently writing the Spanish into the English field, which is how a column set
ends up looking trilingual while being anything but.
"""

LANGS = ("pt", "en", "es")

# --- authored observation fragments -----------------------------------------
FRAGMENTS: dict[str, dict[str, str]] = {
    "partition": {
        "pt": "Coluna de particao",
        "en": "Partition column",
        "es": "Columna de particion",
    },
    "weight": {
        "pt": (
            "Ponderador amostral adimensional, por isso nao leva unidade de "
            "medida. Deve ser usado em todo calculo de agregados populacionais"
        ),
        "en": (
            "Dimensionless sampling weight, which is why it carries no "
            "measurement unit. It must be used in any population aggregate"
        ),
        "es": (
            "Ponderador muestral adimensional, por lo que no lleva unidad de "
            "medida. Debe usarse en todo calculo de agregados poblacionales"
        ),
    },
    "no_unit_slug": {
        "pt": (
            "{what}. O vocabulario de unidades de medida do backend nao tem um "
            "slug equivalente, por isso a coluna fica sem unidade"
        ),
        "en": (
            "{what}. The backend's measurement-unit vocabulary has no matching "
            "slug, so the column is left without a unit"
        ),
        "es": (
            "{what}. El vocabulario de unidades de medida del backend no tiene "
            "un slug equivalente, por lo que la columna queda sin unidad"
        ),
    },
    "classifier": {
        "pt": (
            "Codigo que se resolve contra o {classifier}, nao contra a tabela "
            "dicionario deste conjunto"
        ),
        "en": (
            "A code resolved against {classifier}, not against this dataset's "
            "dicionario table"
        ),
        "es": (
            "Codigo que se resuelve contra el {classifier}, no contra la tabla "
            "dicionario de este conjunto"
        ),
    },
    "sentinel_minus_nine": {
        "pt": (
            "O valor -9 indica Ns./Nr. e nao um montante negativo; deve ser "
            "excluido antes de qualquer calculo"
        ),
        "en": (
            "The value -9 means no answer, not a negative amount; it must be "
            "excluded before any calculation"
        ),
        "es": (
            "El valor -9 indica Ns./Nr. y no un monto negativo; debe excluirse "
            "antes de cualquier calculo"
        ),
    },
    "age_sentinels": {
        "pt": (
            "O valor -1 identifica as pessoas menores de um ano; o 99 "
            "corresponde a Ns./Nr."
        ),
        "en": (
            "The value -1 marks people under one year old; 99 means no answer"
        ),
        "es": (
            "El valor -1 identifica a las personas menores de un anio; el 99 "
            "corresponde a Ns./Nr."
        ),
    },
    "codusu": {
        "pt": (
            "Identificador de domicilio. Muda de formato em 2016: ate 2015 T2 e "
            "um numero de 6 digitos e desde 2016 T2 uma cadeia alfanumerica de "
            "29 caracteres, por isso nao permite seguir um domicilio atraves "
            "desse corte"
        ),
        "en": (
            "Dwelling identifier. It changes format in 2016: up to 2015 Q2 it "
            "is a 6-digit number and from 2016 Q2 a 29-character alphanumeric "
            "string, so it cannot follow a dwelling across that break"
        ),
        "es": (
            "Identificador de vivienda. Cambia de formato en 2016: hasta 2015 "
            "Q2 es un numero de 6 digitos y desde 2016 Q2 una cadena "
            "alfanumerica de 29 caracteres, por lo que no permite seguir una "
            "vivienda a traves de ese corte"
        ),
    },
    "mas_500": {
        "pt": (
            "A fonte usa 'S' e, conforme a onda, 'N' ou 'NO' para o mesmo valor "
            "negativo; os codigos originais sao preservados"
        ),
        "en": (
            "The source uses 'S' and, depending on the wave, either 'N' or 'NO' "
            "for the same negative value; the original codes are preserved"
        ),
        "es": (
            "La fuente usa 'S' y, segun la onda, 'N' o 'NO' para el mismo valor "
            "negativo; se preservan los codigos originales"
        ),
    },
    "ch05_date": {
        "pt": (
            "Data de nascimento no formato DD/MM/AAAA tal como a fonte publica; "
            "preservada como cadeia para nao perder os valores que nao sao "
            "datas validas"
        ),
        "en": (
            "Date of birth in DD/MM/YYYY form as the source publishes it; kept "
            "as a string so the values that are not valid dates are not lost"
        ),
        "es": (
            "Fecha de nacimiento en formato DD/MM/AAAA tal como la publica la "
            "fuente; se preserva como cadena para no perder los valores que no "
            "son fechas validas"
        ),
    },
    "decile": {
        "pt": (
            "Numero de grupo decilico, nao uma quantidade: preservado como "
            "cadeia com o zero a esquerda tal como a fonte publica. Observam-se "
            "valores fora do intervalo 1 a 10 (0 e 12); seu tratamento e "
            "descrito no Anexo I do desenho de registros do INDEC"
        ),
        "en": (
            "A decile group number, not a quantity: kept as a string with its "
            "leading zero as the source publishes it. Values outside 1 to 10 "
            "occur (0 and 12); INDEC describes their treatment in Annex I of "
            "the record layout"
        ),
        "es": (
            "Numero de grupo decilico, no una cantidad: se preserva como cadena "
            "con el cero a la izquierda tal como lo publica la fuente. Se "
            "observan valores fuera del rango 1 a 10 (0 y 12); su tratamiento "
            "se describe en el Anexo I del diseno de registros del INDEC"
        ),
    },
    "geo_code_encoding": {
        "pt": (
            "Atencao: a codificacao muda entre ondas. As tres ondas de 2016 "
            "(2016 T2 a 2016 T4) usam abreviaturas de tres letras, com "
            "maiusculas e minusculas misturadas, enquanto o resto da serie usa "
            "os codigos numericos de provincia e pais do INDEC. Os valores "
            "originais sao preservados, portanto comparar essas tres ondas com "
            "o resto exige uma tabela de equivalencias"
        ),
        "en": (
            "Note that the encoding changes across waves. The three 2016 waves "
            "(2016 Q2 to 2016 Q4) use three-letter abbreviations in mixed case, "
            "while the rest of the series uses INDEC's numeric province and "
            "country codes. The original values are preserved, so comparing "
            "those three waves with the rest requires a crosswalk"
        ),
        "es": (
            "Atencion: la codificacion cambia entre ondas. Las tres ondas de "
            "2016 (2016 Q2 a 2016 Q4) usan abreviaturas de tres letras, con "
            "mayusculas y minusculas mezcladas, mientras que el resto de la "
            "serie usa los codigos numericos de provincia y pais de INDEC. Se "
            "preservan los valores originales, por lo que comparar esas tres "
            "ondas con el resto exige una tabla de equivalencias"
        ),
    },
    "partial_waves": {
        "pt": "Presente em {n} de {total} ondas ({first} a {last})",
        "en": "Present in {n} of {total} waves ({first} to {last})",
        "es": "Presente en {n} de {total} ondas ({first} a {last})",
    },
    "from_pdf": {
        "pt": (
            "Descricao tomada do desenho de registros do INDEC; as bases TXT "
            "nao trazem etiquetas de variavel"
        ),
        "en": (
            "Description taken from INDEC's record layout; the TXT releases "
            "carry no variable labels"
        ),
        "es": (
            "Descripcion tomada del diseno de registros de INDEC; las bases TXT "
            "no traen etiquetas de variable"
        ),
    },
    "source_name": {
        "pt": "Nome na fonte: {name}",
        "en": "Name in the source: {name}",
        "es": "Nombre en la fuente: {name}",
    },
}

# Values substituted into the fragments above, per language.
VALUES: dict[str, dict[str, str]] = {
    "rooms": {
        "pt": "Quantidade de ambientes/comodos",
        "en": "Number of rooms",
        "es": "Cantidad de ambientes/habitaciones",
    },
    "occupations": {
        "pt": "Quantidade de ocupacoes",
        "en": "Number of occupations",
        "es": "Cantidad de ocupaciones",
    },
    "cno": {
        "pt": "Classificador Nacional de Ocupacoes (CNO) do INDEC",
        "en": "INDEC's Clasificador Nacional de Ocupaciones (CNO)",
        "es": "Clasificador Nacional de Ocupaciones (CNO) de INDEC",
    },
    "caes": {
        "pt": "classificador de atividade (CAES/CLANAE) do INDEC",
        "en": "INDEC's activity classifier (CAES/CLANAE)",
        "es": "clasificador de actividad (CAES/CLANAE) de INDEC",
    },
    "caes10": {
        "pt": "classificador de atividade CAES-1.0 do INDEC",
        "en": "INDEC's CAES-1.0 activity classifier",
        "es": "clasificador de actividad CAES-1.0 de INDEC",
    },
    "birthplace": {
        "pt": "codigo de local de nascimento (provincia ou pais) do INDEC",
        "en": "INDEC's place-of-birth code (province or country)",
        "es": "codigo de lugar de nacimiento (provincia o pais) de INDEC",
    },
    "prior_residence": {
        "pt": "codigo de residencia anterior (provincia ou pais) do INDEC",
        "en": "INDEC's previous-residence code (province or country)",
        "es": "codigo de residencia anterior (provincia o pais) de INDEC",
    },
}


def fragment(key: str, lang: str, **kwargs) -> str:
    text = FRAGMENTS[key][lang]
    if kwargs:
        resolved = {
            k: (VALUES[v][lang] if k in ("what", "classifier") else v)
            for k, v in kwargs.items()
        }
        return text.format(**resolved)
    return text
