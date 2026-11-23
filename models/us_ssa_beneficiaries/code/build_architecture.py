"""Generate the architecture CSVs for us_ssa_beneficiaries.

The architecture table is the source of truth for column names, order, types
and metadata.  Generating it keeps the shared column definitions identical
across the five tables instead of drifting between five hand-edited sheets.
"""

from pathlib import Path

import pandas as pd

OUT = Path(__file__).resolve().parent / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "observations_en",
    "observations_es",
    "original_name",
    "description_en",
    "description_es",
]

# name: (type, pt, en, es, dict, directory, unit, observations, original_name)
C = {}


def col(
    name,
    bq,
    pt,
    en,
    es,
    dic="no",
    directory="",
    unit="",
    obs="",
    original="",
    obs_en="",
    obs_es="",
):
    """Define one column.

    ``obs`` is the Portuguese observations text; ``obs_en`` and ``obs_es`` are
    its translations. Passing only ``obs`` would put Portuguese text in the
    English and Spanish fields, which is how thousands of production columns
    ended up nominally trilingual and actually Portuguese.
    """
    C[name] = dict(
        name=name,
        bigquery_type=bq,
        description=pt,
        temporal_coverage="",
        covered_by_dictionary=dic,
        directory_column=directory,
        measurement_unit=unit,
        has_sensitive_data="no",
        observations=obs,
        observations_en=obs_en or obs,
        observations_es=obs_es or obs,
        original_name=original,
        description_en=en,
        description_es=es,
    )


col(
    "year",
    "INT64",
    "Ano de referência, correspondente à edição anual de dezembro",
    "Reference year, corresponding to the annual December edition",
    "Año de referencia, correspondiente a la edición anual de diciembre",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
    obs="As estatísticas são um retrato do mês de dezembro do ano de referência.",
    original="month",
    obs_en="The statistics are a snapshot of December of the reference year.",
    obs_es="Las estadísticas son una instantánea de diciembre del año de referencia.",
)

col(
    "state_id",
    "STRING",
    "Código ANSI (FIPS) de dois dígitos do estado",
    "Two-digit state ANSI (FIPS) code",
    "Código ANSI (FIPS) de dos dígitos del estado",
    directory="br_bd_diretorios_us.state:id_state",
    obs="Nulo nas linhas que não correspondem a um estado: 'All areas' (total nacional), "
    "'Other', 'Foreign countries' e 'Unknown'.",
    original="ansi",
    obs_en="Null on rows that do not correspond to a state: 'All areas' (the national total), 'Other', 'Foreign countries' and 'Unknown'.",
    obs_es="Nulo en las filas que no corresponden a un estado: 'All areas' (el total nacional), 'Other', 'Foreign countries' y 'Unknown'.",
)

col(
    "county_id",
    "STRING",
    "Código ANSI (FIPS) de cinco dígitos do condado ou cidade independente",
    "Five-digit county or independent city ANSI (FIPS) code",
    "Código ANSI (FIPS) de cinco dígitos del condado o ciudad independiente",
    directory="br_bd_diretorios_us.county:id_county",
    obs="A SSA só passou a publicar o código ANSI na edição de 2008 (OASDI) ou 2009 (SSI); "
    "para os anos anteriores o código foi reconstruído a partir do próprio arquivo da "
    "SSA, cruzando estado e nome do condado com os anos já codificados. Permanece nulo "
    "para as linhas 'Unknown' e para entidades extintas antes da codificação (áreas "
    "censitárias do Alasca, cidades independentes da Virgínia que voltaram a ser "
    "municípios).",
    original="ansi",
    obs_en="SSA only began publishing the ANSI code in the 2008 (OASDI) or 2009 (SSI) edition; for earlier years the code was reconstructed from SSA's own file, matching state and county name against the years that are already coded. It stays null for the 'Unknown' rows and for entities abolished before coding began (Alaska census areas, Virginia independent cities that reverted to towns).",
    obs_es="La SSA solo comenzó a publicar el código ANSI en la edición de 2008 (OASDI) o 2009 (SSI); para los años anteriores el código se reconstruyó a partir del propio archivo de la SSA, cruzando estado y nombre del condado con los años ya codificados. Permanece nulo para las filas 'Unknown' y para entidades abolidas antes de la codificación (áreas censales de Alaska, ciudades independientes de Virginia que volvieron a ser municipios).",
)

col(
    "state_name",
    "STRING",
    "Nome do estado ou área",
    "Name of the state or area",
    "Nombre del estado o área",
    original="state_or_area",
)

col(
    "county_name",
    "STRING",
    "Nome do condado ou cidade independente, conforme publicado pela SSA",
    "Name of the county or independent city, as published by SSA",
    "Nombre del condado o ciudad independiente, según lo publicado por la SSA",
    obs="Até a edição de 2004 as cidades independentes recebiam o sufixo ' City' e "
    "eram listadas em ordem alfabética junto aos condados; a partir de 2005 o sufixo "
    "é omitido e elas aparecem em bloco separado, depois dos condados. O código ANSI "
    "só passa a acompanhá-las na edição de 2008.",
    obs_en="Up to the 2004 edition independent cities carried the ' City' suffix and "
    "were listed alphabetically among the counties; from 2005 the suffix is dropped "
    "and they appear in a separate block after the counties. The ANSI code only "
    "accompanies them from the 2008 edition.",
    obs_es="Hasta la edición de 2004 las ciudades independientes llevaban el sufijo "
    "' City' y se listaban alfabéticamente junto a los condados; a partir de 2005 el "
    "sufijo se omite y aparecen en un bloque separado, después de los condados. El "
    "código ANSI solo las acompaña desde la edición de 2008.",
    original="county_or_city",
)

col(
    "state_or_area",
    "STRING",
    "Estado, território ou categoria agregada",
    "State, territory or aggregate category",
    "Estado, territorio o categoría agregada",
    obs="Inclui o total nacional ('All areas'), os territórios e as categorias residuais "
    "'Other', 'Foreign countries' e 'Unknown'. Filtre por state_id não nulo para obter "
    "apenas estados e territórios.",
    original="state_or_area",
    obs_en="Includes the national total ('All areas'), the territories and the residual categories 'Other', 'Foreign countries' and 'Unknown'. Filter on a non-null state_id for states and territories only.",
    obs_es="Incluye el total nacional ('All areas'), los territorios y las categorías residuales 'Other', 'Foreign countries' y 'Unknown'. Filtre por state_id no nulo para obtener solo estados y territorios.",
)

col(
    "benefit_type",
    "STRING",
    "Tipo de benefício do OASDI",
    "Type of OASDI benefit",
    "Tipo de beneficio del OASDI",
    dic="yes",
    obs="Os nove tipos de benefício somam o valor de 'total'. As linhas com sexo "
    "('men', 'women') são um recorte do subconjunto de 65 anos ou mais e não devem ser "
    "somadas aos tipos de benefício.",
    original="measure",
    obs_en="The nine benefit types sum to the value of 'total'. The rows with a sex ('men', 'women') are a cut of the 65-or-older subset and must not be added to the benefit types.",
    obs_es="Los nueve tipos de beneficio suman el valor de 'total'. Las filas con sexo ('men', 'women') son un recorte del subconjunto de 65 años o más y no deben sumarse a los tipos de beneficio.",
)

col(
    "eligibility_category",
    "STRING",
    "Categoria de elegibilidade ao SSI",
    "SSI eligibility category",
    "Categoría de elegibilidad al SSI",
    dic="yes",
    obs="'aged' e 'blind_or_disabled' somam o valor de 'total'.",
    original="measure",
    obs_en="'aged' and 'blind_or_disabled' sum to the value of 'total'.",
    obs_es="'aged' y 'blind_or_disabled' suman el valor de 'total'.",
)

col(
    "age_group",
    "STRING",
    "Faixa etária do beneficiário",
    "Age group of the beneficiary",
    "Grupo de edad del beneficiario",
    dic="yes",
    obs="É um recorte alternativo do total, não um subconjunto do tipo de benefício ou da "
    "categoria de elegibilidade.",
    original="measure",
    obs_en="It is an alternative cut of the total, not a subset of the benefit type or the eligibility category.",
    obs_es="Es un recorte alternativo del total, no un subconjunto del tipo de beneficio o de la categoría de elegibilidad.",
)

col(
    "sex",
    "STRING",
    "Sexo do beneficiário, disponível apenas para a faixa de 65 anos ou mais",
    "Sex of the beneficiary, available only for the 65-or-older group",
    "Sexo del beneficiario, disponible solo para el grupo de 65 años o más",
    dic="yes",
    original="measure",
)

col(
    "oasdi_concurrent",
    "STRING",
    "Indica se o recorte é o dos recebedores de SSI que também recebem OASDI",
    "Whether the slice is of SSI recipients who also receive OASDI",
    "Indica si el recorte es el de los beneficiarios de SSI que también reciben OASDI",
    dic="yes",
    original="persons_concurrent",
)

col(
    "population_group",
    "STRING",
    "Grupo populacional de referência",
    "Reference population group",
    "Grupo poblacional de referencia",
    dic="yes",
    original="measure",
)

col(
    "beneficiary_count",
    "INT64",
    "Número de beneficiários do OASDI em current-payment status",
    "Number of OASDI beneficiaries in current-payment status",
    "Número de beneficiarios del OASDI en current-payment status",
    unit="person",
    obs="Valores de condado são arredondados pela fonte. Nulo quando suprimido ou "
    "indisponível; veja beneficiary_count_note para o motivo. Nunca preenchido com zero.",
    original="persons_*",
    obs_en="County values are rounded by the source. Null when suppressed or unavailable; see beneficiary_count_note for the reason. Never filled with zero.",
    obs_es="Los valores de condado son redondeados por la fuente. Nulo cuando está suprimido o no disponible; vea beneficiary_count_note para el motivo. Nunca se completa con cero.",
)

col(
    "recipient_count",
    "INT64",
    "Número de recebedores do SSI",
    "Number of SSI recipients",
    "Número de beneficiarios del SSI",
    unit="person",
    obs="Nulo quando suprimido ou indisponível; veja recipient_count_note para o motivo. "
    "Nunca preenchido com zero.",
    original="persons_*",
    obs_en="Null when suppressed or unavailable; see recipient_count_note for the reason. Never filled with zero.",
    obs_es="Nulo cuando está suprimido o no disponible; vea recipient_count_note para el motivo. Nunca se completa con cero.",
)

col(
    "benefit_amount_month",
    "INT64",
    "Valor total dos benefícios do OASDI pagos no mês de dezembro, em dólares",
    "Total OASDI benefits paid in the month of December, in dollars",
    "Monto total de los beneficios del OASDI pagados en el mes de diciembre, en dólares",
    unit="usd",
    obs="A fonte publica o valor em milhares de dólares ('in thousands of dollars' no "
    "cabeçalho da tabela anual, apesar de o metadado JSON registrar a unidade como "
    "'dollars'); aqui ele é convertido para dólares. A SSA já arredondou para o milhar "
    "mais próximo, de modo que todo valor termina em três zeros: a conversão muda a "
    "unidade, não a precisão. Nulo quando suprimido ou indisponível.",
    obs_en="The source publishes the value in thousands of dollars ('in thousands of "
    "dollars' in the annual table header, although the JSON metadata records the unit "
    "as 'dollars'); it is converted to dollars here. SSA has already rounded to the "
    "nearest thousand, so every value ends in three zeros: the conversion changes the "
    "unit, not the precision. Null when suppressed or unavailable.",
    obs_es="La fuente publica el valor en miles de dólares ('in thousands of dollars' "
    "en el encabezado de la tabla anual, aunque el metadato JSON registra la unidad "
    "como 'dollars'); aquí se convierte a dólares. La SSA ya redondeó al millar más "
    "cercano, de modo que todo valor termina en tres ceros: la conversión cambia la "
    "unidad, no la precisión. Nulo cuando está suprimido o no disponible.",
    original="benefits_month_total_*",
)

col(
    "payment_amount_month",
    "INT64",
    "Valor total dos pagamentos do SSI no mês de dezembro, em dólares",
    "Total SSI payments in the month of December, in dollars",
    "Monto total de los pagos del SSI en el mes de diciembre, en dólares",
    unit="usd",
    obs="A fonte publica o valor em milhares de dólares, apesar de o metadado JSON "
    "registrar a unidade como 'dollars'; aqui ele é convertido para dólares. A SSA já "
    "arredondou para o milhar mais próximo, de modo que todo valor termina em três "
    "zeros. No nível de condado o valor só existe na linha de total.",
    obs_en="The source publishes the value in thousands of dollars, although the JSON "
    "metadata records the unit as 'dollars'; it is converted to dollars here. SSA has "
    "already rounded to the nearest thousand, so every value ends in three zeros. At "
    "county level the value exists only on the total row.",
    obs_es="La fuente publica el valor en miles de dólares, aunque el metadato JSON "
    "registra la unidad como 'dollars'; aquí se convierte a dólares. La SSA ya redondeó "
    "al millar más cercano, de modo que todo valor termina en tres ceros. A nivel de "
    "condado el valor solo existe en la fila de total.",
    original="payments_month_*",
)

col(
    "population",
    "INT64",
    "População residente estimada em 1º de julho do ano de referência",
    "Estimated resident population as of July 1 of the reference year",
    "Población residente estimada al 1 de julio del año de referencia",
    unit="person",
    obs="Estimativa do Census Bureau, não da SSA.",
    original="persons_us_pop*",
    obs_en="A Census Bureau estimate, not an SSA one.",
    obs_es="Una estimación del Census Bureau, no de la SSA.",
)

col(
    "percentage_receiving_oasdi",
    "FLOAT64",
    "Percentual do grupo populacional que recebe benefícios do OASDI",
    "Percentage of the population group receiving OASDI benefits",
    "Porcentaje del grupo poblacional que recibe beneficios del OASDI",
    unit="percent",
    obs="Não pode ser recalculado a partir dos valores estaduais.",
    original="percent_us_pop*_oasdi",
    obs_en="It cannot be recomputed from the state values.",
    obs_es="No puede recalcularse a partir de los valores estatales.",
)

for base in [
    "beneficiary_count",
    "recipient_count",
    "benefit_amount_month",
    "payment_amount_month",
    "population",
    "percentage_receiving_oasdi",
]:
    col(
        f"{base}_note",
        "STRING",
        f"Motivo pelo qual {base} está nulo",
        f"Reason why {base} is null",
        f"Motivo por el cual {base} es nulo",
        dic="yes",
        obs="Nulo quando o valor está presente.",
        obs_en="Null when the value is present.",
        obs_es="Nulo cuando el valor está presente.",
        original="",
    )

TABLES = {
    "oasdi_county": [
        "year",
        "state_id",
        "county_id",
        "state_name",
        "county_name",
        "benefit_type",
        "age_group",
        "sex",
        "beneficiary_count",
        "beneficiary_count_note",
        "benefit_amount_month",
        "benefit_amount_month_note",
    ],
    "oasdi_state": [
        "year",
        "state_id",
        "state_or_area",
        "benefit_type",
        "age_group",
        "sex",
        "beneficiary_count",
        "beneficiary_count_note",
        "benefit_amount_month",
        "benefit_amount_month_note",
    ],
    "oasdi_population_share": [
        "year",
        "state_id",
        "state_or_area",
        "population_group",
        "population",
        "population_note",
        "percentage_receiving_oasdi",
        "percentage_receiving_oasdi_note",
    ],
    "ssi_county": [
        "year",
        "state_id",
        "county_id",
        "state_name",
        "county_name",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
        "recipient_count",
        "recipient_count_note",
        "payment_amount_month",
        "payment_amount_month_note",
    ],
    "ssi_state": [
        "year",
        "state_id",
        "state_or_area",
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
        "recipient_count",
        "recipient_count_note",
        "payment_amount_month",
        "payment_amount_month_note",
    ],
}

DICIONARIO = [
    (
        "id_tabela",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
    ),
    (
        "nome_coluna",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    (
        "chave",
        "Valor da chave armazenado na coluna",
        "Key value stored in the column",
        "Valor de la clave almacenado en la columna",
    ),
    (
        "cobertura_temporal",
        "Cobertura temporal da chave",
        "Temporal coverage of the key",
        "Cobertura temporal de la clave",
    ),
    (
        "valor",
        "Descrição da chave em português",
        "Description of the key in Portuguese",
        "Descripción de la clave en portugués",
    ),
    (
        "valor_en",
        "Descrição da chave em inglês",
        "Description of the key in English",
        "Descripción de la clave en inglés",
    ),
    (
        "valor_es",
        "Descrição da chave em espanhol",
        "Description of the key in Spanish",
        "Descripción de la clave en español",
    ),
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        rows = []
        for name in columns:
            row = dict(C[name])
            rows.append(row)
        pd.DataFrame(rows)[HEADER].to_csv(OUT / f"{table}.csv", index=False)
        print(f"  {table:26s} {len(rows):2d} columns")
    rows = [
        dict(
            name=n,
            bigquery_type="STRING",
            description=pt,
            temporal_coverage="",
            covered_by_dictionary="no",
            directory_column="",
            measurement_unit="",
            has_sensitive_data="no",
            observations="",
            observations_en="",
            observations_es="",
            original_name="",
            description_en=en,
            description_es=es,
        )
        for n, pt, en, es in DICIONARIO
    ]
    pd.DataFrame(rows)[HEADER].to_csv(OUT / "dicionario.csv", index=False)
    print(f"  {'dicionario':26s} {len(rows):2d} columns")


if __name__ == "__main__":
    main()
