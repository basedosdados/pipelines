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
    "original_name",
    "description_en",
    "description_es",
]

# name: (type, pt, en, es, dict, directory, unit, observations, original_name)
C = {}


def col(
    name, bq, pt, en, es, dic="no", directory="", unit="", obs="", original=""
):
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
    obs="Até a edição de 2007 as cidades independentes recebiam o sufixo ' City' e eram "
    "listadas em ordem alfabética junto aos condados; a partir de 2008 o sufixo é "
    "omitido e elas aparecem em bloco separado.",
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
    unit="persons",
    obs="Valores de condado são arredondados pela fonte. Nulo quando suprimido ou "
    "indisponível; veja beneficiary_count_note para o motivo. Nunca preenchido com zero.",
    original="persons_*",
)

col(
    "recipient_count",
    "INT64",
    "Número de recebedores do SSI",
    "Number of SSI recipients",
    "Número de beneficiarios del SSI",
    unit="persons",
    obs="Nulo quando suprimido ou indisponível; veja recipient_count_note para o motivo. "
    "Nunca preenchido com zero.",
    original="persons_*",
)

col(
    "benefit_amount_month",
    "INT64",
    "Valor total dos benefícios do OASDI pagos no mês de dezembro, em milhares de dólares",
    "Total OASDI benefits paid in the month of December, in thousands of dollars",
    "Monto total de los beneficios del OASDI pagados en el mes de diciembre, en miles de dólares",
    unit="thousand_dollars",
    obs="A fonte publica o valor em milhares de dólares ('in thousands of dollars' no "
    "cabeçalho da tabela anual), apesar de o metadado JSON registrar a unidade como "
    "'dollars'. Nulo quando suprimido ou indisponível.",
    original="benefits_month_total_*",
)

col(
    "payment_amount_month",
    "INT64",
    "Valor total dos pagamentos do SSI no mês de dezembro, em milhares de dólares",
    "Total SSI payments in the month of December, in thousands of dollars",
    "Monto total de los pagos del SSI en el mes de diciembre, en miles de dólares",
    unit="thousand_dollars",
    obs="A fonte publica o valor em milhares de dólares, apesar de o metadado JSON "
    "registrar a unidade como 'dollars'. No nível de condado o valor só existe na "
    "linha de total.",
    original="payments_month_*",
)

col(
    "population",
    "INT64",
    "População residente estimada em 1º de julho do ano de referência",
    "Estimated resident population as of July 1 of the reference year",
    "Población residente estimada al 1 de julio del año de referencia",
    unit="persons",
    obs="Estimativa do Census Bureau, não da SSA.",
    original="persons_us_pop*",
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
            # The county tables carry no directory link on state_id: county_id
            # already resolves the state, and the FK would be redundant.
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
