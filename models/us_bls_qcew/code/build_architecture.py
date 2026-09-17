"""Generate architecture CSVs and multilingual columns_json for us_bls_qcew.

16 data tables ({naics,sic} x {quarterly,annual} x {national,state,county,metro})
plus the standard `dicionario`. NAICS carries the full BLS schema (core measures
+ location quotients `lq_*` + over-the-year changes `oty_*`); SIC carries the
core measures only. Column descriptions are English in the architecture CSV
(US dataset) and PT/EN/ES in columns_json (consumed by bulk_upsert_columns).

Run: uv run python models/us_bls_qcew/code/build_architecture.py
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
JSON = HERE / "columns_json"

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
]

# unit slugs by measure kind
UNIT = {"estab": "establishment", "emp": "worker", "wage": "dollar"}
TYPE = {"estab": "INT64", "emp": "INT64", "wage": "FLOAT64"}


def col(
    name,
    bqtype,
    en,
    pt,
    es,
    dic=False,
    directory="",
    unit="",
    obs="",
    original=None,
):
    """One architecture column as a dict carrying all three languages."""
    return {
        "name": name,
        "type": bqtype,
        "en": en,
        "pt": pt,
        "es": es,
        "dic": dic,
        "directory": directory,
        "unit": unit,
        "obs": obs,
        "original": original if original is not None else name,
    }


# ── key columns ─────────────────────────────────────────────────────────────
YEAR = col(
    "year",
    "INT64",
    "Reference year of the observation",
    "Ano de referência da observação",
    "Año de referencia de la observación",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
    obs="Partition column",
    original="year",
)
QTR = col(
    "qtr",
    "INT64",
    "Reference quarter of the observation, from 1 to 4",
    "Trimestre de referência da observação, de 1 a 4",
    "Trimestre de referencia de la observación, de 1 a 4",
    unit="quarter",
    original="qtr",
)
OWN = col(
    "own_code",
    "STRING",
    "Ownership code identifying the sector (private, federal, state, local government)",
    "Código de propriedade que identifica o setor (privado, governo federal, estadual, local)",
    "Código de propiedad que identifica el sector (privado, gobierno federal, estatal, local)",
    dic=True,
    original="own_code",
)
IND = col(
    "industry_code",
    "STRING",
    "Industry code (NAICS, supersector, or aggregate pseudo-code)",
    "Código de indústria (NAICS, supersetor ou pseudocódigo agregado)",
    "Código de industria (NAICS, supersector o pseudocódigo agregado)",
    dic=True,
    obs="Real NAICS codes align with br_bd_diretorios_us.naics_2022; aggregate and supersector codes are QCEW-specific",
    original="industry_code",
)
IND_SIC = col(
    "industry_code",
    "STRING",
    "SIC industry code, carrying the SIC_ prefix",
    "Código de indústria SIC, com o prefixo SIC_",
    "Código de industria SIC, con el prefijo SIC_",
    dic=True,
    obs="SIC-prefixed code (up to 10 characters)",
    original="industry_code",
)
AGG = col(
    "agglvl_code",
    "STRING",
    "Aggregation-level code indicating the geographic and industry summarization of the row",
    "Código de nível de agregação que indica a sumarização geográfica e de indústria da linha",
    "Código de nivel de agregación que indica la sumarización geográfica e industrial de la fila",
    dic=True,
    original="agglvl_code",
)
SIZE = col(
    "size_code",
    "STRING",
    "Establishment size-class code, 0 for all sizes",
    "Código de classe de tamanho do estabelecimento, 0 para todos os tamanhos",
    "Código de clase de tamaño del establecimiento, 0 para todos los tamaños",
    dic=True,
    original="size_code",
)
DISC = col(
    "disclosure_code",
    "STRING",
    "Disclosure flag: N when the figure is not disclosable, blank otherwise",
    "Indicador de sigilo: N quando o valor não é divulgável, em branco caso contrário",
    "Indicador de divulgación: N cuando la cifra no es divulgable, en blanco en caso contrario",
    original="disclosure_code",
)


def area_col(geo):
    """The area_fips column, treated per geographic level."""
    if geo == "county":
        return col(
            "area_fips",
            "STRING",
            "County FIPS code (5-digit)",
            "Código FIPS do condado (5 dígitos)",
            "Código FIPS del condado (5 dígitos)",
            directory="br_bd_diretorios_us.county:id_county",
            obs="Directory FK pending br_bd_diretorios_us onboarding",
            original="area_fips",
        )
    if geo == "state":
        return col(
            "area_fips",
            "STRING",
            "Statewide area code (SS000)",
            "Código de área estadual (SS000)",
            "Código de área estatal (SS000)",
            obs="Two-digit state FIPS followed by 000; see id_state for the state FK",
            original="area_fips",
        )
    if geo == "national":
        return col(
            "area_fips",
            "STRING",
            "National area code (U.S. total and special U.S.-wide aggregates)",
            "Código de área nacional (total dos EUA e agregados especiais nacionais)",
            "Código de área nacional (total de EE. UU. y agregados especiales nacionales)",
            dic=True,
            original="area_fips",
        )
    return col(  # metro
        "area_fips",
        "STRING",
        "QCEW metropolitan-area code (MSA, CSA, or Micropolitan)",
        "Código de área metropolitana do QCEW (MSA, CSA ou Micropolitana)",
        "Código de área metropolitana del QCEW (MSA, CSA o Micropolitana)",
        dic=True,
        obs="QCEW 4-digit metro codes; a CBSA crosswalk is future work",
        original="area_fips",
    )


ID_STATE = col(
    "id_state",
    "STRING",
    "State FIPS code (2-digit)",
    "Código FIPS do estado (2 dígitos)",
    "Código FIPS del estado (2 dígitos)",
    directory="br_bd_diretorios_us.state:id_state",
    obs="Derived from area_fips (first two digits); directory FK pending br_bd_diretorios_us onboarding",
    original="area_fips",
)


# ── measure specs: (name, kind, en, pt, es, short_en, short_pt, short_es) ────
QUARTERLY_MEASURES = [
    (
        "qtrly_estabs",
        "estab",
        "Count of establishments in the quarter",
        "Número de estabelecimentos no trimestre",
        "Número de establecimientos en el trimestre",
        "the establishment count",
        "a contagem de estabelecimentos",
        "el número de establecimientos",
    ),
    (
        "month1_emplvl",
        "emp",
        "Employment level in the first month of the quarter",
        "Nível de emprego no primeiro mês do trimestre",
        "Nivel de empleo en el primer mes del trimestre",
        "first-month employment",
        "o emprego do primeiro mês",
        "el empleo del primer mes",
    ),
    (
        "month2_emplvl",
        "emp",
        "Employment level in the second month of the quarter",
        "Nível de emprego no segundo mês do trimestre",
        "Nivel de empleo en el segundo mes del trimestre",
        "second-month employment",
        "o emprego do segundo mês",
        "el empleo del segundo mes",
    ),
    (
        "month3_emplvl",
        "emp",
        "Employment level in the third month of the quarter",
        "Nível de emprego no terceiro mês do trimestre",
        "Nivel de empleo en el tercer mes del trimestre",
        "third-month employment",
        "o emprego do terceiro mês",
        "el empleo del tercer mes",
    ),
    (
        "total_qtrly_wages",
        "wage",
        "Total wages paid in the quarter",
        "Total de salários pagos no trimestre",
        "Total de salarios pagados en el trimestre",
        "total quarterly wages",
        "o total de salários do trimestre",
        "el total de salarios del trimestre",
    ),
    (
        "taxable_qtrly_wages",
        "wage",
        "Wages subject to unemployment-insurance taxation in the quarter",
        "Salários sujeitos à tributação do seguro-desemprego no trimestre",
        "Salarios sujetos a la tributación del seguro de desempleo en el trimestre",
        "taxable quarterly wages",
        "os salários tributáveis do trimestre",
        "los salarios gravables del trimestre",
    ),
    (
        "qtrly_contributions",
        "wage",
        "Unemployment-insurance contributions in the quarter",
        "Contribuições ao seguro-desemprego no trimestre",
        "Contribuciones al seguro de desempleo en el trimestre",
        "quarterly contributions",
        "as contribuições do trimestre",
        "las contribuciones del trimestre",
    ),
    (
        "avg_wkly_wage",
        "wage",
        "Average weekly wage in the quarter",
        "Salário médio semanal no trimestre",
        "Salario medio semanal en el trimestre",
        "the average weekly wage",
        "o salário médio semanal",
        "el salario medio semanal",
    ),
]

ANNUAL_MEASURES = [
    (
        "annual_avg_estabs",
        "estab",
        "Annual average of quarterly establishment counts",
        "Média anual das contagens trimestrais de estabelecimentos",
        "Promedio anual de los conteos trimestrales de establecimientos",
        "the annual average establishment count",
        "a contagem média anual de estabelecimentos",
        "el número medio anual de establecimientos",
    ),
    (
        "annual_avg_emplvl",
        "emp",
        "Annual average of monthly employment levels",
        "Média anual dos níveis mensais de emprego",
        "Promedio anual de los niveles mensuales de empleo",
        "annual average employment",
        "o emprego médio anual",
        "el empleo medio anual",
    ),
    (
        "total_annual_wages",
        "wage",
        "Sum of the four quarters' total wages for the year",
        "Soma dos salários totais dos quatro trimestres do ano",
        "Suma de los salarios totales de los cuatro trimestres del año",
        "total annual wages",
        "o total de salários anuais",
        "el total de salarios anuales",
    ),
    (
        "taxable_annual_wages",
        "wage",
        "Sum of the four quarters' taxable wages for the year",
        "Soma dos salários tributáveis dos quatro trimestres do ano",
        "Suma de los salarios gravables de los cuatro trimestres del año",
        "taxable annual wages",
        "os salários tributáveis anuais",
        "los salarios gravables anuales",
    ),
    (
        "annual_contributions",
        "wage",
        "Sum of the four quarters' unemployment-insurance contributions for the year",
        "Soma das contribuições ao seguro-desemprego dos quatro trimestres do ano",
        "Suma de las contribuciones al seguro de desempleo de los cuatro trimestres del año",
        "annual contributions",
        "as contribuições anuais",
        "las contribuciones anuales",
    ),
    (
        "annual_avg_wkly_wage",
        "wage",
        "Average weekly wage based on the year's employment and total wages",
        "Salário médio semanal com base no emprego e nos salários totais do ano",
        "Salario medio semanal según el empleo y los salarios totales del año",
        "the annual average weekly wage",
        "o salário médio semanal anual",
        "el salario medio semanal anual",
    ),
    (
        "avg_annual_pay",
        "wage",
        "Average annual pay based on the year's employment and wage levels",
        "Remuneração média anual com base no emprego e nos salários do ano",
        "Remuneración media anual según el empleo y los salarios del año",
        "average annual pay",
        "a remuneração média anual",
        "la remuneración media anual",
    ),
]


def de_pt(noun):
    """Contract Portuguese 'de' + article: de+a=da, de+o=do, de+os=dos, de+as=das."""
    for art, contr in (
        ("as ", "das "),
        ("os ", "dos "),
        ("a ", "da "),
        ("o ", "do "),
    ):
        if noun.startswith(art):
            return contr + noun[len(art) :]
    return "de " + noun


def de_es(noun):
    """Contract Spanish 'de' + 'el' = 'del'; other articles stay 'de ...'."""
    if noun.startswith("el "):
        return "del " + noun[3:]
    return "de " + noun


def measure_cols(measures):
    """Core measure columns from a measure spec list."""
    out = []
    for name, kind, en, pt, es, *_ in measures:
        out.append(
            col(name, TYPE[kind], en, pt, es, unit=UNIT[kind], original=name)
        )
    return out


def naics_extra_cols(measures):
    """Location-quotient and over-the-year columns (NAICS only)."""
    out = [
        col(
            "lq_disclosure_code",
            "STRING",
            "Disclosure flag for the location quotient: N when not disclosable, blank otherwise",
            "Indicador de sigilo do quociente locacional: N quando não divulgável, em branco caso contrário",
            "Indicador de divulgación del cociente de localización: N cuando no es divulgable, en blanco en caso contrario",
            original="lq_disclosure_code",
        )
    ]
    for name, _kind, _en, _pt, _es, sen, spt, ses in measures:
        out.append(
            col(
                f"lq_{name}",
                "FLOAT64",
                f"Location quotient of {sen} relative to the U.S., rounded to hundredths",
                f"Quociente locacional {de_pt(spt)} em relação aos EUA, arredondado ao centésimo",
                f"Cociente de localización {de_es(ses)} respecto a EE. UU., redondeado a la centésima",
                unit="ratio",
                original=f"lq_{name}",
            )
        )
    out.append(
        col(
            "oty_disclosure_code",
            "STRING",
            "Disclosure flag for the over-the-year change: N when not disclosable, blank otherwise",
            "Indicador de sigilo da variação interanual: N quando não divulgável, em branco caso contrário",
            "Indicador de divulgación de la variación interanual: N cuando no es divulgable, en blanco en caso contrario",
            original="oty_disclosure_code",
        )
    )
    for name, kind, _en, _pt, _es, sen, spt, ses in measures:
        out.append(
            col(
                f"oty_{name}_chg",
                TYPE[kind],
                f"Over-the-year change in {sen}",
                f"Variação interanual {de_pt(spt)}",
                f"Variación interanual {de_es(ses)}",
                unit=UNIT[kind],
                original=f"oty_{name}_chg",
            )
        )
        out.append(
            col(
                f"oty_{name}_pct_chg",
                "FLOAT64",
                f"Over-the-year percent change in {sen}, rounded to tenths",
                f"Variação percentual interanual {de_pt(spt)}, arredondada ao décimo",
                f"Variación porcentual interanual {de_es(ses)}, redondeada a la décima",
                unit="percent",
                original=f"oty_{name}_pct_chg",
            )
        )
    return out


def build_table_cols(classification, freq, geo):
    """Assemble the ordered column list for one data table."""
    cols = [YEAR]
    if freq == "quarterly":
        cols.append(QTR)
    cols.append(area_col(geo))
    if geo == "state":
        cols.append(ID_STATE)
    cols.append(OWN)
    cols.append(IND_SIC if classification == "sic" else IND)
    cols += [AGG, SIZE, DISC]
    measures = QUARTERLY_MEASURES if freq == "quarterly" else ANNUAL_MEASURES
    cols += measure_cols(measures)
    if classification == "naics":
        cols += naics_extra_cols(measures)
    return cols


DICIONARIO = [
    (
        "id_tabela",
        "Slug of the us_bls_qcew table the dictionary entry describes",
        "Slug da tabela us_bls_qcew que a entrada do dicionário descreve",
        "Slug de la tabla us_bls_qcew que describe la entrada del diccionario",
    ),
    (
        "nome_coluna",
        "Name of the column the dictionary entry describes",
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    (
        "chave",
        "Coded value (key) exactly as stored in the data",
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Valor codificado (clave) exactamente como se almacena en los datos",
    ),
    (
        "cobertura_temporal",
        "Temporal coverage of the key",
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    (
        "valor",
        "Human-readable label corresponding to the coded value",
        "Rótulo legível correspondente ao valor codificado",
        "Etiqueta legible correspondiente al valor codificado",
    ),
]


def write_csv(path, cols):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for c in cols:
            w.writerow(
                [
                    c["name"],
                    c["type"],
                    c["en"],
                    "",
                    "yes" if c["dic"] else "no",
                    c["directory"],
                    c["unit"],
                    "no",
                    c["obs"],
                    c["original"],
                ]
            )


def write_json(path, cols):
    payload = []
    for c in cols:
        entry = {
            "name": c["name"],
            "bigquery_type": c["type"],
            "description_pt": c["pt"],
            "description_en": c["en"],
            "description_es": c["es"],
            "covered_by_dictionary": c["dic"],
            "has_sensitive_data": False,
        }
        if c["directory"]:
            entry["directory_column"] = c["directory"]
        if c["unit"]:
            entry["measurement_unit"] = c["unit"]
        payload.append(entry)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def main():
    ARCH.mkdir(parents=True, exist_ok=True)
    JSON.mkdir(parents=True, exist_ok=True)
    tables = {}
    for c in ("naics", "sic"):
        for f in ("quarterly", "annual"):
            for g in ("national", "state", "county", "metro"):
                tables[f"{c}_{f}_{g}"] = build_table_cols(c, f, g)
    dic_cols = [
        col(n, "STRING", en, pt, es, original=n)
        for n, en, pt, es in DICIONARIO
    ]
    tables["dicionario"] = dic_cols
    for slug, cols in tables.items():
        write_csv(ARCH / f"{slug}.csv", cols)
        write_json(JSON / f"{slug}.json", cols)
    print(f"wrote {len(tables)} tables to {ARCH} and {JSON}")
    for slug, cols in tables.items():
        print(f"  {slug}: {len(cols)} cols")


if __name__ == "__main__":
    main()
