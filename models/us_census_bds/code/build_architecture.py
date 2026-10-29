"""Write the us_census_bds architecture CSVs and the dictionary rows.

The architecture CSVs are the single source of truth for column names, types
and descriptions: the cleaning step reads its column order from them, and the
dbt models and metadata registration are generated from them.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.build_architecture
"""

from __future__ import annotations

import csv
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"
FIELDS = [
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
    "description_pt",
    "description_es",
    "observations_pt",
    "observations_es",
]

AGE_OBS = (
    "Fine and coarse classes share this column; the breakdown column says "
    "which applies. Values keep the source's letter prefix, so they sort in "
    "class order. Left Censored marks units born before 1977, whose age is "
    "unknown",
    "Classes finas e grossas compartilham esta coluna; a coluna breakdown "
    "indica qual se aplica. Os valores mantêm o prefixo de letra da fonte, "
    "de modo que ordenam na ordem das classes. Left Censored marca unidades "
    "nascidas antes de 1977, cuja idade é desconhecida",
    "Las clases finas y gruesas comparten esta columna; la columna breakdown "
    "indica cuál se aplica. Los valores mantienen el prefijo de letra de la "
    "fuente, de modo que se ordenan en el orden de las clases. Left Censored "
    "marca unidades nacidas antes de 1977, cuya edad es desconocida",
)
SIZE_OBS = (
    "Fine and coarse classes share this column; the breakdown column says "
    "which applies. Values keep the source's letter prefix, so they sort in "
    "class order. Size is in employees",
    "Classes finas e grossas compartilham esta coluna; a coluna breakdown "
    "indica qual se aplica. Os valores mantêm o prefixo de letra da fonte, "
    "de modo que ordenam na ordem das classes. O tamanho é medido em "
    "empregados",
    "Las clases finas y gruesas comparten esta columna; la columna breakdown "
    "indica cuál se aplica. Los valores mantienen el prefijo de letra de la "
    "fuente, de modo que se ordenan en el orden de las clases. El tamaño se "
    "mide en empleados",
)
NULL_OBS = (
    "NULL where the source suppresses the cell; suppression_flags records why",
    "NULL quando a fonte suprime a célula; suppression_flags registra o motivo",
    "NULL cuando la fuente suprime la celda; suppression_flags registra el "
    "motivo",
)


def col(
    name: str,
    bq_type: str,
    desc: tuple[str, str, str],
    *,
    dictionary: str = "no",
    directory: str = "",
    unit: str = "",
    obs: tuple[str, str, str] = ("", "", ""),
    original: str = "",
) -> dict[str, str]:
    """Build one architecture row from English, Portuguese and Spanish text."""
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": desc[0],
        "temporal_coverage": "",
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs[0],
        "original_name": original or name,
        "description_pt": desc[1],
        "description_es": desc[2],
        "observations_pt": obs[1],
        "observations_es": obs[2],
    }


YEAR = col(
    "year",
    "INT64",
    (
        "Reference year; flows measure change from March of the prior year",
        "Ano de referência; os fluxos medem a variação desde março do ano "
        "anterior",
        "Año de referencia; los flujos miden la variación desde marzo del año "
        "anterior",
    ),
    directory="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
)
BREAKDOWN = col(
    "breakdown",
    "STRING",
    (
        "Combination of dimensions the row is tabulated by, as the "
        "comma-separated BDS variable names",
        "Combinação de dimensões pela qual a linha é tabulada, como os nomes "
        "das variáveis do BDS separados por vírgula",
        "Combinación de dimensiones por la que se tabula la fila, como los "
        "nombres de las variables del BDS separados por comas",
    ),
    dictionary="yes",
    obs=(
        "Each breakdown is a complete tabulation of the same universe, so "
        "rows of different breakdowns overlap. Filter on one breakdown "
        "before aggregating, or the same firms are counted several times",
        "Cada breakdown é uma tabulação completa do mesmo universo, então "
        "linhas de breakdowns diferentes se sobrepõem. Filtre um único "
        "breakdown antes de agregar, ou as mesmas firmas serão contadas "
        "várias vezes",
        "Cada breakdown es una tabulación completa del mismo universo, por lo "
        "que las filas de distintos breakdowns se superponen. Filtre un único "
        "breakdown antes de agregar, o las mismas empresas se contarán varias "
        "veces",
    ),
    original="(file name)",
)
STATE = col(
    "state_id",
    "STRING",
    (
        "Two-digit FIPS state code",
        "Código FIPS de dois dígitos do estado",
        "Código FIPS de dos dígitos del estado",
    ),
    directory="br_bd_diretorios_us.state:id_state",
    obs=(
        "The 50 states and the District of Columbia, plus Puerto Rico (72) "
        "from the separate Puerto Rico files",
        "Os 50 estados e o Distrito de Colúmbia, mais Porto Rico (72) a "
        "partir dos arquivos separados de Porto Rico",
        "Los 50 estados y el Distrito de Columbia, más Puerto Rico (72) a "
        "partir de los archivos separados de Puerto Rico",
    ),
    original="st",
)
COUNTY = col(
    "county_id",
    "STRING",
    (
        "Five-digit FIPS county code, state code followed by county code",
        "Código FIPS de cinco dígitos do condado, código do estado seguido "
        "do código do condado",
        "Código FIPS de cinco dígitos del condado, código del estado seguido "
        "del código del condado",
    ),
    directory="br_bd_diretorios_us.county:id_county",
    obs=(
        "NULL on statewide rows (source county code 999), which hold "
        "establishments without a fixed location such as employee leasing "
        "establishments. County codes are vintage consistent and reflect 2023 "
        "geography",
        "NULL nas linhas estaduais (código de condado 999 na fonte), que "
        "reúnem estabelecimentos sem localização fixa, como os de locação de "
        "mão de obra. Os códigos de condado são consistentes no tempo e "
        "refletem a geografia de 2023",
        "NULL en las filas estatales (código de condado 999 en la fuente), "
        "que reúnen establecimientos sin ubicación fija, como los de "
        "suministro de personal. Los códigos de condado son consistentes en "
        "el tiempo y reflejan la geografía de 2023",
    ),
    original="st, cty",
)
CBSA = col(
    "cbsa_id",
    "STRING",
    (
        "Code of the metropolitan or micropolitan statistical area",
        "Código da área estatística metropolitana ou micropolitana",
        "Código del área estadística metropolitana o micropolitana",
    ),
    directory="br_bd_diretorios_us.cbsa_2023:id_cbsa",
    obs=(
        "OMB delineation of July 2023, applied to every year",
        "Delimitação do OMB de julho de 2023, aplicada a todos os anos",
        "Delimitación de la OMB de julio de 2023, aplicada a todos los años",
    ),
    original="msa",
)
METRO = col(
    "metro_status",
    "STRING",
    (
        "Whether establishments are in a metropolitan or micropolitan area "
        "(M), outside one (N), or without a fixed location (SW)",
        "Se os estabelecimentos estão em área metropolitana ou micropolitana "
        "(M), fora dela (N) ou sem localização fixa (SW)",
        "Si los establecimientos están en un área metropolitana o "
        "micropolitana (M), fuera de ella (N) o sin ubicación fija (SW)",
    ),
    dictionary="yes",
    original="metro",
)
NAICS = col(
    "naics_id",
    "STRING",
    (
        "NAICS 2017 industry code: 2-digit sector, or vintage consistent 3- "
        "or 4-digit industry",
        "Código de indústria NAICS 2017: setor de 2 dígitos, ou indústria de "
        "3 ou 4 dígitos consistente no tempo",
        "Código de industria NAICS 2017: sector de 2 dígitos, o industria de "
        "3 o 4 dígitos consistente en el tiempo",
    ),
    directory="br_bd_diretorios_us.naics_2017:id_naics",
    obs=(
        "The breakdown column says the level (sector, vcnaics3 or vcnaics4). "
        "The hyphenated sectors 31-33, 44-45 and 48-49 are stored as 31, 44 "
        "and 48, the keys the NAICS directory uses for them",
        "A coluna breakdown indica o nível (sector, vcnaics3 ou vcnaics4). "
        "Os setores hifenizados 31-33, 44-45 e 48-49 são armazenados como "
        "31, 44 e 48, as chaves que o diretório NAICS usa para eles",
        "La columna breakdown indica el nivel (sector, vcnaics3 o vcnaics4). "
        "Los sectores con guion 31-33, 44-45 y 48-49 se almacenan como 31, "
        "44 y 48, las claves que el directorio NAICS usa para ellos",
    ),
    original="sector, vcnaics3, vcnaics4",
)


def _class(name: str, en: str, pt: str, es: str, obs, original: str) -> dict:
    return col(name, "STRING", (en, pt, es), obs=obs, original=original)


FIRM_AGE = _class(
    "firm_age",
    "Firm age class in years, from the year the firm first had employment",
    "Classe de idade da firma em anos, desde o ano em que a firma teve "
    "empregados pela primeira vez",
    "Clase de edad de la empresa en años, desde el año en que la empresa "
    "tuvo empleados por primera vez",
    AGE_OBS,
    "fage, fagecoarse",
)
FIRM_SIZE = _class(
    "firm_size",
    "Firm size class, by the average of the firm's employment in the prior "
    "and current year",
    "Classe de tamanho da firma, pela média do emprego da firma no ano "
    "anterior e no ano corrente",
    "Clase de tamaño de la empresa, por el promedio del empleo de la empresa "
    "en el año anterior y en el año corriente",
    SIZE_OBS,
    "fsize, fsizecoarse",
)
INITIAL_FIRM_SIZE = _class(
    "initial_firm_size",
    "Initial firm size class, by the firm's employment in the prior year, or "
    "in the current year if the prior year's was zero",
    "Classe de tamanho inicial da firma, pelo emprego da firma no ano "
    "anterior, ou no ano corrente se o do ano anterior for zero",
    "Clase de tamaño inicial de la empresa, por el empleo de la empresa en "
    "el año anterior, o en el año corriente si el del año anterior es cero",
    SIZE_OBS,
    "ifsize, ifsizecoarse",
)
ESTAB_AGE = _class(
    "establishment_age",
    "Establishment age class in years, from the year the establishment first "
    "had employment",
    "Classe de idade do estabelecimento em anos, desde o ano em que o "
    "estabelecimento teve empregados pela primeira vez",
    "Clase de edad del establecimiento en años, desde el año en que el "
    "establecimiento tuvo empleados por primera vez",
    AGE_OBS,
    "eage, eagecoarse",
)
ESTAB_SIZE = _class(
    "establishment_size",
    "Establishment size class, by the average of the establishment's "
    "employment in the prior and current year",
    "Classe de tamanho do estabelecimento, pela média do emprego do "
    "estabelecimento no ano anterior e no ano corrente",
    "Clase de tamaño del establecimiento, por el promedio del empleo del "
    "establecimiento en el año anterior y en el año corriente",
    SIZE_OBS,
    "esize, esizecoarse",
)
INITIAL_ESTAB_SIZE = _class(
    "initial_establishment_size",
    "Initial establishment size class, by the establishment's employment in "
    "the prior year, or in the current year if the prior year's was zero",
    "Classe de tamanho inicial do estabelecimento, pelo emprego do "
    "estabelecimento no ano anterior, ou no ano corrente se o do ano "
    "anterior for zero",
    "Clase de tamaño inicial del establecimiento, por el empleo del "
    "establecimiento en el año anterior, o en el año corriente si el del año "
    "anterior es cero",
    SIZE_OBS,
    "iesize, iesizecoarse",
)

# (name, source name, unit, EN, PT, ES)
MEASURES = [
    ("firms", "firms", "firm",
     "Number of firms",
     "Número de firmas",
     "Número de empresas"),
    ("establishments", "estabs", "establishment",
     "Number of establishments",
     "Número de estabelecimentos",
     "Número de establecimientos"),
    ("employment", "emp", "employee",
     "Paid employment in the pay period including March 12",
     "Emprego remunerado no período de pagamento que inclui 12 de março",
     "Empleo remunerado en el período de pago que incluye el 12 de marzo"),
    ("dhs_denominator", "denom", "employee",
     "Davis-Haltiwanger-Schuh denominator: average of employment in the "
     "prior and current year",
     "Denominador de Davis-Haltiwanger-Schuh: média do emprego no ano "
     "anterior e no ano corrente",
     "Denominador de Davis-Haltiwanger-Schuh: promedio del empleo en el año "
     "anterior y en el año corriente"),
    ("establishment_entries", "estabs_entry", "establishment",
     "Number of establishments born in the last 12 months",
     "Número de estabelecimentos nascidos nos últimos 12 meses",
     "Número de establecimientos nacidos en los últimos 12 meses"),
    ("establishment_entry_rate", "estabs_entry_rate", "percent",
     "Establishment entries over the average of establishments in the prior "
     "and current year, times 100",
     "Entradas de estabelecimentos sobre a média de estabelecimentos no ano "
     "anterior e no ano corrente, vezes 100",
     "Entradas de establecimientos sobre el promedio de establecimientos en "
     "el año anterior y en el año corriente, por 100"),
    ("establishment_exits", "estabs_exit", "establishment",
     "Number of establishments that exited in the last 12 months",
     "Número de estabelecimentos que saíram nos últimos 12 meses",
     "Número de establecimientos que salieron en los últimos 12 meses"),
    ("establishment_exit_rate", "estabs_exit_rate", "percent",
     "Establishment exits over the average of establishments in the prior "
     "and current year, times 100",
     "Saídas de estabelecimentos sobre a média de estabelecimentos no ano "
     "anterior e no ano corrente, vezes 100",
     "Salidas de establecimientos sobre el promedio de establecimientos en "
     "el año anterior y en el año corriente, por 100"),
    ("job_creation", "job_creation", "job",
     "Employment gains from opening and expanding establishments since "
     "March of the prior year",
     "Ganhos de emprego de estabelecimentos que abriram ou se expandiram "
     "desde março do ano anterior",
     "Ganancias de empleo de establecimientos que abrieron o se expandieron "
     "desde marzo del año anterior"),
    ("job_creation_births", "job_creation_births", "job",
     "Employment gains from establishment births since March of the prior "
     "year",
     "Ganhos de emprego de nascimentos de estabelecimentos desde março do "
     "ano anterior",
     "Ganancias de empleo de nacimientos de establecimientos desde marzo del "
     "año anterior"),
    ("job_creation_continuers", "job_creation_continuers", "job",
     "Employment gains from continuing establishments that expanded since "
     "March of the prior year",
     "Ganhos de emprego de estabelecimentos contínuos que se expandiram "
     "desde março do ano anterior",
     "Ganancias de empleo de establecimientos continuos que se expandieron "
     "desde marzo del año anterior"),
    ("job_creation_rate_births", "job_creation_rate_births", "percent",
     "Job creation from births over the DHS denominator, times 100",
     "Criação de empregos por nascimentos sobre o denominador DHS, vezes 100",
     "Creación de empleos por nacimientos sobre el denominador DHS, por 100"),
    ("job_creation_rate", "job_creation_rate", "percent",
     "Job creation over the DHS denominator, times 100",
     "Criação de empregos sobre o denominador DHS, vezes 100",
     "Creación de empleos sobre el denominador DHS, por 100"),
    ("job_destruction", "job_destruction", "job",
     "Employment losses from closing and contracting establishments since "
     "March of the prior year",
     "Perdas de emprego de estabelecimentos que fecharam ou se contraíram "
     "desde março do ano anterior",
     "Pérdidas de empleo de establecimientos que cerraron o se contrajeron "
     "desde marzo del año anterior"),
    ("job_destruction_deaths", "job_destruction_deaths", "job",
     "Employment losses from establishment deaths since March of the prior "
     "year",
     "Perdas de emprego de mortes de estabelecimentos desde março do ano "
     "anterior",
     "Pérdidas de empleo de muertes de establecimientos desde marzo del año "
     "anterior"),
    ("job_destruction_continuers", "job_destruction_continuers", "job",
     "Employment losses from continuing establishments that contracted since "
     "March of the prior year",
     "Perdas de emprego de estabelecimentos contínuos que se contraíram "
     "desde março do ano anterior",
     "Pérdidas de empleo de establecimientos continuos que se contrajeron "
     "desde marzo del año anterior"),
    ("job_destruction_rate_deaths", "job_destruction_rate_deaths", "percent",
     "Job destruction from deaths over the DHS denominator, times 100",
     "Destruição de empregos por mortes sobre o denominador DHS, vezes 100",
     "Destrucción de empleos por muertes sobre el denominador DHS, por 100"),
    ("job_destruction_rate", "job_destruction_rate", "percent",
     "Job destruction over the DHS denominator, times 100",
     "Destruição de empregos sobre o denominador DHS, vezes 100",
     "Destrucción de empleos sobre el denominador DHS, por 100"),
    ("net_job_creation", "net_job_creation", "job",
     "Job creation minus job destruction",
     "Criação de empregos menos destruição de empregos",
     "Creación de empleos menos destrucción de empleos"),
    ("net_job_creation_rate", "net_job_creation_rate", "percent",
     "Job creation rate minus job destruction rate",
     "Taxa de criação de empregos menos taxa de destruição de empregos",
     "Tasa de creación de empleos menos tasa de destrucción de empleos"),
    ("reallocation_rate", "reallocation_rate", "percent",
     "Excess job reallocation rate: job creation rate plus job destruction "
     "rate minus the absolute net job creation rate",
     "Taxa de realocação excedente de empregos: taxa de criação mais taxa de "
     "destruição de empregos menos o valor absoluto da taxa líquida",
     "Tasa de reasignación excedente de empleos: tasa de creación más tasa "
     "de destrucción de empleos menos el valor absoluto de la tasa neta"),
    ("firm_death_firms", "firmdeath_firms", "firm",
     "Number of firms that exited entirely, with every establishment closed",
     "Número de firmas que saíram por completo, com todos os "
     "estabelecimentos fechados",
     "Número de empresas que salieron por completo, con todos los "
     "establecimientos cerrados"),
    ("firm_death_establishments", "firmdeath_estabs", "establishment",
     "Number of establishments of firms that exited entirely",
     "Número de estabelecimentos de firmas que saíram por completo",
     "Número de establecimientos de empresas que salieron por completo"),
    ("firm_death_employment", "firmdeath_emp", "employee",
     "Employment of firms that exited entirely",
     "Emprego de firmas que saíram por completo",
     "Empleo de empresas que salieron por completo"),
]  # fmt: skip

FLAGS = col(
    "suppression_flags",
    "STRING",
    (
        "Measures the source suppressed in this row, as measure=flag pairs "
        "separated by semicolons",
        "Medidas que a fonte suprimiu nesta linha, como pares medida=flag "
        "separados por ponto e vírgula",
        "Medidas que la fuente suprimió en esta fila, como pares "
        "medida=indicador separados por punto y coma",
    ),
    obs=(
        "D: disclosure suppression (too few firms). S: data quality "
        "suppression. X: structurally missing or structurally zero, e.g. "
        "exits of age-0 firms, or firms aged 5 before 1982. N: rate not "
        "available because its denominator is zero. NULL when no measure is "
        "suppressed",
        "D: supressão por sigilo (poucas firmas). S: supressão por qualidade "
        "dos dados. X: estruturalmente ausente ou estruturalmente zero, por "
        "exemplo saídas de firmas de idade 0, ou firmas de idade 5 antes de "
        "1982. N: taxa indisponível porque o denominador é zero. NULL quando "
        "nenhuma medida é suprimida",
        "D: supresión por confidencialidad (pocas empresas). S: supresión por "
        "calidad de los datos. X: estructuralmente ausente o estructuralmente "
        "cero, por ejemplo salidas de empresas de edad 0, o empresas de edad "
        "5 antes de 1982. N: tasa no disponible porque el denominador es "
        "cero. NULL cuando ninguna medida está suprimida",
    ),
    original="(D, S, X, N cell values)",
)


def measure_columns() -> list[dict[str, str]]:
    """Return the measure columns, which every table shares."""
    rows = []
    for name, original, unit, en, pt, es in MEASURES:
        bq_type = "FLOAT64" if unit == "percent" else "INT64"
        rows.append(
            col(
                name,
                bq_type,
                (en, pt, es),
                unit=unit,
                obs=NULL_OBS,
                original=original,
            )
        )
    return rows


CLASSES = [FIRM_AGE, FIRM_SIZE, INITIAL_FIRM_SIZE, ESTAB_AGE, ESTAB_SIZE,
           INITIAL_ESTAB_SIZE]  # fmt: skip
# County and metropolitan-area files are published only with the coarse
# firm and establishment-age, firm-size and initial-firm-size classes.
SUBSTATE_CLASSES = [FIRM_AGE, FIRM_SIZE, INITIAL_FIRM_SIZE, ESTAB_AGE]

TABLES = {
    "country": [YEAR, BREAKDOWN, METRO, NAICS, *CLASSES],
    "state": [YEAR, BREAKDOWN, STATE, METRO, NAICS, *CLASSES],
    "county": [YEAR, BREAKDOWN, STATE, COUNTY, NAICS, *SUBSTATE_CLASSES],
    "metropolitan_area": [YEAR, BREAKDOWN, CBSA, NAICS, *SUBSTATE_CLASSES],
}

DICTIONARY = [
    col("table_id", "STRING", (
        "Slug of the us_census_bds table the dictionary entry describes",
        "Slug da tabela de us_census_bds que a entrada do dicionário descreve",
        "Slug de la tabla de us_census_bds que describe la entrada del "
        "diccionario")),
    col("column_name", "STRING", (
        "Name of the column the dictionary entry describes",
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario")),
    col("key", "STRING", (
        "Coded value (key) exactly as stored in the data",
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Valor codificado (clave) exactamente como se almacena en los datos")),
    col("temporal_coverage", "STRING", (
        "Temporal coverage of the key",
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave")),
    col("value", "STRING", (
        "Human-readable label corresponding to the coded value",
        "Rótulo legível correspondente ao valor codificado",
        "Etiqueta legible correspondiente al valor codificado")),
]  # fmt: skip


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write architecture rows with LF line endings, as the formatters expect."""
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, dims in TABLES.items():
        rows = [*dims, *measure_columns(), FLAGS]
        write_csv(ARCH / f"{table}.csv", rows)
        print(f"{table}: {len(rows)} columns")
    write_csv(ARCH / "dictionary.csv", DICTIONARY)


if __name__ == "__main__":
    main()
