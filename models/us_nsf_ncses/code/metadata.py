"""Register the us_nsf_ncses metadata in the Data Basis backend.

Fills the existing NCSES dataset shell — the record created for the Survey of
Earned Doctorates, id 60aaf560-d82a-42b9-bf86-155d18b8c5bf — and broadens it to
the agency's two surveys rather than creating a second dataset. See
``models/us_nsf_ncses/README.md`` for why.

Columns come from the architecture CSVs, so the registered types, descriptions
and directory links cannot drift from the dbt models, which are generated from
the same files.

Run with the shared venv, which has fastmcp and requests:

    ~/.venvs/bd-pipelines/bin/python models/us_nsf_ncses/code/metadata.py staging
    ~/.venvs/bd-pipelines/bin/python models/us_nsf_ncses/code/metadata.py prod
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path.home()
        / "Monash Uni Enterprise Dropbox"
        / "Ricardo Dahis"
        / "BD"
        / "mcp"
    ),
)

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"

DATASET_ID = "60aaf560-d82a-42b9-bf86-155d18b8c5bf"
DATASET_SLUG = "ncses"
GCP_DATASET = "us_nsf_ncses"

NAME_PT = "Estatísticas de Ciência e Engenharia do NCSES (HERD e SED)"
NAME_EN = "NCSES Science and Engineering Statistics (HERD and SED)"
NAME_ES = "Estadísticas de Ciencia e Ingeniería del NCSES (HERD y SED)"

DESCRIPTION_PT = (
    "Dois levantamentos do National Center for Science and Engineering "
    "Statistics (NCSES), a agência estatística federal dentro da National "
    "Science Foundation dos Estados Unidos. O Higher Education Research and "
    "Development Survey (HERD) é um censo anual das faculdades e universidades "
    "americanas que gastam ao menos US$ 150 mil em pesquisa e desenvolvimento "
    "orçado separadamente; aqui estão seus arquivos de uso público em nível de "
    "instituição para os anos fiscais de 1972 a 2024, incluindo o levantamento "
    "anterior, o Survey of R&D Expenditures at Universities and Colleges, com "
    "despesas por campo de pesquisa, fonte de recursos e agência federal "
    "financiadora, além das contagens de pessoal de pesquisa. O Survey of "
    "Earned Doctorates (SED) é um censo anual, realizado desde o ano acadêmico "
    "de 1958, de todos os que recebem um doutorado de pesquisa de uma "
    "instituição americana credenciada; aqui estão suas tabelas agregadas "
    "publicadas, já que os registros individuais são de uso restrito. As "
    "instituições do HERD carregam o UNITID do IPEDS, o que liga este conjunto "
    "a us_ed_ipeds e us_ed_college_scorecard; com us_nih_reporter, os três "
    "descrevem uma mesma cadeia: o financiamento federal que entra, o gasto em "
    "pesquisa que a instituição realiza e os pesquisadores que o sistema forma."
)
DESCRIPTION_EN = (
    "Two surveys run by the National Center for Science and Engineering "
    "Statistics (NCSES), the federal statistical agency inside the U.S. "
    "National Science Foundation. The Higher Education Research and "
    "Development (HERD) Survey is an annual census of U.S. colleges and "
    "universities that spend at least $150,000 on separately budgeted research "
    "and development; this dataset carries its institution-level public use "
    "files for fiscal years 1972 to 2024, including the predecessor Survey of "
    "R&D Expenditures at Universities and Colleges, with expenditures by field "
    "of research, source of funds and funding federal agency, and counts of "
    "research personnel. The Survey of Earned Doctorates (SED) is an annual "
    "census, conducted since academic year 1958, of everyone receiving a "
    "research doctorate from an accredited U.S. institution; this dataset "
    "carries its published aggregate tables, since the individual records are "
    "restricted use. HERD institutions carry their IPEDS UNITID, which joins "
    "this dataset to us_ed_ipeds and us_ed_college_scorecard; with "
    "us_nih_reporter the three describe one chain: the federal funding that "
    "comes in, the research spending the institution performs, and the "
    "researchers the system produces."
)
DESCRIPTION_ES = (
    "Dos encuestas del National Center for Science and Engineering Statistics "
    "(NCSES), la agencia estadística federal dentro de la National Science "
    "Foundation de los Estados Unidos. La Higher Education Research and "
    "Development Survey (HERD) es un censo anual de las universidades "
    "estadounidenses que gastan al menos 150 mil dólares en investigación y "
    "desarrollo presupuestado por separado; aquí están sus archivos de uso "
    "público a nivel de institución para los años fiscales de 1972 a 2024, "
    "incluida la encuesta anterior, el Survey of R&D Expenditures at "
    "Universities and Colleges, con gastos por campo de investigación, fuente "
    "de recursos y agencia federal financiadora, además de los conteos de "
    "personal de investigación. La Survey of Earned Doctorates (SED) es un "
    "censo anual, realizado desde el año académico de 1958, de todas las "
    "personas que reciben un doctorado de investigación de una institución "
    "estadounidense acreditada; aquí están sus tablas agregadas publicadas, ya "
    "que los registros individuales son de uso restringido. Las instituciones "
    "del HERD llevan el UNITID del IPEDS, lo que conecta este conjunto con "
    "us_ed_ipeds y us_ed_college_scorecard; junto con us_nih_reporter, los tres "
    "describen una misma cadena: el financiamiento federal que entra, el gasto "
    "en investigación que realiza la institución y los investigadores que el "
    "sistema forma."
)

# Reference ids resolved from discover_ids; identical on staging and prod for
# everything except the tag slugs, which the script re-resolves per environment.
REFS = {
    "status_under_review": "47208305-325a-4da9-9222-ac6849405b78",
    "status_published": "e16221de-ac30-4926-83d3-de219998dab3",
    "theme_economics": "ad6a413a-e882-4dd6-a497-8a62eec8511b",
    "theme_science": "dc2e153b-8b1f-42ad-beac-7d450c9953b5",
    "theme_education": "011ab0e3-d5b3-47c8-807f-81c07897fe12",
    "entity_year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "entity_institution": "cc6669a8-4c95-4250-b04b-1a9724546e62",
    "entity_document": "1d5e94c7-65e7-405b-b788-4d5975eddde9",
    "license_ppdl": "8ab5a987-34e6-4f37-86a1-13e97ca498e3",
    "availability_online": "dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe",
}

HERD_LEVELS = ("year", "institution")
SED_LEVELS = ("year", "document")

TABLES = [
    {
        "slug": "herd_institution",
        "name_pt": "Instituições pesquisadas (HERD)",
        "name_en": "Surveyed institutions (HERD)",
        "name_es": "Instituciones encuestadas (HERD)",
        "description_pt": (
            "Instituições de ensino superior dos Estados Unidos pesquisadas "
            "pelo levantamento HERD, uma linha por instituição e ano fiscal, "
            "de 1972 a 2024. Reúne a era atual do levantamento, a partir do "
            "ano fiscal de 2010, e a anterior, o Survey of R&D Expenditures at "
            "Universities and Colleges. A coluna unitid liga a instituição ao "
            "IPEDS."
        ),
        "description_en": (
            "U.S. higher education institutions surveyed by HERD, one row per "
            "institution and fiscal year, from 1972 to 2024. It brings "
            "together the current era of the survey, from fiscal year 2010, "
            "and the earlier Survey of R&D Expenditures at Universities and "
            "Colleges. The unitid column links the institution to IPEDS."
        ),
        "description_es": (
            "Instituciones de educación superior de los Estados Unidos "
            "encuestadas por HERD, una fila por institución y año fiscal, de "
            "1972 a 2024. Reúne la era actual de la encuesta, desde el año "
            "fiscal de 2010, y la anterior, el Survey of R&D Expenditures at "
            "Universities and Colleges. La columna unitid conecta la "
            "institución con IPEDS."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 1972,
        "end": 2024,
    },
    {
        "slug": "herd_expenditure",
        "name_pt": "Despesas em pesquisa e desenvolvimento (HERD)",
        "name_en": "Research and development expenditures (HERD)",
        "name_es": "Gastos en investigación y desarrollo (HERD)",
        "description_pt": (
            "Despesas de pesquisa e desenvolvimento declaradas por instituição "
            "de ensino superior ao levantamento HERD, de 1972 a 2024, em "
            "formato longo: uma linha por instituição, ano fiscal e célula do "
            "questionário. As células cobrem fonte de recursos, campo de "
            "pesquisa, agência federal financiadora, tipo de custo, recursos "
            "estrangeiros, ensaios clínicos, repasses recebidos e repassados e "
            "equipamentos capitalizados. Valores em dólares correntes, "
            "convertidos dos milhares de dólares publicados pelo NCSES."
        ),
        "description_en": (
            "Research and development expenditures reported by higher "
            "education institutions to the HERD Survey, from 1972 to 2024, in "
            "long form: one row per institution, fiscal year and questionnaire "
            "cell. The cells cover source of funds, field of research, funding "
            "federal agency, type of cost, foreign funds, clinical trials, "
            "funds received as a subrecipient and passed through, and "
            "capitalized equipment. Values in current dollars, converted from "
            "the thousands of dollars NCSES publishes."
        ),
        "description_es": (
            "Gastos en investigación y desarrollo declarados por instituciones "
            "de educación superior a la encuesta HERD, de 1972 a 2024, en "
            "formato largo: una fila por institución, año fiscal y celda del "
            "cuestionario. Las celdas cubren fuente de recursos, campo de "
            "investigación, agencia federal financiadora, tipo de costo, "
            "recursos extranjeros, ensayos clínicos, transferencias recibidas y "
            "traspasadas, y equipos capitalizados. Valores en dólares "
            "corrientes, convertidos de los miles de dólares que publica el "
            "NCSES."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 1972,
        "end": 2024,
    },
    {
        "slug": "herd_personnel",
        "name_pt": "Pessoal de pesquisa e desenvolvimento (HERD)",
        "name_en": "Research and development personnel (HERD)",
        "name_es": "Personal de investigación y desarrollo (HERD)",
        "description_pt": (
            "Pessoal de pesquisa e desenvolvimento nas instituições "
            "pesquisadas pelo HERD, em número de pessoas e em equivalentes de "
            "tempo integral, de 2010 a 2024. Os arquivos de uso público não "
            "trazem contagem de pessoal para os anos fiscais de 2020 e 2021, e "
            "os equivalentes de tempo integral passaram a ser coletados no ano "
            "fiscal de 2022."
        ),
        "description_en": (
            "Research and development personnel at the institutions HERD "
            "surveys, as headcounts and as full-time equivalents, from 2010 to "
            "2024. The public use files carry no personnel count for fiscal "
            "years 2020 and 2021, and full-time equivalents were first "
            "collected in fiscal year 2022."
        ),
        "description_es": (
            "Personal de investigación y desarrollo en las instituciones que "
            "encuesta HERD, en número de personas y en equivalentes de tiempo "
            "completo, de 2010 a 2024. Los archivos de uso público no traen "
            "conteo de personal para los años fiscales de 2020 y 2021, y los "
            "equivalentes de tiempo completo comenzaron a recolectarse en el "
            "año fiscal de 2022."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 2010,
        "end": 2024,
    },
    {
        "slug": "herd_survey_item",
        "name_pt": "Outros itens do questionário (HERD)",
        "name_en": "Other questionnaire items (HERD)",
        "name_es": "Otros ítems del cuestionario (HERD)",
        "description_pt": (
            "Itens do questionário HERD que não são despesas nem contagens de "
            "pessoal, de 2010 a 2024: a composição dos recursos próprios da "
            "instituição declarados como pesquisa financiada internamente, a "
            "inclusão de ensaios clínicos no relatório do ano fiscal de 2009 e "
            "os limites de capitalização de equipamentos e de software."
        ),
        "description_en": (
            "HERD questionnaire items that are neither expenditures nor "
            "personnel counts, from 2010 to 2024: what the institution counted "
            "as institutionally financed research, whether clinical trials "
            "were included in the fiscal year 2009 report, and the "
            "capitalization thresholds for equipment and software."
        ),
        "description_es": (
            "Ítems del cuestionario HERD que no son gastos ni conteos de "
            "personal, de 2010 a 2024: la composición de los recursos propios "
            "de la institución declarados como investigación financiada "
            "internamente, la inclusión de ensayos clínicos en el informe del "
            "año fiscal de 2009 y los límites de capitalización de equipos y "
            "de software."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 2010,
        "end": 2024,
    },
    {
        "slug": "sed_estimate",
        "name_pt": "Estimativas publicadas (SED)",
        "name_en": "Published estimates (SED)",
        "name_es": "Estimaciones publicadas (SED)",
        "description_pt": (
            "Estimativas publicadas pelo Survey of Earned Doctorates em "
            "formato longo: uma linha por célula das tabelas de dados do "
            "ciclo, com o caminho hierárquico completo da linha e da coluna "
            "preservado. Cobre contagens de doutores por ano, campo, sexo, "
            "situação de cidadania, etnia e raça, além de compromissos após a "
            "titulação, apoio financeiro, dívida educacional, tempo até o "
            "título, salários e instituições de origem e de titulação. A "
            "microdados individual do SED é de uso restrito e não está aqui. "
            "Cada ciclo republica a própria série histórica, então filtre pelo "
            "maior reference_year em vez de somar entre ciclos."
        ),
        "description_en": (
            "Estimates published by the Survey of Earned Doctorates in long "
            "form: one row per cell of the cycle's data tables, with the full "
            "hierarchical path of both the row and the column preserved. It "
            "covers counts of doctorate recipients by year, field, sex, "
            "citizenship status, ethnicity and race, along with postgraduation "
            "commitments, financial support, education-related debt, time to "
            "degree, salaries, and baccalaureate-origin and doctorate-granting "
            "institutions. SED individual microdata is restricted use and is "
            "not here. Each cycle republishes its own history, so filter to the "
            "largest reference_year rather than summing across cycles."
        ),
        "description_es": (
            "Estimaciones publicadas por la Survey of Earned Doctorates en "
            "formato largo: una fila por celda de las tablas de datos del "
            "ciclo, con la ruta jerárquica completa de la fila y de la columna "
            "preservada. Cubre conteos de doctores por año, campo, sexo, "
            "situación de ciudadanía, etnia y raza, además de compromisos tras "
            "la titulación, apoyo financiero, deuda educativa, tiempo hasta el "
            "título, salarios e instituciones de origen y de titulación. Los "
            "microdatos individuales del SED son de uso restringido y no están "
            "aquí. Cada ciclo republica su propia serie histórica, así que "
            "filtre por el mayor reference_year en vez de sumar entre ciclos."
        ),
        "levels": SED_LEVELS,
        "level_columns": {"year": "year", "document": "table_id"},
        "start": 1958,
        "end": 2024,
    },
    {
        "slug": "sed_data_table",
        "name_pt": "Catálogo das tabelas publicadas (SED)",
        "name_en": "Catalogue of published tables (SED)",
        "name_es": "Catálogo de las tablas publicadas (SED)",
        "description_pt": (
            "Catálogo das tabelas de dados publicadas pelo Survey of Earned "
            "Doctorates em cada ciclo da pesquisa: identificador, título, grupo "
            "temático e declaração de unidade. Serve de índice para "
            "sed_estimate, que traz as células dessas mesmas tabelas."
        ),
        "description_en": (
            "Catalogue of the data tables the Survey of Earned Doctorates "
            "publishes in each survey cycle: identifier, title, thematic group "
            "and unit statement. It indexes sed_estimate, which carries the "
            "cells of those same tables."
        ),
        "description_es": (
            "Catálogo de las tablas de datos que publica la Survey of Earned "
            "Doctorates en cada ciclo de la encuesta: identificador, título, "
            "grupo temático y declaración de unidad. Sirve de índice para "
            "sed_estimate, que trae las celdas de esas mismas tablas."
        ),
        "levels": SED_LEVELS,
        "level_columns": {"year": "reference_year", "document": "table_id"},
        "start": 2024,
        "end": 2024,
    },
    {
        "slug": "dicionario",
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário de valores codificados do conjunto. Vários códigos "
            "mudam de significado entre as duas eras do levantamento HERD, por "
            "isso cada entrada traz sua própria cobertura temporal."
        ),
        "description_en": (
            "Dictionary of the dataset's coded values. Several codes change "
            "meaning between the two eras of the HERD Survey, so each entry "
            "carries its own temporal coverage."
        ),
        "description_es": (
            "Diccionario de los valores codificados del conjunto. Varios "
            "códigos cambian de significado entre las dos eras de la encuesta "
            "HERD, por eso cada entrada trae su propia cobertura temporal."
        ),
        "levels": (),
        "level_columns": {},
        "start": 1972,
        "end": 2024,
    },
]

RAW_SOURCES = [
    {
        "name_pt": "Arquivos de uso público do HERD",
        "name_en": "HERD public use data files",
        "name_es": "Archivos de uso público del HERD",
        "url": (
            "https://ncses.nsf.gov/explore-data/microdata/"
            "higher-education-research-development"
        ),
        "tables": [
            "herd_institution",
            "herd_expenditure",
            "herd_personnel",
            "herd_survey_item",
        ],
    },
    {
        "name_pt": "Tabelas de dados do SED",
        "name_en": "SED data tables",
        "name_es": "Tablas de datos del SED",
        "url": "https://ncses.nsf.gov/surveys/earned-doctorates",
        "tables": ["sed_estimate", "sed_data_table"],
    },
]


def architecture(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_payload(table: str) -> str:
    """Build the bulk_upsert_columns payload from the architecture CSV."""
    payload = []
    for i, row in enumerate(architecture(table)):
        entry = {
            "name": row["name"],
            "bigquery_type": row["bigquery_type"].lower(),
            "description": row["description"],
            "description_en": row["description_en"],
            "description_es": row["description_es"],
            "covered_by_dictionary": row["covered_by_dictionary"] == "yes",
            "has_sensitive_data": row["has_sensitive_data"] == "yes",
            "is_partition": row["name"] in {"year", "reference_year"}
            and i == 0,
            "order": i,
        }
        if row["directory_column"]:
            entry["directory_column"] = row["directory_column"]
        if row["measurement_unit"]:
            entry["measurement_unit"] = row["measurement_unit"]
        if row["observations"]:
            entry["observations"] = row["observations"]
        payload.append(entry)
    return json.dumps(payload, ensure_ascii=False)


def main() -> int:
    env = sys.argv[1] if len(sys.argv) > 1 else "staging"
    if env not in {"staging", "prod"}:
        raise SystemExit("env must be 'staging' or 'prod'")
    print(f"registering us_nsf_ncses metadata on {env}", flush=True)
    print(json.dumps({"dataset": DATASET_ID, "tables": len(TABLES)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
