"""Register the us_census_cog metadata in the Data Basis backend.

    python metadata.py staging
    python metadata.py prod

Re-runnable: every record it creates is written back to ``metadata_ids.json``
next to this file and passed as ``id`` on the next run, because the backend's
create_update_* calls are not idempotent without one and would otherwise
duplicate observation levels, cloud tables, coverages and updates.

Column payloads are built from the architecture CSVs, which stay the source of
truth. Tables are linked to their raw data source before any coverage exists:
create_update_table fails once a table has one.
"""

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent
REPO_ROOT = CODE_DIR.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from common import ARCHITECTURE, DATASET_ID  # noqa: E402

from pipelines.datasets.us_census_cog.utils import load_cols  # noqa: E402


def import_databasis_server():
    """Import the Data Basis MCP server module, which holds the backend client.

    The module lives outside this repository. Its location comes from
    ``DATABASIS_MCP_DIR`` when set, and is otherwise found by looking for an
    ``mcp`` checkout beside any ancestor of the repository root -- the extra
    reach matters inside a git worktree, where the root sits several levels
    deeper than usual. Either way this file carries no absolute path of its own.

    Returns:
        The imported ``server`` module.

    Raises:
        SystemExit: The directory holds no ``server.py``.
    """
    override = os.environ.get("DATABASIS_MCP_DIR")
    candidates = (
        [Path(override)]
        if override
        else [parent / "mcp" for parent in REPO_ROOT.parents]
    )
    for candidate in candidates:
        if (candidate / "server.py").exists():
            sys.path.insert(0, str(candidate))
            break
    else:
        raise SystemExit(
            "no mcp/server.py beside this repository. Point DATABASIS_MCP_DIR "
            "at the Data Basis MCP checkout."
        )
    import server

    return server


server = import_databasis_server()


IDS = CODE_DIR / "metadata_ids.json"
# The per-table documentation bundles built by build_auxiliary_files.py. They
# sit on the data bucket rather than gs://basedosdados-public because the
# uploader account cannot write to the latter, so the URL returns HTTP 400 to an
# anonymous visitor — the same state as every other auxiliary link in production.
AUXILIARY_FILES = "https://storage.googleapis.com/basedosdados-dev/auxiliary_files/us_census_cog"
SLUG = "census_governments"
AREA = "us"

# Row counts and coverage measured by validate.py on the cleaned output.
TABLES = {
    "government_unit": {
        "name": (
            "Unidades de governo",
            "Government units",
            "Unidades de gobierno",
        ),
        "years": (1997, 2025),
        "source": "gus",
        "levels": ["year", "agency"],
    },
    "employment": {
        "name": (
            "Emprego e folha de pagamento",
            "Employment and payroll",
            "Empleo y nómina",
        ),
        "years": (1992, 2024),
        "source": "apes",
        "levels": ["year", "agency", "item"],
    },
    "employment_unit": {
        "name": (
            "Unidades do levantamento de emprego",
            "Employment survey units",
            "Unidades del censo de empleo",
        ),
        "years": (1992, 2024),
        "source": "apes",
        "levels": ["year", "agency"],
    },
    "finance": {
        "name": ("Finanças", "Finances", "Finanzas"),
        "years": (1967, 2018),
        "source": "finances",
        "levels": ["year", "agency", "item"],
    },
    "finance_unit": {
        "name": (
            "Unidades do levantamento de finanças",
            "Finance survey units",
            "Unidades del censo de finanzas",
        ),
        "years": (1967, 2018),
        "source": "finances",
        "levels": ["year", "agency"],
    },
    "dicionario": {
        "name": ("Dicionário", "Dictionary", "Diccionario"),
        "years": None,
        "source": None,
        "levels": [],
    },
}
TABLE_ORDER = list(TABLES)

RAW_SOURCES = {
    "landing": {
        "name": (
            "Censo de Governos",
            "Census of Governments",
            "Censo de Gobiernos",
        ),
        "url": "https://www.census.gov/programs-surveys/cog.html",
        "description": (
            "Página do programa Censo de Governos, com os três componentes do "
            "levantamento, a documentação metodológica e as tabelas publicadas",
            "Program page for the Census of Governments, carrying the three "
            "components of the survey, the methodological documentation and the "
            "published tables",
            "Página del programa Censo de Gobiernos, con los tres componentes "
            "del censo, la documentación metodológica y las tablas publicadas",
        ),
    },
    "gus": {
        "name": (
            "Levantamento de unidades de governo",
            "Government Units Survey",
            "Censo de unidades de gobierno",
        ),
        "url": "https://www2.census.gov/programs-surveys/gus/datasets/",
        "description": (
            "Planilhas com a lista de todas as unidades de governo estaduais e "
            "locais, uma por ano de levantamento, com uma aba por tipo de "
            "governo",
            "Workbooks listing every state and local government unit, one per "
            "survey year, with one worksheet per type of government",
            "Planillas con la lista de todas las unidades de gobierno estatales "
            "y locales, una por año de censo, con una hoja por tipo de gobierno",
        ),
    },
    "apes": {
        "name": (
            "Arquivos por unidade de emprego e folha de pagamento",
            "Employment and payroll individual unit files",
            "Archivos por unidad de empleo y nómina",
        ),
        "url": "https://www2.census.gov/programs-surveys/apes/datasets/",
        "description": (
            "Arquivos de largura fixa com emprego e folha de pagamento por "
            "unidade de governo e categoria funcional, um par de arquivos de "
            "dados e de diretório por ano",
            "Fixed-width files carrying employment and payroll by government "
            "unit and functional category, one data and directory file pair per "
            "year",
            "Archivos de ancho fijo con empleo y nómina por unidad de gobierno "
            "y categoría funcional, un par de archivos de datos y de directorio "
            "por año",
        ),
    },
    "finances": {
        "name": (
            "Arquivos por unidade de finanças",
            "Finance individual unit files",
            "Archivos por unidad de finanzas",
        ),
        "url": "https://www2.census.gov/programs-surveys/gov-finances/datasets/",
        "description": (
            "Receita, despesa, dívida e ativos por unidade de governo. Os "
            "exercícios de 1967 a 2012 estão em um arquivo histórico com uma "
            "linha por governo e 529 colunas; de 2013 a 2018 há um arquivo por "
            "ano com uma linha por item",
            "Revenue, expenditure, debt and assets by government unit. Fiscal "
            "1967 to 2012 sit in a historical archive with one row per "
            "government and 529 columns; 2013 to 2018 have one file per year "
            "with one row per item",
            "Ingresos, gastos, deuda y activos por unidad de gobierno. Los "
            "ejercicios de 1967 a 2012 están en un archivo histórico con una "
            "fila por gobierno y 529 columnas; de 2013 a 2018 hay un archivo "
            "por año con una fila por ítem",
        ),
    },
}

DATASET_DESCRIPTION = (
    "O Censo de Governos mede a estrutura, o emprego e as finanças de todos os "
    "governos estaduais e locais dos Estados Unidos. O conjunto reúne os três "
    "componentes do programa: a lista de unidades de governo dos levantamentos "
    "de 1997 a 2025, o emprego e a folha de pagamento por categoria funcional "
    "de 1992 a 2024, e receita, despesa, dívida e ativos por item financeiro "
    "dos exercícios de 1967 a 2018. As três famílias usam identificadores "
    "distintos, preservados nas colunas government_id e government_id_govs.",
    "The Census of Governments measures the structure, employment and finances "
    "of every state and local government in the United States. This dataset "
    "brings together the three components of the program: the list of "
    "government units from the 1997 to 2025 surveys, employment and payroll by "
    "functional category from 1992 to 2024, and revenue, expenditure, debt and "
    "assets by finance item for fiscal 1967 to 2018. The three families use "
    "different identifiers, preserved in the government_id and "
    "government_id_govs columns.",
    "El Censo de Gobiernos mide la estructura, el empleo y las finanzas de "
    "todos los gobiernos estatales y locales de Estados Unidos. El conjunto "
    "reúne los tres componentes del programa: la lista de unidades de gobierno "
    "de los censos de 1997 a 2025, el empleo y la nómina por categoría "
    "funcional de 1992 a 2024, e ingresos, gastos, deuda y activos por ítem "
    "financiero de los ejercicios de 1967 a 2018. Las tres familias usan "
    "identificadores distintos, preservados en las columnas government_id y "
    "government_id_govs.",
)

TABLE_DESCRIPTIONS = {
    "government_unit": (
        "Uma linha por unidade de governo listada em cada levantamento de "
        "unidades de governo, com nome, tipo, endereço, população ou matrícula "
        "e localização. Os anos de 2002 e 2007 são publicados em um formato "
        "por tipo de governo incompatível com os demais e ficaram de fora. A "
        "coluna unit_category separa os governos independentes dos sistemas "
        "escolares e previdenciários dependentes, que a fonte lista ao lado "
        "deles mas não conta como governos: em 2022 as três categorias "
        "independentes somam 90.837 unidades, exatamente a contagem publicada "
        "pelo Census Bureau.",
        "One row per government unit listed in each government units survey, "
        "with name, type, address, population or enrollment and location. The "
        "2002 and 2007 surveys are published in a per-type format incompatible "
        "with the rest and are left out. The unit_category column separates "
        "independent governments from the dependent school and pension systems "
        "the source lists beside them but does not count as governments: in "
        "2022 the three independent categories total 90,837 units, exactly the "
        "count the Census Bureau published.",
        "Una fila por unidad de gobierno listada en cada censo de unidades de "
        "gobierno, con nombre, tipo, dirección, población o matrícula y "
        "ubicación. Los años 2002 y 2007 se publican en un formato por tipo de "
        "gobierno incompatible con los demás y quedaron fuera. La columna "
        "unit_category separa los gobiernos independientes de los sistemas "
        "escolares y de pensiones dependientes, que la fuente lista junto a "
        "ellos pero no cuenta como gobiernos: en 2022 las tres categorías "
        "independientes suman 90.837 unidades, exactamente el conteo publicado "
        "por el Census Bureau.",
    ),
    "employment": (
        "Uma linha por unidade de governo e categoria funcional no "
        "levantamento anual de emprego e folha de pagamento do setor público. "
        "Emprego e folha referem-se ao mês de março, e a folha é o equivalente "
        "mensal de 31 dias. Os anos de 1992, 1997, 2002, 2007, 2012, 2017 e "
        "2022 são censos e cobrem todas as unidades; os demais são amostras de "
        "cerca de onze mil unidades, cuja probabilidade de seleção está em "
        "employment_unit. Não há levantamento em 1996, e horas em tempo "
        "parcial e emprego equivalente a tempo integral deixaram de ser "
        "publicados em 2019.",
        "One row per government unit and functional category in the annual "
        "survey of public employment and payroll. Employment and payroll refer "
        "to the month of March, and payroll is the 31-day monthly equivalent. "
        "1992, 1997, 2002, 2007, 2012, 2017 and 2022 are census years covering "
        "every unit; the rest are samples of about eleven thousand units, whose "
        "probability of selection sits in employment_unit. There is no survey "
        "for 1996, and part-time hours and full-time equivalent employment "
        "stopped being published in 2019.",
        "Una fila por unidad de gobierno y categoría funcional en el censo "
        "anual de empleo y nómina del sector público. Empleo y nómina se "
        "refieren al mes de marzo, y la nómina es el equivalente mensual de 31 "
        "días. 1992, 1997, 2002, 2007, 2012, 2017 y 2022 son años censales que "
        "cubren todas las unidades; los demás son muestras de unas once mil "
        "unidades, cuya probabilidad de selección está en employment_unit. No "
        "hay censo en 1996, y las horas a tiempo parcial y el empleo "
        "equivalente a tiempo completo dejaron de publicarse en 2019.",
    ),
    "employment_unit": (
        "Uma linha por unidade de governo pesquisada em cada ano do "
        "levantamento anual de emprego e folha de pagamento do setor público, "
        "com nome, localização, nível de ensino e probabilidade de seleção na "
        "amostra. Complementa a tabela employment, que traz os valores por "
        "categoria funcional. Exceção: o diretório de 1992 lista catorze "
        "governos duas vezes.",
        "One row per government unit surveyed in each year of the annual survey "
        "of public employment and payroll, with name, location, school level "
        "and probability of selection into the sample. It complements the "
        "employment table, which carries the values by functional category. "
        "Exception: the 1992 directory lists fourteen governments twice.",
        "Una fila por unidad de gobierno encuestada en cada año del censo anual "
        "de empleo y nómina del sector público, con nombre, ubicación, nivel de "
        "enseñanza y probabilidad de selección en la muestra. Complementa la "
        "tabla employment, que trae los valores por categoría funcional. "
        "Excepción: el directorio de 1992 lista catorce gobiernos dos veces.",
    ),
    "finance": (
        "Uma linha por unidade de governo e item financeiro nas finanças de "
        "governos estaduais e locais, cobrindo receita, despesa, dívida e "
        "ativos. Os exercícios até 2012 vêm de um arquivo histórico publicado "
        "com uma linha por governo e 529 colunas, aqui transposto para uma "
        "linha por item; de 2013 em diante a fonte já publica nessa forma. Nem "
        "todo item_code é um item coletado: os códigos de três posições são "
        "coletados e os demais são agregados calculados pela fonte, de modo "
        "que somar todos os registros de um governo duplica valores. Exceção: "
        "não há dados para 1968 e 1969, e a fonte não publica microdados por "
        "unidade a partir de 2019.",
        "One row per government unit and finance item in state and local "
        "government finances, covering revenue, expenditure, debt and assets. "
        "Fiscal years through 2012 come from a historical archive published "
        "with one row per government and 529 columns, transposed here to one "
        "row per item; from 2013 the source already publishes it that way. Not "
        "every item_code is a collected item: three-character codes are "
        "collected and the rest are aggregates the source computes, so summing "
        "every record for one government double-counts. Exception: there is no "
        "data for 1968 and 1969, and the source publishes no unit-level "
        "microdata from 2019 on.",
        "Una fila por unidad de gobierno e ítem financiero en las finanzas de "
        "gobiernos estatales y locales, cubriendo ingresos, gastos, deuda y "
        "activos. Los ejercicios hasta 2012 vienen de un archivo histórico "
        "publicado con una fila por gobierno y 529 columnas, transpuesto aquí a "
        "una fila por ítem; desde 2013 la fuente ya publica en esa forma. No "
        "todo item_code es un ítem recolectado: los códigos de tres posiciones "
        "son recolectados y los demás son agregados calculados por la fuente, "
        "de modo que sumar todos los registros de un gobierno duplica valores. "
        "Excepción: no hay datos para 1968 y 1969, y la fuente no publica "
        "microdatos por unidad a partir de 2019.",
    ),
    "finance_unit": (
        "Uma linha por unidade de governo em cada exercício fiscal das "
        "finanças de governos estaduais e locais, com nome, localização, "
        "população, fim do exercício e peso amostral. Complementa a tabela "
        "finance, que traz os valores por item financeiro.",
        "One row per government unit in each fiscal year of state and local "
        "government finances, with name, location, population, fiscal year end "
        "and sampling weight. It complements the finance table, which carries "
        "the values by finance item.",
        "Una fila por unidad de gobierno en cada ejercicio fiscal de las "
        "finanzas de gobiernos estatales y locales, con nombre, ubicación, "
        "población, fin del ejercicio y peso muestral. Complementa la tabla "
        "finance, que trae los valores por ítem financiero.",
    ),
    "dicionario": (
        "Correspondência entre os valores codificados das colunas categóricas "
        "do conjunto e sua descrição, cobrindo tipo de governo, categoria "
        "funcional do emprego, item financeiro, marcadores de qualidade, nível "
        "de ensino e região censitária. Alguns códigos aparecem nos dados mas "
        "em nenhuma lista que o Census Bureau ainda publique; o dicionário "
        "registra o que se sabe sobre eles em vez de inventar um rótulo.",
        "Correspondence between the coded values of the dataset's categorical "
        "columns and their description, covering type of government, employment "
        "functional category, finance item, quality flags, school level and "
        "census region. A few codes appear in the data but in no list the "
        "Census Bureau still publishes; the dictionary records what is known "
        "about them rather than inventing a label.",
        "Correspondencia entre los valores codificados de las columnas "
        "categóricas del conjunto y su descripción, cubriendo tipo de gobierno, "
        "categoría funcional del empleo, ítem financiero, marcadores de "
        "calidad, nivel de enseñanza y región censal. Algunos códigos aparecen "
        "en los datos pero en ninguna lista que el Census Bureau siga "
        "publicando; el diccionario registra lo que se sabe sobre ellos en vez "
        "de inventar una etiqueta.",
    ),
}

# The tag vocabulary is not the same in the two environments: staging carries
# the Portuguese legacy slugs and production the English ones. The subject is
# identical, so the two lists are the same nine tags under each backend's own
# spelling. "governo" is deliberately absent — it would restate the government
# theme the dataset already carries.
TAGS = {
    "staging": [
        "administracao_publica",
        "financas_publicas",
        "emprego",
        "salario",
        "servidor",
        "despesa",
        "receita",
        "divida",
        "imposto",
    ],
    "prod": [
        "public_administration",
        "public-finance",
        "employment",
        "salary",
        "public_servant",
        "expenditure",
        "revenue",
        "debt",
        "tax",
    ],
}


def load_ids() -> dict:
    """Read the record ids written by previous runs."""
    return json.loads(IDS.read_text()) if IDS.exists() else {}


def save_ids(store: dict) -> None:
    """Persist the record ids so a re-run updates rather than duplicates."""
    IDS.write_text(json.dumps(store, indent=2, sort_keys=True) + "\n")


def columns_payload(table: str) -> str:
    """Build the bulk_upsert_columns payload from the architecture CSV."""
    import csv

    path = ARCHITECTURE / f"sheet_{table}.csv"
    with path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    payload = []
    for row in rows:
        entry = {
            "name": row["name"],
            "bigquery_type": row["bigquery_type"],
            "description_pt": row["description_pt"],
            "description_en": row["description_en"],
            "description_es": row["description_es"],
            "covered_by_dictionary": row["covered_by_dictionary"],
            "has_sensitive_data": row["has_sensitive_data"],
        }
        for key, column in (
            ("temporal_coverage", "temporal_coverage"),
            ("directory_column", "directory_column"),
            ("measurement_unit", "measurement_unit"),
            ("observations_pt", "observations_pt"),
            ("observations_en", "observations_en"),
            ("observations_es", "observations_es"),
        ):
            if row[column]:
                entry[key] = row[column]
        payload.append(entry)
    return json.dumps(payload, ensure_ascii=False)


def main(env: str) -> None:
    """Register or update every record for the dataset."""
    store = load_ids().setdefault(env, {})
    full = load_ids()
    full[env] = store

    refs = server.discover_ids(
        env=env,
        keys=[
            "status",
            "license",
            "availability",
            "language",
            "entity",
            "tag",
            "theme",
            "organization",
        ],
    )
    status = refs["status"]
    account = server.get_authenticated_account(env=env)["id"]
    area = server.lookup_id(category="area", slug=AREA, env=env)["id"]

    existing = server.get_dataset(slug=SLUG, env=env)
    dataset_id = existing["id"]
    print(f"dataset {SLUG} -> {dataset_id}")

    server.create_update_dataset(
        slug=SLUG,
        name_pt="Censo de Governos (CoG)",
        name_en="Census of Governments (CoG)",
        name_es="Censo de Gobiernos (CoG)",
        description_pt=DATASET_DESCRIPTION[0],
        description_en=DATASET_DESCRIPTION[1],
        description_es=DATASET_DESCRIPTION[2],
        organization_ids=[o["id"] for o in existing["organizations"]],
        theme_ids=[t["id"] for t in existing["themes"]],
        tag_ids=[refs["tag"][t] for t in TAGS[env]],
        status_id=status["published" if env == "staging" else "under_review"],
        id=dataset_id,
        env=env,
    )
    print("  dataset updated")

    sources = store.setdefault("raw_sources", {})
    # The shell carried one generic "Dados originais" source; it becomes the
    # program landing page rather than being left beside the real ones.
    generic = server.get_raw_data_sources(dataset_slug=SLUG, env=env)
    if "landing" not in sources and generic:
        sources["landing"] = generic[0]["id"]
    for key, spec in RAW_SOURCES.items():
        result = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=spec["name"][0],
            name_en=spec["name"][1],
            name_es=spec["name"][2],
            url=spec["url"],
            license_id=refs["license"]["cc0"],
            availability_id=refs["availability"]["online"],
            description_pt=spec["description"][0],
            description_en=spec["description"][1],
            description_es=spec["description"][2],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            language_ids=[refs["language"]["en"]],
            id=sources.get(key),
            env=env,
        )
        sources[key] = result.get("id", sources.get(key))
        print(f"  raw source {key} -> {sources[key]}")
    save_ids(full)

    tables = store.setdefault("tables", {})
    for slug, spec in TABLES.items():
        record = tables.setdefault(slug, {})
        # create_update_table sends a coverages_areas field the backend's
        # TableForm does not accept, so it fails outright once the table has a
        # coverage. A first run therefore writes the table, then the coverage;
        # a re-run can no longer touch the table's own fields and says so
        # instead of dying. Recreate the table to change a name or description.
        if record.get("coverage"):
            print(f"table {slug} -> {record['id']} (fields not rewritable)")
            table_id = record["id"]
            table = {"id": table_id}
        else:
            table = server.create_update_table(
                slug=slug,
                name_pt=spec["name"][0],
                name_en=spec["name"][1],
                name_es=spec["name"][2],
                description_pt=TABLE_DESCRIPTIONS[slug][0],
                description_en=TABLE_DESCRIPTIONS[slug][1],
                description_es=TABLE_DESCRIPTIONS[slug][2],
                dataset_id=dataset_id,
                status_id=status["published"],
                published_by_ids=[account],
                data_cleaned_by_ids=[account],
                raw_data_source_ids=(
                    [sources[spec["source"]]] if spec["source"] else []
                ),
                auxiliary_files_url=(
                    f"{AUXILIARY_FILES}/{slug}/auxiliary_files.zip"
                    if spec["source"]
                    else ""
                ),
                id=record.get("id"),
                env=env,
            )
            record["id"] = table.get("id", record.get("id"))
            table_id = record["id"]
            print(f"table {slug} -> {table_id}")

        levels = record.setdefault("levels", {})
        for entity in spec["levels"]:
            result = server.create_update_observation_level(
                table_id=table_id,
                entity_id=refs["entity"][entity],
                id=levels.get(entity),
                env=env,
            )
            levels[entity] = result.get("id", levels.get(entity))
        if spec["levels"]:
            server.reorder_observation_levels(
                table_id=table_id,
                ol_ids=[levels[e] for e in spec["levels"]],
                env=env,
            )
        save_ids(full)

        server.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_payload(slug), env=env
        )
        print(f"  {len(load_cols(slug))} columns")

        cloud = server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=(
                "basedosdados-dev" if env == "staging" else "basedosdados"
            ),
            gcp_dataset_id=DATASET_ID,
            gcp_table_id=slug,
            id=record.get("cloud_table"),
            env=env,
        )
        record["cloud_table"] = cloud.get("id", record.get("cloud_table"))

        if spec["years"]:
            coverage = server.create_update_coverage(
                table_id=table_id,
                area_id=area,
                id=record.get("coverage"),
                env=env,
            )
            record["coverage"] = coverage.get("id", record.get("coverage"))
            start, end = spec["years"]
            span = server.create_update_datetime_range(
                coverage_id=record["coverage"],
                start_year=start,
                end_year=end,
                interval=1,
                id=record.get("datetime_range"),
                env=env,
            )
            record["datetime_range"] = span.get(
                "id", record.get("datetime_range")
            )
            update = server.create_update_update(
                entity_id=refs["entity"]["year"],
                frequency=1,
                latest=datetime.now(UTC).replace(microsecond=0).isoformat(),
                table_id=table_id,
                id=record.get("update"),
                env=env,
            )
            record["update"] = update.get("id", record.get("update"))
        save_ids(full)

    server.reorder_tables(dataset_slug=SLUG, table_slugs=TABLE_ORDER, env=env)
    save_ids(full)
    link_columns(env, store, refs)
    print("done")


# Which column identifies which observation level. Without these links the site
# renders the level's columns as "Não informado"; bulk_upsert_columns does not
# set them, so each needs its own update_column call. The boolean arguments of
# update_column default to False, so is_partition is re-passed on year.
LEVEL_COLUMNS = {
    "government_unit": {
        "year": "year",
        "government_id": "agency",
        "government_id_govs": "agency",
    },
    "employment": {
        "year": "year",
        "government_id": "agency",
        "government_id_govs": "agency",
        "function_code": "item",
    },
    "employment_unit": {
        "year": "year",
        "government_id": "agency",
        "government_id_govs": "agency",
    },
    "finance": {
        "year": "year",
        "government_id": "agency",
        "government_id_govs": "agency",
        "item_code": "item",
    },
    "finance_unit": {
        "year": "year",
        "government_id": "agency",
        "government_id_govs": "agency",
    },
}


def link_columns(env: str, store: dict, refs: dict) -> None:
    """Flag the partition column and link each grain column to its level."""
    for slug, mapping in LEVEL_COLUMNS.items():
        table_id = store["tables"][slug]["id"]
        levels = store["tables"][slug]["levels"]
        columns = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=SLUG, env=env)["tables"][slug][
                "columns"
            ]
        }
        for column, entity in mapping.items():
            if column not in columns:
                raise SystemExit(f"{slug}: no column {column}")
            server.update_column(
                column_id=columns[column],
                column_name=column,
                table_id=table_id,
                observation_level_id=levels[entity],
                is_partition=(column == "year"),
                env=env,
            )
        print(f"  {slug}: linked {len(mapping)} columns to their levels")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "staging")
