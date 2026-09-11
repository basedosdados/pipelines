"""Register br_cgu_despesas_publicas metadata in the Data Basis backend.

Usage:
    uv run python models/br_cgu_despesas_publicas/code/register_metadata.py \
        --env staging [--apply]

Without ``--apply`` it prints the current state and exits, so a run can always
be previewed first.

**Idempotency is the whole point of the structure here.** ``create_update_*`` is
idempotent for dataset/table/column (they match on slug or name) but NOT for a
table's child records: observation levels, cloud tables, coverages, datetime
ranges and Updates each create a brand-new row whenever ``id`` is omitted. A
second run without id reuse silently multiplies them, and once a table has more
than one coverage carrying a datetime range, ``create_update_table`` starts
failing with ``'TableForm' has no field named 'coverages_areas'``. So this
script reads the current state first and passes every existing id back.

Order matters too: the table (including the raw-source link) is written before
any coverage exists, for the same reason.
"""

import argparse
import json
import os
import sys
from pathlib import Path

# The databasis MCP server module is not a package dependency — it lives in its
# own repo and is imported by path so this script uses exactly the same tools
# and credentials the MCP does. Override with DATABASIS_MCP_PATH.
sys.path.insert(
    0,
    os.environ.get(
        "DATABASIS_MCP_PATH", str(Path.home() / "Dropbox" / "BD" / "mcp")
    ),
)

# pyrefly: ignore [missing-import]
import server

CODE = Path(__file__).resolve().parent
DATASET_SLUG = "despesas_publicas"
DATASET_ID = "ef31c5c6-452f-4eaa-8d61-128b45e65823"
TABLE_SLUG = "execucao"
GCP_DATASET_ID = "br_cgu_despesas_publicas"

# Same UUIDs on staging and prod — verified 2026-09-11.
ST_PUBLISHED = "e16221de-ac30-4926-83d3-de219998dab3"
ST_UNDER_REVIEW = "47208305-325a-4da9-9222-ac6849405b78"
LIC_UNKNOWN = "77dfe32b-6a14-4490-806f-22af1f26c425"
AVAIL_ONLINE = "dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe"
AREA_BR = "5503dd29-4d9b-483b-ae09-63dc8ed28875"
ENT = {
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "month": "f9659fea-e9bb-4177-9ca0-54076a8c0932",
    "agency": "24326cfe-d061-4e4c-86be-dbcd0d8943ce",
    "expenditure": "5e4f445b-02e4-4eda-b06e-8d16fe2a8741",
}
ACCOUNT = {"staging": "57", "prod": "4"}

# Kept as an explicit literal: get_dataset returns no name keys, so reading the
# "current" values back from it silently blanks them on the next write.
TABLE_NAMES = {
    "name_pt": "Execução da Despesa",
    "name_en": "Expenditure Execution",
    "name_es": "Ejecución del Gasto",
}
TABLE_DESC = {
    "description_pt": (
        "Execução mensal da despesa do Governo Federal nos orçamentos fiscal e da "
        "seguridade social: valores empenhado, liquidado e pago do exercício e a "
        "movimentação de restos a pagar, detalhados por órgão, unidade gestora, "
        "unidade orçamentária, função, subfunção, programa, ação, plano orçamentário, "
        "localizador e natureza da despesa. Cada linha é um cruzamento dessas "
        "dimensões em um mês. Fonte: Portal da Transparência, Controladoria-Geral da "
        "União."
    ),
    "description_en": (
        "Monthly execution of Brazilian federal expenditure under the fiscal and "
        "social security budgets: amounts committed, verified and paid within the "
        "year, plus movement of commitments carried over from previous years, broken "
        "down by agency, managing unit, budget unit, function, subfunction, "
        "programme, action, budget plan, localiser and expenditure nature. Each row "
        "is one combination of those dimensions in a month. Source: Transparency "
        "Portal, Office of the Comptroller General of Brazil."
    ),
    "description_es": (
        "Ejecución mensual del gasto del Gobierno Federal brasileño en los "
        "presupuestos fiscal y de seguridad social: valores comprometido, liquidado y "
        "pagado del ejercicio y el movimiento de residuos por pagar, detallados por "
        "órgano, unidad gestora, unidad presupuestaria, función, subfunción, "
        "programa, acción, plan presupuestario, localizador y naturaleza del gasto. "
        "Fuente: Portal da Transparência, Contraloría General de la Unión."
    ),
}

RAW_NAME = "Portal da Transparência — Execução da Despesa"

# Column -> observation level. Exactly the grain columns, nothing else: an
# unlinked level renders as "Não informado" on the site.
OL_FOR_COLUMN = {
    "ano": "year",
    "mes": "month",
    "id_orgao_superior": "agency",
    "id_orgao_subordinado": "agency",
    "id_unidade_gestora": "agency",
    "id_unidade_orcamentaria": "agency",
    "id_funcao": "expenditure",
    "id_subfuncao": "expenditure",
    "id_programa_orcamentario": "expenditure",
    "id_acao": "expenditure",
    "id_plano_orcamentario": "expenditure",
    "id_categoria_economica": "expenditure",
    "id_grupo_despesa": "expenditure",
    "id_elemento_despesa": "expenditure",
    "id_modalidade_despesa": "expenditure",
}
PARTITION_COLUMNS = {"ano", "mes"}


def read_state(env: str) -> dict:
    q = """query($slug:String!){ allDataset(slug:$slug){ edges{ node{ id
      tables{ edges{ node{ id slug
        rawDataSource{ edges{ node{ id namePt } } }
        cloudTables{ edges{ node{ id } } }
        coverages{ edges{ node{ id isClosed
          datetimeRanges{ edges{ node{ id } } } } } }
        observationLevels{ edges{ node{ id entity{ slug } } } }
        updates{ edges{ node{ id entity{ slug } } } }
        columns{ edges{ node{ id name } } } } } } } } } }"""
    r = server._gql(q, {"slug": DATASET_SLUG}, env=env)
    ds = r["allDataset"]["edges"][0]["node"]
    tables = {e["node"]["slug"]: e["node"] for e in ds["tables"]["edges"]}
    t = tables.get(TABLE_SLUG)
    strip = server._strip_id
    if t is None:
        return {
            "table_id": None,
            "raw": {},
            "cloud": None,
            "coverages": [],
            "levels": {},
            "updates": {},
            "columns": {},
        }
    cov = []
    for e in t["coverages"]["edges"]:
        n = e["node"]
        cov.append(
            {
                "id": strip(n["id"]),
                "is_closed": n["isClosed"],
                "range_id": (
                    strip(n["datetimeRanges"]["edges"][0]["node"]["id"])
                    if n["datetimeRanges"]["edges"]
                    else None
                ),
            }
        )
    return {
        "table_id": strip(t["id"]),
        "raw": {
            e["node"]["namePt"]: strip(e["node"]["id"])
            for e in t["rawDataSource"]["edges"]
        },
        "cloud": strip(t["cloudTables"]["edges"][0]["node"]["id"])
        if t["cloudTables"]["edges"]
        else None,
        "coverages": cov,
        "levels": {
            e["node"]["entity"]["slug"]: strip(e["node"]["id"])
            for e in t["observationLevels"]["edges"]
        },
        "updates": {
            e["node"]["entity"]["slug"]: strip(e["node"]["id"])
            for e in t["updates"]["edges"]
        },
        "columns": {
            e["node"]["name"]: strip(e["node"]["id"])
            for e in t["columns"]["edges"]
        },
    }


def _id(result) -> str:
    for key in ("id",):
        if isinstance(result, dict) and key in result:
            return server._strip_id(result[key])
    for v in (result or {}).values():
        if isinstance(v, dict) and "id" in v:
            return server._strip_id(v["id"])
    raise RuntimeError(f"no id in {result!r}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging", choices=["staging", "prod"])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--publish",
        action="store_true",
        help="flip the dataset to published (dev/staging only pre-merge)",
    )
    ap.add_argument(
        "--free-end",
        default="2026-03",
        help="last free month, YYYY-MM; pro starts the month after",
    )
    ap.add_argument(
        "--max-period",
        default="2026-09",
        help="last month present in the table, YYYY-MM",
    )
    args = ap.parse_args()
    env = args.env

    st = read_state(env)
    print(
        json.dumps(
            {
                k: (len(v) if isinstance(v, (dict, list)) else v)
                for k, v in st.items()
            },
            indent=1,
        )
    )
    if not args.apply:
        print("\n[dry run] pass --apply to write")
        return

    account = ACCOUNT[env]

    raw = server.create_update_raw_data_source(
        dataset_id=DATASET_ID,
        name_pt=RAW_NAME,
        name_en="Transparency Portal — Expenditure Execution",
        name_es="Portal da Transparência — Ejecución del Gasto",
        url="https://portaldatransparencia.gov.br/download-de-dados/despesas-execucao",
        license_id=LIC_UNKNOWN,
        availability_id=AVAIL_ONLINE,
        description_pt="Arquivos CSV mensais de execução da despesa federal, um ZIP por mês desde janeiro de 2014",
        description_en="Monthly CSV files of federal expenditure execution, one ZIP per month since January 2014",
        description_es="Archivos CSV mensuales de ejecución del gasto federal, un ZIP por mes desde enero de 2014",
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        id=st["raw"].get(RAW_NAME),
        env=env,
    )
    raw_id = _id(raw)
    print("raw source:", raw_id)

    table = server.create_update_table(
        slug=TABLE_SLUG,
        dataset_id=DATASET_ID,
        status_id=ST_PUBLISHED,
        published_by_ids=[account],
        data_cleaned_by_ids=[account],
        raw_data_source_ids=[raw_id],
        id=st["table_id"],
        env=env,
        **TABLE_NAMES,
        **TABLE_DESC,
    )
    table_id = _id(table)
    print("table:", table_id)

    payload = json.loads((CODE / "columns_json" / "execucao.json").read_text())
    print(
        "columns:",
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        ),
    )

    st = read_state(env)
    ol_ids = {}
    for slug in ("year", "month", "agency", "expenditure"):
        ol_ids[slug] = _id(
            server.create_update_observation_level(
                table_id=table_id,
                entity_id=ENT[slug],
                id=st["levels"].get(slug),
                env=env,
            )
        )
        print("OL", slug, ol_ids[slug])

    st = read_state(env)
    for col, slug in OL_FOR_COLUMN.items():
        cid = st["columns"].get(col)
        if not cid:
            print("  !! missing column", col)
            continue
        # update_column's booleans default to False, so is_partition has to be
        # re-passed here or the flag set earlier is silently cleared.
        server.update_column(
            column_id=cid,
            column_name=col,
            table_id=table_id,
            observation_level_id=ol_ids[slug],
            is_partition=col in PARTITION_COLUMNS,
            env=env,
        )
    print("linked", len(OL_FOR_COLUMN), "columns to observation levels")

    gcp_project = "basedosdados-dev" if env == "staging" else "basedosdados"
    print(
        "cloud:",
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=TABLE_SLUG,
            id=st["cloud"],
            env=env,
        ),
    )

    # PartBdpro needs BOTH a free and a pro Coverage to exist, or the pipeline's
    # assert_coverage_topology hard-fails before writing anything. is_closed is
    # set on the Coverage AND its DateTimeRange; the pipeline never writes it.
    free_y, free_m = (int(x) for x in args.free_end.split("-"))
    max_y, max_m = (int(x) for x in args.max_period.split("-"))
    # Pro starts the month AFTER free_end, so the two ranges never overlap:
    # the RAP grants allUsers `date <= free_end`, which is inclusive.
    pro_y, pro_m = (free_y + 1, 1) if free_m == 12 else (free_y, free_m + 1)
    if (pro_y, pro_m) > (max_y, max_m):
        raise SystemExit(
            f"free_end {args.free_end} leaves no pro window before {args.max_period}"
        )
    existing = {c["is_closed"]: c for c in st["coverages"]}
    for is_closed, (sy, sm, ey, em) in {
        False: (2014, 1, free_y, free_m),
        True: (pro_y, pro_m, max_y, max_m),
    }.items():
        prev = existing.get(is_closed)
        cov_id = _id(
            server.create_update_coverage(
                table_id=table_id,
                area_id=AREA_BR,
                is_closed=is_closed,
                id=prev["id"] if prev else None,
                env=env,
            )
        )
        rng = server.create_update_datetime_range(
            coverage_id=cov_id,
            start_year=sy,
            start_month=sm,
            end_year=ey,
            end_month=em,
            interval=1,
            is_closed=is_closed,
            id=prev["range_id"] if prev else None,
            env=env,
        )
        print(
            f"coverage is_closed={is_closed}: {cov_id} range {sy}-{sm:02d}..{ey}-{em:02d} {rng}"
        )

    print(
        "update:",
        server.create_update_update(
            entity_id=ENT["month"],
            frequency=1,
            lag=1,
            latest=f"{max_y:04d}-{max_m:02d}-01T00:00:00",
            table_id=table_id,
            id=st["updates"].get("month"),
            env=env,
        ),
    )

    if args.publish:
        print(
            "publish:",
            server.create_update_dataset(
                slug=DATASET_SLUG,
                name_pt="Despesas Públicas",
                name_en="Public Expenditures",
                name_es="Gastos Públicos",
                organization_ids=["b5de5696-57b0-4d79-9bce-35d0861464db"],
                theme_ids=["6dd730bb-89ab-4dba-a1bf-a25ca1c35003"],
                tag_ids=[
                    "8648b1da-a80b-4eaf-89be-76dbe1e9d102",
                    "2195dbbf-7f5f-437c-a71e-e1aab0ac2337",
                    "83b37841-83b0-45e7-9455-b7b1008f1e30",
                ],
                status_id=ST_PUBLISHED,
                id=DATASET_ID,
                env=env,
            ),
        )

    print(
        "\nAFTER:",
        json.dumps(
            {
                k: (len(v) if isinstance(v, (dict, list)) else v)
                for k, v in read_state(env).items()
            },
            indent=1,
        ),
    )


if __name__ == "__main__":
    main()
