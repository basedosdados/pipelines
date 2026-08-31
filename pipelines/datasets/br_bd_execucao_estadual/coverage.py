"""Per-state coverage refresh for br_bd_execucao_estadual.

This dataset does NOT use `register_table_materialization_task`, and the reason is
structural rather than a preference.

Every table here is a union of states with different spans, so the coverage is
registered per state: `licitacao` is Bahia 2004-01..2026-12 AND Minas Gerais
2009-01..2024-03, two `Coverage` rows on one table, both `isClosed=False`. The shared
task resolves a table's coverage with `MetadataClient._query_id`, which raises when a
filter matches more than one node -- by design, since for a normal dataset two free
coverages would be a duplicate. Four tables here legitimately have two.

So the range refresh is done directly against each state's own coverage. Nothing else
from the shared task is lost in the process: it also applies Row Access Policies and
that is only meaningful for a `PartBdpro` tier, whereas every table in this dataset is
fully free.

The state's span is re-read from BigQuery on each run rather than assumed, so a state
gaining a year updates on its own.
"""

from __future__ import annotations

from pipelines.datasets.br_bd_execucao_estadual.constants import constants

DATASET_ID = constants.DATASET_ID.value

# Which tables carry a usable month, and therefore get a month-granular range.
# Pernambuco is excluded on purpose even in `despesa`, which does have a `mes` column:
# PE fills it only for 2008-2010, and there it holds accounting periods rather than
# calendar months -- 28,517 rows use 13 (year-end close) and period 0 also occurs.
MONTHLY = {
    ("despesa", "MG"),
    ("pagamento", "PE"),
    ("despesa_mensal", "BA"),
    ("empenho_credor", "BA"),
    ("licitacao", "BA"),
    ("licitacao", "MG"),
}

# Tables with no date column at all: their coverage carries no range to refresh.
NO_RANGE = {"relacionamentos", "dicionario"}


def _coverage_id_for_area(client, table_pk: str, area_slug: str) -> str | None:
    """The id of this table's Coverage for one area.

    Every coverage on the table is fetched and matched on the area slug here, rather
    than filtered server-side: `allCoverage` exposes `area_Id` but no `area_Slug`, and
    resolving the slug to an id first would cost a second round trip to save nothing.
    A table has one or two coverages, so the list is tiny.
    """
    query = """
    query($table_Id: ID) {
      allCoverage(table_Id: $table_Id) {
        edges { node { _id area { slug } } }
      }
    }
    """
    response = client._execute(query, {"table_Id": table_pk})
    nodes = [
        n
        for n in response["allCoverage"]["items"]
        if (n.get("area") or {}).get("slug") == area_slug
    ]
    if len(nodes) > 1:
        raise ValueError(
            f"{DATASET_ID}.{table_pk}: {len(nodes)} coverages for area {area_slug}; "
            "expected one per (table, state)."
        )
    return nodes[0]["_id"] if nodes else None


def _span(
    billing_project: str,
    bq_project: str,
    table_id: str,
    state: str,
    monthly: bool,
):
    """min/max exercise for one state, and the latest real month when there is one.

    The month is taken as `max(ano * 100 + mes)`, NOT `max(mes)`. Minas Gerais'
    procurement ends in March 2024, and its busiest month across the whole series is
    December: `max(mes)` returns 12 and would advertise nine months of coverage that
    do not exist. Only the year-month pair is meaningful.

    `mes` is filtered to 1-12 rather than taken raw: Pernambuco's legacy rows carry
    accounting periods 0 and 13, and a coverage ending in month 13 is not a date.
    """
    from google.cloud import bigquery

    columns = "min(ano) lo, max(ano) hi" + (
        ", max(if(mes between 1 and 12, ano * 100 + mes, null)) ym_hi"
        if monthly
        else ""
    )
    client = bigquery.Client(project=billing_project)
    rows = list(
        client.query(
            f"select {columns} from `{bq_project}.{DATASET_ID}.{table_id}` "
            f"where sigla_uf = @uf",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("uf", "STRING", state)
                ]
            ),
        ).result()
    )
    return rows[0] if rows else None


def refresh_state_coverage(
    client, table_id: str, state: str, bq_project: str, billing_project: str
) -> str:
    """Update one (table, state) coverage range from what the table now holds."""
    from pipelines.utils.metadata.dto import DateTimeRangeInput

    if table_id in NO_RANGE:
        return f"{table_id}/{state}: no date column, nothing to refresh"

    monthly = (table_id, state) in MONTHLY
    row = _span(billing_project, bq_project, table_id, state, monthly)
    if row is None or row["lo"] is None:
        return f"{table_id}/{state}: no rows, coverage left alone"

    table_pk = client.get_table_id(DATASET_ID, table_id)
    coverage_id = _coverage_id_for_area(
        client, table_pk, f"br_{state.lower()}"
    )
    if coverage_id is None:
        raise ValueError(
            f"{DATASET_ID}.{table_id}: no coverage registered for br_{state.lower()}. "
            "Run models/br_bd_execucao_estadual/code/register_coverage.py first."
        )

    payload = {
        "coverage": coverage_id,
        "startYear": int(row["lo"]),
        "endYear": int(row["hi"]),
    }
    # The start month is left as registered: the first exercise's opening month does
    # not move, while the end month advances every refresh. The end month is only
    # meaningful when the latest month observed actually falls in the last year --
    # otherwise the final exercise carries no usable month and the range stays annual.
    if (
        monthly
        and row["ym_hi"] is not None
        and row["ym_hi"] // 100 == row["hi"]
    ):
        payload["endMonth"] = int(row["ym_hi"] % 100)
    client.upsert_coverage_datetime_range(DateTimeRangeInput(**payload))
    span = f"{row['lo']}..{row['hi']}"
    return f"{table_id}/{state}: coverage now {span}"
