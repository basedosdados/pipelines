"""Pure download + cleaning functions for us_state_foreign_assistance.

No Prefect here. The one-shot onboarding scripts under
``models/us_state_foreign_assistance/code/`` and the recurring flow both
import from this module.

Source: ForeignAssistance.gov bulk files (see ``constants.py``). Two raw CSVs
become three tables:

    transaction  us_foreign_aid_complete.csv  (~4M rows, FY1946-present)
    budget       us_foreign_budget_complete.csv (~63k rows, FY2004-present)
    dicionario   labels for every coded column, built from both files

The transform runs in DuckDB (the complete file is 3.75 GB) and writes
all-STRING Snappy Parquet, one flat file per fiscal year with ``year`` kept as
a column (no hive directories), plus a 0-row ``00_header.parquet``. Staging is
all-STRING by house convention and the dbt model ``safe_cast``s every column.
Amounts are copied verbatim (integer dollar strings), dates are normalised to
ISO ``YYYY-MM-DD``, and ``year`` is rendered as ``"1976"`` rather than
``"1976.0"``.
"""

from __future__ import annotations

import csv
import shutil
from datetime import date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import duckdb
import requests

from pipelines.datasets.us_state_foreign_assistance.constants import constants

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# Architecture (single source of truth for column order)
# ---------------------------------------------------------------------------


def read_arch(table: str) -> list[dict]:
    path = (
        constants.ARCHITECTURE_DIR.value
        / f"{constants.DATASET_ID.value}__{table}.csv"
    )
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def column_order(table: str) -> list[str]:
    return [a["name"] for a in read_arch(table)]


# ---------------------------------------------------------------------------
# Source freshness + download
# ---------------------------------------------------------------------------


def source_last_modified(table: str = "transaction") -> date:
    """Return the S3 ``Last-Modified`` date of a raw file (the release date)."""
    url = f"{constants.S3_BASE.value}/{constants.FILES.value[table]}"
    r = requests.head(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return parsedate_to_datetime(r.headers["Last-Modified"]).date()


def download_file(name: str, input_dir: Path) -> Path:
    """Stream one raw file to ``input_dir``; skip when a complete copy exists."""
    input_dir.mkdir(parents=True, exist_ok=True)
    url = f"{constants.S3_BASE.value}/{name}"
    dest = input_dir / name
    head = requests.head(url, headers=HEADERS, timeout=60)
    head.raise_for_status()
    size = int(head.headers.get("Content-Length", "0"))
    if dest.exists() and size and dest.stat().st_size == size:
        return dest
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, headers=HEADERS, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    if size and tmp.stat().st_size != size:
        raise RuntimeError(
            f"{name}: downloaded {tmp.stat().st_size} B, expected {size} B"
        )
    tmp.replace(dest)
    return dest


def download_all(input_dir: Path) -> dict[str, Path]:
    return {
        t: download_file(n, input_dir)
        for t, n in constants.FILES.value.items()
    }


# ---------------------------------------------------------------------------
# Cleaning (DuckDB)
# ---------------------------------------------------------------------------

_NON_ISO = ", ".join(f"'{c}'" for c in constants.NON_ISO_CODES.value)

# Common expressions shared by the two fact tables.
_YEAR = (
    "cast(case when lower(\"Fiscal Year\") = '1976tq' then 1976 "
    'else cast("Fiscal Year" as integer) end as varchar)'
)
_ISO3 = (
    'case when length("Country ID") <= 3 '
    f'and "Country Code" not in ({_NON_ISO}) '
    'then "Country Code" end'
)


def _s(src: str) -> str:
    """Trim a source string column and turn blanks into NULL."""
    return f"nullif(trim(\"{src}\"), '')"


TRANSACTION_SELECT = f"""
select
    {_YEAR} as year,
    upper("Fiscal Year") as fiscal_period,
    strftime(try_strptime("Transaction Date", '%d%b%Y'), '%Y-%m-%d') as transaction_date,
    {_s("Transaction Type ID")} as transaction_type_id,
    {_ISO3} as country_iso3_code,
    {_s("Country ID")} as country_id,
    {_s("Country Code")} as country_code,
    {_s("Region ID")} as region_id,
    {_s("Income Group ID")} as income_group_id,
    {_s("Managing Agency ID")} as managing_agency_id,
    {_s("Managing Sub-agency or Bureau ID")} as managing_subagency_id,
    {_s("Funding Agency ID")} as funding_agency_id,
    {_s("Funding Account ID")} as funding_account_id,
    {_s("Implementing Partner ID")} as implementing_partner_id,
    {_s("Implementing Partner Name")} as implementing_partner_name,
    {_s("Implementing Partner Category ID")} as implementing_partner_category_id,
    {_s("Implementing Partner Sub-category ID")} as implementing_partner_subcategory_id,
    {_s("International Category ID")} as international_category_id,
    {_s("International Sector Code")} as international_sector_code,
    {_s("International Purpose Code")} as international_purpose_code,
    {_s("US Category ID")} as us_category_id,
    {_s("US Sector ID")} as us_sector_id,
    {_s("Foreign Assistance Objective ID")} as objective_id,
    {_s("Aid Type Group ID")} as aid_type_group_id,
    {_s("aid_type_id")} as aid_type_id,
    {_s("Activity ID")} as activity_id,
    {_s("Submission ID")} as submission_id,
    {_s("Activity Name")} as activity_name,
    {_s("Activity Description")} as activity_description,
    {_s("Activity Project Number")} as activity_project_number,
    strftime(try_cast("Activity Start Date" as date), '%Y-%m-%d') as activity_start_date,
    strftime(try_cast("Activity End Date" as date), '%Y-%m-%d') as activity_end_date,
    {_s("activity_budget_amount")} as activity_budget_amount,
    {_s("Current Dollar Amount")} as current_amount,
    {_s("Constant Dollar Amount")} as constant_amount
from raw
"""

BUDGET_SELECT = f"""
select
    {_YEAR} as year,
    {_s("Transaction Type ID")} as transaction_type_id,
    {_ISO3} as country_iso3_code,
    {_s("Country ID")} as country_id,
    {_s("Country Code")} as country_code,
    {_s("Region ID")} as region_id,
    {_s("Income Group ID")} as income_group_id,
    {_s("Managing Sub-agency or Bureau ID")} as managing_subagency_id,
    {_s("Operating Unit")} as operating_unit,
    {_s("Funding Agency ID")} as funding_agency_id,
    {_s("Funding Account ID")} as funding_account_id,
    {_s("International Category ID")} as international_category_id,
    {_s("International Sector Code")} as international_sector_code,
    {_s("International Purpose Code")} as international_purpose_code,
    {_s("US Category ID")} as us_category_id,
    {_s("US Sector ID")} as us_sector_id,
    {_s("OCO Flag")} as oco_flag,
    {_s("Activity ID")} as activity_id,
    {_s("Activity Name")} as activity_name,
    {_s("Activity Description")} as activity_description,
    {_s("current_amount")} as current_amount,
    {_s("constant_amount")} as constant_amount
from raw
"""

# (target column, source id column, label expression) per table. The label
# expression may reference any raw column of that table.
_AGENCY_LABEL = "\"{name}\" || coalesce(' (' || \"{acr}\" || ')', '')"

_DICT_TRANSACTION = [
    ("transaction_type_id", "Transaction Type ID", '"Transaction Type Name"'),
    ("country_id", "Country ID", '"Country Name"'),
    ("region_id", "Region ID", '"Region Name"'),
    (
        "income_group_id",
        "Income Group ID",
        _AGENCY_LABEL.format(
            name="Income Group Name", acr="Income Group Acronym"
        ),
    ),
    (
        "managing_agency_id",
        "Managing Agency ID",
        _AGENCY_LABEL.format(
            name="Managing Agency Name", acr="Managing Agency Acronym"
        ),
    ),
    (
        "managing_subagency_id",
        "Managing Sub-agency or Bureau ID",
        _AGENCY_LABEL.format(
            name="Managing Sub-agency or Bureau Name",
            acr="Managing Sub-agency or Bureau Acronym",
        ),
    ),
    (
        "funding_agency_id",
        "Funding Agency ID",
        _AGENCY_LABEL.format(
            name="Funding Agency Name", acr="Funding Agency Acronym"
        ),
    ),
    ("funding_account_id", "Funding Account ID", '"Funding Account Name"'),
    (
        "implementing_partner_category_id",
        "Implementing Partner Category ID",
        '"Implementing Partner Category Name"',
    ),
    (
        "implementing_partner_subcategory_id",
        "Implementing Partner Sub-category ID",
        '"Implementing Partner Sub-category Name"',
    ),
    (
        "international_category_id",
        "International Category ID",
        '"International Category Name"',
    ),
    (
        "international_sector_code",
        "International Sector Code",
        '"International Sector Name"',
    ),
    (
        "international_purpose_code",
        "International Purpose Code",
        '"International Purpose Name"',
    ),
    ("us_category_id", "US Category ID", '"US Category Name"'),
    ("us_sector_id", "US Sector ID", '"US Sector Name"'),
    (
        "objective_id",
        "Foreign Assistance Objective ID",
        '"Foreign Assistance Objective Name"',
    ),
    ("aid_type_group_id", "Aid Type Group ID", '"Aid Type Group Name"'),
    ("aid_type_id", "aid_type_id", '"aid_type_name"'),
]

_DICT_BUDGET = [
    ("transaction_type_id", "Transaction Type ID", '"Transaction Type Name"'),
    ("country_id", "Country ID", '"Country Name"'),
    ("region_id", "Region ID", '"Region Name"'),
    (
        "income_group_id",
        "Income Group ID",
        _AGENCY_LABEL.format(
            name="Income Group Name", acr="Income Group Acronym"
        ),
    ),
    (
        "managing_subagency_id",
        "Managing Sub-agency or Bureau ID",
        _AGENCY_LABEL.format(
            name="Managing Sub-agency or Bureau Name",
            acr="Managing Sub-agency or Bureau Acronym",
        ),
    ),
    (
        "funding_agency_id",
        "Funding Agency ID",
        _AGENCY_LABEL.format(
            name="Funding Agency Name", acr="Funding Agency Acronym"
        ),
    ),
    ("funding_account_id", "Funding Account ID", '"Funding Account Name"'),
    (
        "international_category_id",
        "International Category ID",
        '"International Category Name"',
    ),
    (
        "international_sector_code",
        "International Sector Code",
        '"International Sector Name"',
    ),
    (
        "international_purpose_code",
        "International Purpose Code",
        '"International Purpose Name"',
    ),
    ("us_category_id", "US Category ID", '"US Category Name"'),
    ("us_sector_id", "US Sector ID", '"US Sector Name"'),
    ("oco_flag", "OCO Flag", "'Overseas Contingency Operations'"),
]


def _scalar(
    con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
):
    row = con.execute(sql, params or []).fetchone()
    if row is None:
        raise RuntimeError(f"query returned no row: {sql[:80]}")
    return row[0]


def _connect(memory_limit: str, threads: int) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"set memory_limit='{memory_limit}'")
    con.execute(f"set threads={threads}")
    con.execute("set preserve_insertion_order=false")
    return con


def _read_raw(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(
        "create or replace table raw as select * from read_csv(?, header=true, "
        "all_varchar=true, nullstr=['NULL', ''], sample_size=-1)",
        [str(path)],
    )


def _dict_rows_sql(table: str, spec: list[tuple[str, str, str]]) -> str:
    parts = [
        f"""select '{table}' as id_tabela, '{target}' as nome_coluna,
                   nullif(trim("{src}"), '') as chave,
                   cast(null as varchar) as cobertura_temporal,
                   {label} as valor
            from raw where nullif(trim("{src}"), '') is not null
            group by all"""
        for target, src, label in spec
    ]
    return " union all ".join(parts)


def _write_year_files(
    con: duckdb.DuckDBPyConnection, view: str, table: str, output_dir: Path
) -> int:
    order = column_order(table)
    cols = ", ".join(order)
    tdir = output_dir / table
    if tdir.exists():
        shutil.rmtree(tdir)
    tdir.mkdir(parents=True)
    con.execute(
        f"copy (select {cols} from {view} limit 0) to '{tdir / '00_header.parquet'}' "
        "(format parquet, compression snappy)"
    )
    years = [
        r[0]
        for r in con.execute(
            f"select distinct year from {view} order by 1"
        ).fetchall()
    ]
    total = 0
    for y in years:
        con.execute(
            f"copy (select {cols} from {view} where year = '{y}') to "
            f"'{tdir / f'{table}_{y}.parquet'}' (format parquet, compression snappy)"
        )
        total += _scalar(
            con, f"select count(*) from {view} where year = '{y}'"
        )
    return total


def clean_all(
    input_dir: Path,
    output_dir: Path,
    memory_limit: str = "12GB",
    threads: int = 4,
) -> dict[str, int]:
    """Transform both raw CSVs into the three output tables.

    Returns the row count written per table.
    """
    con = _connect(memory_limit, threads)
    counts: dict[str, int] = {}
    dict_parts: list[str] = []

    # transaction
    _read_raw(con, input_dir / constants.FILES.value["transaction"])
    con.execute(f"create or replace table tx as {TRANSACTION_SELECT}")
    con.execute(
        f"create or replace table dict_tx as {_dict_rows_sql('transaction', _DICT_TRANSACTION)}"
    )
    counts["transaction"] = _write_year_files(
        con, "tx", "transaction", output_dir
    )
    con.execute("drop table tx")
    dict_parts.append("select * from dict_tx")

    # budget
    _read_raw(con, input_dir / constants.FILES.value["budget"])
    con.execute(f"create or replace table bg as {BUDGET_SELECT}")
    con.execute(
        f"create or replace table dict_bg as {_dict_rows_sql('budget', _DICT_BUDGET)}"
    )
    counts["budget"] = _write_year_files(con, "bg", "budget", output_dir)
    con.execute("drop table bg")
    dict_parts.append("select * from dict_bg")

    # dicionario
    con.execute(
        "create or replace table dic as select * from ("
        + " union all ".join(dict_parts)
        + ") order by id_tabela, nome_coluna, try_cast(chave as integer), chave"
    )
    dup = _scalar(
        con,
        "select count(*) from (select id_tabela, nome_coluna, chave, count(*) c "
        "from dic group by all having c > 1)",
    )
    if dup:
        raise RuntimeError(f"dicionario has {dup} duplicated keys")
    ddir = output_dir / "dicionario"
    if ddir.exists():
        shutil.rmtree(ddir)
    ddir.mkdir(parents=True)
    cols = ", ".join(column_order("dicionario"))
    con.execute(
        f"copy (select {cols} from dic) to '{ddir / 'dicionario.parquet'}' "
        "(format parquet, compression snappy)"
    )
    counts["dicionario"] = _scalar(con, "select count(*) from dic")
    con.close()
    return counts


def max_fiscal_year(output_dir: Path) -> int:
    """Largest ``year`` in the cleaned transaction output (coverage end)."""
    con = duckdb.connect()
    y = _scalar(
        con,
        "select max(cast(year as integer)) from read_parquet(?)",
        [str(output_dir / "transaction" / "transaction_*.parquet")],
    )
    con.close()
    return int(y)


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")
