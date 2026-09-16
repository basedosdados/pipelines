"""Download + cleaning transform for au_apra_superannuation (shared by the
recurring pipeline and the one-shot bootstrap in models/au_apra_superannuation/code/).

Pure functions (no Prefect) so they are importable and unit-testable. Schema and
column order come from the architecture CSVs (the single source of truth).

The source is one APRA workbook, "Quarterly superannuation performance
statistics". Each financial statement is a family of tabs — one per fund type
(Table 1=all, 2=corporate, 3=industry, 4=public sector, 5=retail) and one letter
per statement (a=financial performance, b=financial position, c=performance
ratios). Within a tab, rows are measures and columns are quarter-ends. We fold
the fund-type dimension into a ``fund_type`` row column and keep each statement as
its own wide table (one column per measure), matching how APRA structures it.
"""

import csv
import datetime
import logging
import re
from pathlib import Path

import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.au_apra_superannuation.constants import constants

log = logging.getLogger("au_apra_superannuation")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
_ARCH = constants.ARCHITECTURE_DIR.value
FUND_TABS = constants.FUND_TABS.value
STATEMENT_SUFFIX = constants.STATEMENT_SUFFIX.value
FUND_LABELS = constants.FUND_LABELS.value

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# ── measure mapping ──────────────────────────────────────────────────────────
# Per statement, the exact source row labels in source order paired with the
# output column and unit. ``None`` drops the row (kept in the list so the parser
# can validate the full source sequence and fail loudly if APRA relayouts). The
# duplicate "Inward"/"Outward" labels are disambiguated positionally.
MEASURES: dict[str, list[tuple[str, str | None, str | None]]] = {
    "financial_performance": [
        (
            "Net assets at the beginning of the period",
            "net_assets_beginning",
            "aud_million",
        ),
        ("Total contributions", "total_contributions", "aud_million"),
        ("Employer", "employer_contributions", "aud_million"),
        (
            "of which: Defined benefit contributions",
            "employer_defined_benefit_contributions",
            "aud_million",
        ),
        (
            "of which: Super guarantee contributions",
            "employer_super_guarantee_contributions",
            "aud_million",
        ),
        (
            "of which: Salary sacrifice contributions",
            "employer_salary_sacrifice_contributions",
            "aud_million",
        ),
        ("Member", "member_contributions", "aud_million"),
        (
            "Personal contributions",
            "member_personal_contributions",
            "aud_million",
        ),
        (
            "Government co-contributions",
            "government_co_contributions",
            "aud_million",
        ),
        (
            "Low income super contributions",
            "low_income_super_contributions",
            "aud_million",
        ),
        (
            "Other member contributions",
            "other_member_contributions",
            "aud_million",
        ),
        (
            "Contribution tax and surcharge",
            "contribution_tax_and_surcharge",
            "aud_million",
        ),
        ("Net benefit transfers", "net_benefit_transfers", "aud_million"),
        (
            "of which: Net rollovers to/from SMSFs",
            "net_rollovers_to_from_smsf",
            "aud_million",
        ),
        ("Inward", "benefit_transfers_inward", "aud_million"),
        ("Outward", "benefit_transfers_outward", "aud_million"),
        ("Benefit payments", "benefit_payments", "aud_million"),
        ("Lump sums", "lump_sum_benefits", "aud_million"),
        ("Pensions", "pension_benefits", "aud_million"),
        (
            "Other members' benefits flows",
            "other_members_benefit_flows",
            "aud_million",
        ),
        ("Net contribution flows", "net_contribution_flows", "aud_million"),
        ("Net insurance flows", "net_insurance_flows", "aud_million"),
        ("Inward", "insurance_flows_inward", "aud_million"),
        ("Outward", "insurance_flows_outward", "aud_million"),
        ("Investment income", "investment_income", "aud_million"),
        (
            "Investment income after impairment expense",
            "investment_income_after_impairment",
            "aud_million",
        ),
        (
            "Total gains/losses on investments",
            "total_gains_losses_on_investments",
            "aud_million",
        ),
        (
            "of which: Foreign exchange gains/ losses",
            "foreign_exchange_gains_losses",
            "aud_million",
        ),
        ("Investment expenses", "investment_expenses", "aud_million"),
        ("Operating income", "operating_income", "aud_million"),
        (
            "Administration and operating expenses",
            "administration_and_operating_expenses",
            "aud_million",
        ),
        ("Net earnings", "net_earnings", "aud_million"),
        (
            "Income tax expense/benefit",
            "income_tax_expense_benefit",
            "aud_million",
        ),
        ("Net earnings after tax", "net_earnings_after_tax", "aud_million"),
        (
            "Net operating performance after tax",
            "net_operating_performance_after_tax",
            "aud_million",
        ),
        ("Other changes", "other_changes", "aud_million"),
        (
            "Net assets at the end of the quarter",
            "net_assets_end",
            "aud_million",
        ),
        ("Number of entities", "number_of_entities", "number"),
    ],
    "financial_position": [
        ("Receivables", "receivables", "aud_million"),
        ("Investments", "investments", "aud_million"),
        # investment-vehicle composition (asset-allocation detail) — out of scope
        ("Directly managed", None, None),
        ("Individually managed mandates", None, None),
        ("Unlisted public offer unit trust", None, None),
        ("Australian wholesale trust", None, None),
        ("Australian pooled superannuation trust", None, None),
        ("Australian life company", None, None),
        ("Other investments", None, None),
        ("Directly held", None, None),
        ("Indirectly held", None, None),
        ("Cash management trust", None, None),
        ("Life company", None, None),
        ("Listed retail trust", None, None),
        ("Pooled superannuation trust", None, None),
        ("Unlisted retail trust", None, None),
        ("Wholesale trust", None, None),
        ("Other indirect investment", None, None),
        (
            "Securities purchased under agreements to resell and securities borrowed",
            "securities_purchased_under_resale_agreements",
            "aud_million",
        ),
        ("Tax assets", "tax_assets", "aud_million"),
        ("Other assets", "other_assets", "aud_million"),
        ("Total assets", "total_assets", "aud_million"),
        (
            "Securities sold under agreements to repurchase and securities loaned",
            "securities_sold_under_repurchase_agreements",
            "aud_million",
        ),
        ("Tax liabilities", "tax_liabilities", "aud_million"),
        ("Other liabilities", "other_liabilities", "aud_million"),
        ("Total liabilities", "total_liabilities", "aud_million"),
        (
            "Liability for allocated accrued benefits",
            "liability_for_allocated_accrued_benefits",
            "aud_million",
        ),
        (
            "Liability for members' benefits",
            "liability_for_members_benefits",
            "aud_million",
        ),
        (
            "Defined contribution members' benefits",
            "defined_contribution_members_benefits",
            "aud_million",
        ),
        (
            "Defined benefit members' benefits",
            "defined_benefit_members_benefits",
            "aud_million",
        ),
        ("Unallocated benefits", "unallocated_benefits", "aud_million"),
        (
            "Reserves including unallocated benefits",
            "reserves_including_unallocated_benefits",
            "aud_million",
        ),
        ("Reserves", "reserves", "aud_million"),
        (
            "Excess/deficiency of assets",
            "excess_deficiency_of_assets",
            "aud_million",
        ),
        (
            "Surplus/deficit in net assets",
            "surplus_deficit_in_net_assets",
            "aud_million",
        ),
        (
            "Net assets available to pay members' benefits",
            "net_assets_available_to_pay_benefits",
            "aud_million",
        ),
        (
            "of which: defined benefit interests",
            "defined_benefit_interests",
            "aud_million",
        ),
        ("Number of entities", "number_of_entities", "number"),
    ],
    "performance_ratios": [
        (
            "Net assets at beginning of the period ($m)",
            "net_assets_beginning",
            "aud_million",
        ),
        ("Net cash flows ($m)", "net_cash_flows", "aud_million"),
        (
            "Cash flow adjusted net assets ($m)",
            "cash_flow_adjusted_net_assets",
            "aud_million",
        ),
        ("Investment income ($m)", "investment_income", "aud_million"),
        ("Investment expense ($m)", "investment_expense", "aud_million"),
        ("Operating income ($m)", "operating_income", "aud_million"),
        (
            "Administration and operating expense ($m)",
            "administration_and_operating_expense",
            "aud_million",
        ),
        (
            "Income tax expense/benefit ($m)",
            "income_tax_expense_benefit",
            "aud_million",
        ),
        (
            "Net earnings after tax ($m)",
            "net_earnings_after_tax",
            "aud_million",
        ),
        ("Rate of Return (%)", "rate_of_return", "proportion"),
        (
            "Five year annualised Rate of Return (%)",
            "five_year_annualised_rate_of_return",
            "proportion",
        ),
        ("Number of entities", "number_of_entities", "number"),
    ],
}

# statement/measure -> BigQuery type (only number_of_entities is INT64)
_INT_COLUMNS = {"number_of_entities"}


def _norm(s) -> str:
    """Normalise a source label: collapse whitespace, strip trailing footnotes."""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s


def _as_quarter(v):
    """Return (year, quarter) if a cell reads as a quarter-end, else None."""
    if isinstance(v, (datetime.datetime, datetime.date)):
        return (v.year, (v.month - 1) // 3 + 1)
    if isinstance(v, str):
        m = re.match(r"^([A-Za-z]{3})[a-z]*\s+(\d{4})$", v.strip())
        if m and m.group(1).lower() in _MONTHS:
            mo = _MONTHS[m.group(1).lower()]
            return (int(m.group(2)), (mo - 1) // 3 + 1)
    return None


def _num(v):
    """Coerce a data cell to float, mapping blanks and confidentiality marks to None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s in ("", "*", "-", "n/a", "N/A", "..", "np", "NP"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ── download ──────────────────────────────────────────────────────────────
def discover_source_url(session: requests.Session | None = None) -> str:
    """Find the current performance-workbook .xlsx link on the APRA landing page.

    The filename embeds the coverage window and changes every quarter, so it is
    resolved from the page rather than hardcoded.

    Returns:
        Absolute URL of the latest "Quarterly superannuation performance
        statistics" workbook.

    Raises:
        RuntimeError: If no matching link is found.
    """
    sess = session or requests.Session()
    headers = {"User-Agent": constants.USER_AGENT.value}
    r = sess.get(constants.LANDING_URL.value, headers=headers, timeout=120)
    r.raise_for_status()
    pat = re.compile(
        r'href="([^"]*?' + constants.FILE_PATTERN.value + r'[^"]*?\.xlsx)"',
        re.IGNORECASE,
    )
    hrefs = pat.findall(r.text)
    if not hrefs:
        raise RuntimeError(
            "No performance-statistics .xlsx link found on landing page"
        )
    href = hrefs[0]
    return href if href.startswith("http") else constants.BASE_URL.value + href


def download_workbook(
    input_dir: Path, session: requests.Session | None = None
) -> Path:
    """Download the current performance workbook into ``input_dir``.

    Returns:
        Path to the saved .xlsx.
    """
    sess = session or requests.Session()
    headers = {"User-Agent": constants.USER_AGENT.value}
    url = discover_source_url(sess)
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / "quarterly_superannuation_performance.xlsx"
    r = sess.get(url, headers=headers, timeout=300)
    r.raise_for_status()
    dest.write_bytes(r.content)
    log.info(f"downloaded {url} -> {dest}")
    return dest


# ── schema ────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema/order source of truth."""
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


# ── parse ───────────────────────────────────────────────────────────────────
def _parse_tab(
    ws,
) -> tuple[list, list]:
    """Parse one worksheet into (quarter columns, measure rows).

    Returns:
        quarter_cols: list of (column_index, (year, quarter)).
        rows: list of (normalised_label, [cell per quarter column]).
    """
    grid = list(ws.iter_rows(values_only=True))
    # header row = the one with the most quarter-parseable cells
    hi, best = 0, -1
    for i, r in enumerate(grid):
        c = sum(1 for v in r if _as_quarter(v))
        if c > best:
            hi, best = i, c
    hdr = grid[hi]
    qcols = [
        (j, _as_quarter(hdr[j]))
        for j in range(len(hdr))
        if _as_quarter(hdr[j])
    ]
    rows = []
    for r in grid[hi + 1 :]:
        lab = r[0]
        if lab is None:
            continue
        s = _norm(lab)
        if not s:
            continue
        low = s.lower()
        if low.startswith(("*", "entities with", "(", "quarter")):
            continue
        rows.append((s, [r[j] for j, _ in qcols]))
    return qcols, rows


def _build_statement(wb, statement: str) -> pd.DataFrame:
    """Melt one statement across all fund tabs into long (year, quarter, fund_type, column, value)."""
    spec = MEASURES[statement]
    expected = [_norm(lbl) for lbl, _, _ in spec]
    suffix = STATEMENT_SUFFIX[statement]
    records = []
    for fund, num in FUND_TABS.items():
        qcols, rows = _parse_tab(wb[f"Table {num}{suffix}"])
        labels = [lbl for lbl, _ in rows]
        if labels != expected:
            raise ValueError(
                f"{statement}/{fund}: source labels drifted from mapping.\n"
                f"  expected[{len(expected)}]={expected}\n"
                f"  actual[{len(labels)}]={labels}"
            )
        for (_lbl, vals), (_, col, _unit) in zip(rows, spec, strict=True):
            if col is None:
                continue
            for (yr, qtr), cell in zip(
                (q for _, q in qcols), vals, strict=True
            ):
                v = _num(cell)
                if v is not None:
                    records.append((yr, qtr, fund, col, v))
    return pd.DataFrame(
        records, columns=["year", "quarter", "fund_type", "measure", "value"]
    )


def build_table(wb, table: str) -> pd.DataFrame:
    """Build one wide statement table: keys + one column per kept measure, in arch order."""
    long = _build_statement(wb, table)
    wide = long.pivot_table(
        index=["year", "quarter", "fund_type"],
        columns="measure",
        values="value",
        # pyrefly: ignore [bad-argument-type]
        aggfunc="first",
    ).reset_index()
    order = [a["name"] for a in read_arch(table)]
    for col in order:
        if col not in wide.columns:
            wide[col] = pd.NA
    wide = wide[order]
    # typed pre-cast so all-STRING serialization yields "375" not "375.0"
    wide["year"] = pd.to_numeric(wide["year"]).astype("int64")
    wide["quarter"] = pd.to_numeric(wide["quarter"]).astype("int64")
    for a in read_arch(table):
        if a["name"] in _INT_COLUMNS:
            wide[a["name"]] = pd.to_numeric(
                wide[a["name"]], errors="coerce"
            ).astype("Int64")
    wide = wide.sort_values(["fund_type", "year", "quarter"]).reset_index(
        drop=True
    )
    return wide


# ── write ───────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by house convention; the dbt model ``safe_cast``s each
    column to its real type. Values pass through the architecture's real types
    first (so ``year`` serialises as ``"2004"``), then arrow-cast to string —
    never ``astype(str)``, which renders NULL as the literal ``"nan"``.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed_schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    out = df[order]
    tdir = output_dir / table
    for year, g in out.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed_schema, preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


def build_dicionario(output_dir: Path) -> Path:
    """Build the ``dicionario`` table mapping the coded ``fund_type`` column to labels.

    Column names stay Portuguese (id_tabela, nome_coluna, chave, cobertura_temporal,
    valor) — the platform's dictionary renderer expects that schema — even though
    this dataset is English.
    """
    rows = [
        (t, "fund_type", code, None, label)
        for t in constants.DATA_TABLES.value
        for code, label in FUND_LABELS.items()
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    schema = pa.schema(
        [
            pa.field(a["name"], PA[a["bigquery_type"]])
            for a in read_arch("dicionario")
        ]
    )
    tdir = output_dir / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        tdir / "data.parquet",
        compression="snappy",
    )
    log.info(f"dicionario: {len(df):,} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build all tables from the workbook in ``input_dir``.

    Returns:
        Mapping of table slug to output dir, plus ``"max_year_quarter"`` — the
        latest ``"YYYY-Q"`` present, used to poll whether APRA has published a
        new quarter.
    """
    xlsx = next(input_dir.glob("*.xlsx"))
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    result: dict = {}
    max_yq = None
    for table in constants.DATA_TABLES.value:
        wide = build_table(wb, table)
        result[table] = write_partitioned(wide, table, output_dir)
        if len(wide):
            last = wide.sort_values(["year", "quarter"]).iloc[-1]
            yq = f"{int(last['year'])}-{int(last['quarter'])}"
            if max_yq is None or yq > max_yq:
                max_yq = yq
    result["dicionario"] = build_dicionario(output_dir)
    result["max_year_quarter"] = max_yq
    return result
