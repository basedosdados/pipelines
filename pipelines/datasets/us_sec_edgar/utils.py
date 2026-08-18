"""Pure helpers for us_sec_edgar — download and cleaning.

No Prefect imports here: `models/us_sec_edgar/code/clean.py` (the one-shot
onboarding bootstrap) and `tasks.py` (the recurring pipeline) both import these,
so the cleaning transform exists in exactly one place.

Source: SEC Financial Statement Data Sets, one ZIP per calendar quarter holding
four tab-delimited files (sub, num, tag, pre). The published tables mirror them
one-to-one, stacked by release quarter (`year`, `quarter`).

Staging parquet is written **all-STRING** with the column order taken from the
architecture CSVs; the dbt models `safe_cast` each column to its real type. See
the "Staging parquet must be all-STRING" note in
`.claude/rules/prefect-pipeline-conventions.md`.
"""

import csv
import glob
import os
import re
import shutil
import time
import zipfile
from collections.abc import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from pyarrow import csv as pacsv

from pipelines.datasets.us_sec_edgar.constants import constants

# Columns holding a yyyymmdd date in the source; they become ISO strings so that
# `safe_cast(col as date)` resolves in dbt.
YYYYMMDD_COLUMNS = {
    "submission": ["period", "filed", "changed"],
    "numeric_fact": ["ddate"],
}

# Columns whose codes are resolved through the `dicionario` table. Keyed by the
# published table slug, valued by published column name.
DICTIONARY_COLUMNS = {
    "submission": [
        "filer_status",
        "well_known_seasoned_issuer",
        "fiscal_period",
        "previous_report",
        "detail",
    ],
    "tag": ["custom", "abstract", "datatype", "period_type", "balance"],
    "presentation": ["statement", "parenthetical", "render_file", "negating"],
}

# Closed code sets documented by the SEC. Values observed in the data but absent
# here fall back to `_derive_label`, so dictionary coverage is always complete.
DICTIONARY_LABELS: dict[tuple[str, str], dict[str, str]] = {
    ("submission", "filer_status"): {
        "1-LAF": "Large accelerated filer",
        "2-ACC": "Accelerated filer",
        "3-SRA": "Smaller reporting accelerated filer",
        "4-NON": "Non-accelerated filer",
        "5-SML": "Smaller reporting filer",
    },
    ("submission", "well_known_seasoned_issuer"): {
        "0": "Not a well known seasoned issuer",
        "1": "Well known seasoned issuer",
    },
    ("submission", "fiscal_period"): {
        "FY": "Fiscal year",
        "CY": "Calendar year",
        "Q1": "First fiscal quarter",
        "Q2": "Second fiscal quarter",
        "Q3": "Third fiscal quarter",
        "Q4": "Fourth fiscal quarter",
        "H1": "First fiscal half",
        "H2": "Second fiscal half",
        "T1": "First fiscal trimester",
        "T2": "Second fiscal trimester",
        "T3": "Third fiscal trimester",
        "M9": "First nine fiscal months",
    },
    ("submission", "previous_report"): {
        "0": "Not subsequently amended",
        "1": "Subsequently amended",
    },
    ("submission", "detail"): {
        "0": "Without detailed quantitative disclosures",
        "1": "With detailed quantitative disclosures",
    },
    ("tag", "custom"): {
        "0": "Standard taxonomy tag",
        "1": "Custom tag defined by the filer",
    },
    ("tag", "abstract"): {
        "0": "Represents a numeric fact",
        "1": "Does not represent a numeric fact",
    },
    ("tag", "datatype"): {
        "monetary": "Monetary amount",
        "shares": "Number of shares",
        "perShare": "Amount per share",
        "percent": "Percentage",
        "pure": "Dimensionless ratio",
        "decimal": "Decimal number",
        "integer": "Integer number",
        "perUnit": "Amount per unit",
        "mass": "Mass",
        "area": "Area",
        "volume": "Volume",
        "energy": "Energy",
        "power": "Power",
        "length": "Length",
        "duration": "Duration",
        "memory": "Memory",
        "nonNegativeInteger": "Non-negative integer",
        "positiveInteger": "Positive integer",
        "monetaryPerVolume": "Monetary amount per unit of volume",
    },
    ("tag", "period_type"): {
        "I": "Instant, a point in time",
        "D": "Duration, a span of time",
    },
    ("tag", "balance"): {"C": "Credit", "D": "Debit"},
    ("presentation", "statement"): {
        "BS": "Balance sheet",
        "IS": "Income statement",
        "CF": "Cash flow",
        "EQ": "Changes in equity",
        "CI": "Comprehensive income",
        "SI": "Schedule of investments",
        "CP": "Cover page",
        "UN": "Unclassifiable statement",
    },
    ("presentation", "parenthetical"): {
        "0": "Presented in a column of the statement",
        "1": "Presented parenthetically",
    },
    ("presentation", "render_file"): {
        "H": "Rendered as an .htm file",
        "X": "Rendered as an .xml file",
    },
    ("presentation", "negating"): {
        "0": "Preferred label does not negate the value",
        "1": "Preferred label negates the value",
    },
}


# --------------------------------------------------------------------------
# Architecture (the source of truth for column names, order and types)
# --------------------------------------------------------------------------
def architecture_columns(table: str) -> list[dict]:
    """Read a table's architecture CSV, in order."""
    path = os.path.join(constants.ARCHITECTURE_DIR.value, f"{table}.csv")
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def rename_map(table: str) -> dict[str, str]:
    """Source column name -> published column name, for source-backed columns."""
    out = {}
    for row in architecture_columns(table):
        original = row["original_name"]
        if original and not original.startswith("("):
            out[original] = row["name"]
    return out


def column_order(table: str) -> list[str]:
    return [row["name"] for row in architecture_columns(table)]


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
def make_session() -> requests.Session:
    """A session that identifies itself per the SEC's fair-access rules."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": constants.USER_AGENT.value,
            "Accept-Encoding": "gzip, deflate",
        }
    )
    return session


def list_source_quarters(
    session: requests.Session | None = None,
) -> list[tuple[int, int]]:
    """Every (year, quarter) the SEC currently publishes, oldest first."""
    session = session or make_session()
    response = session.get(constants.INDEX_URL.value, timeout=120)
    response.raise_for_status()
    found = set(
        (int(y), int(q))
        for y, q in re.findall(
            r"financial-statement-data-sets/(\d{4})q(\d)\.zip", response.text
        )
    )
    return sorted(found)


def latest_source_quarter(
    session: requests.Session | None = None,
) -> tuple[int, int]:
    quarters = list_source_quarters(session)
    if not quarters:
        raise RuntimeError("No quarterly ZIPs found on the SEC index page")
    return quarters[-1]


def download_quarter(
    year: int,
    quarter: int,
    dest_dir: str,
    session: requests.Session | None = None,
) -> str:
    """Download one quarterly ZIP; returns its path."""
    session = session or make_session()
    os.makedirs(dest_dir, exist_ok=True)
    url = constants.ZIP_URL_TEMPLATE.value.format(year=year, quarter=quarter)
    path = os.path.join(dest_dir, f"{year}q{quarter}.zip")
    time.sleep(constants.REQUEST_INTERVAL_SECONDS.value)
    with session.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        with open(path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return path


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------
def _read_tsv(handle, columns: list[str]) -> pa.Table:
    """Read one source TSV as all-string arrow.

    The files are tab-delimited with RFC4180 quoting (the `segments` and
    `footnote` fields do contain tabs), utf-8, `\\n`-terminated.
    """
    return pacsv.read_csv(
        handle,
        read_options=pacsv.ReadOptions(block_size=64 << 20),
        parse_options=pacsv.ParseOptions(
            delimiter="\t",
            quote_char='"',
            double_quote=True,
            newlines_in_values=False,
        ),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in columns},
            strings_can_be_null=True,
            null_values=[""],
        ),
    )


def _to_iso_date(array: pa.Array) -> pa.Array:
    """yyyymmdd -> YYYY-MM-DD, so `safe_cast(col as date)` resolves in dbt."""
    trimmed = pc.utf8_trim_whitespace(array)
    iso = pc.binary_join_element_wise(
        pc.utf8_slice_codeunits(trimmed, 0, 4),
        pc.utf8_slice_codeunits(trimmed, 4, 6),
        pc.utf8_slice_codeunits(trimmed, 6, 8),
        "-",
    )
    return pc.if_else(
        pc.equal(pc.utf8_length(trimmed), 8), iso, pa.scalar(None, pa.string())
    )


def _build_table(
    raw: pa.Table, table: str, year: int, quarter: int
) -> pa.Table:
    """Rename, add the release partition, coerce dates, order to architecture."""
    renames = rename_map(table)
    raw = raw.rename_columns([renames.get(c, c) for c in raw.column_names])

    for source_col in YYYYMMDD_COLUMNS.get(table, []):
        name = renames[source_col]
        raw = raw.set_column(
            raw.schema.get_field_index(name),
            name,
            _to_iso_date(raw.column(name).combine_chunks()),
        )

    n = raw.num_rows
    arrays, names = [], []
    for name in column_order(table):
        if name == "year":
            arrays.append(pa.array([str(year)] * n, type=pa.string()))
        elif name == "quarter":
            arrays.append(pa.array([str(quarter)] * n, type=pa.string()))
        else:
            arrays.append(raw.column(name).combine_chunks().cast(pa.string()))
        names.append(name)
    return pa.Table.from_arrays(arrays, names=names)


def clean_quarter(
    zip_path: str,
    year: int,
    quarter: int,
    output_dir: str,
    observed: dict[tuple[str, str], set] | None = None,
) -> dict[str, int]:
    """Turn one quarterly ZIP into partitioned all-STRING parquet.

    Writes `<output_dir>/<table>/year=<year>/quarter=<quarter>/data.parquet` and
    returns the row count per table. `observed`, when given, accumulates the
    distinct values of every dictionary-covered column so that `build_dicionario`
    can guarantee full coverage.
    """
    counts: dict[str, int] = {}
    with zipfile.ZipFile(zip_path) as archive:
        members = set(archive.namelist())
        for source_file, table in constants.SOURCE_FILES.value.items():
            if source_file not in members:
                raise FileNotFoundError(
                    f"{source_file} missing from {zip_path}"
                )
            source_columns = list(rename_map(table))
            with archive.open(source_file) as handle:
                raw = _read_tsv(handle, source_columns)
            missing = set(source_columns) - set(raw.column_names)
            if missing:
                raise ValueError(
                    f"{zip_path}:{source_file} is missing columns {sorted(missing)}"
                )
            table_arrow = _build_table(raw, table, year, quarter)
            counts[table] = table_arrow.num_rows

            if observed is not None:
                for column in DICTIONARY_COLUMNS.get(table, []):
                    values = table_arrow.column(column).unique().to_pylist()
                    observed.setdefault((table, column), set()).update(
                        v for v in values if v is not None
                    )

            part_dir = os.path.join(
                output_dir, table, f"year={year}", f"quarter={quarter}"
            )
            os.makedirs(part_dir, exist_ok=True)
            pq.write_table(
                table_arrow,
                os.path.join(part_dir, "data.parquet"),
                compression="snappy",
            )
    return counts


# --------------------------------------------------------------------------
# Dictionary
# --------------------------------------------------------------------------
def _derive_label(value: str) -> str:
    """Fallback label for a code the SEC documentation does not enumerate."""
    spaced = (
        re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value).replace("_", " ").strip()
    )
    return spaced[:1].upper() + spaced[1:] if spaced else value


def build_dicionario(
    output_dir: str, observed: dict[tuple[str, str], Iterable[str]]
) -> int:
    """Write the `dicionario` parquet covering every observed coded value."""
    rows = []
    for (table, column), values in sorted(observed.items()):
        labels = DICTIONARY_LABELS.get((table, column), {})
        for value in sorted(values):
            label = labels.get(value) or _derive_label(value)
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": value,
                    "cobertura_temporal": None,
                    "valor": label,
                }
            )
    names = column_order("dicionario")
    arrow = pa.Table.from_arrays(
        [
            pa.array([r[name] for r in rows], type=pa.string())
            for name in names
        ],
        names=names,
    )
    part_dir = os.path.join(output_dir, "dicionario")
    os.makedirs(part_dir, exist_ok=True)
    pq.write_table(
        arrow, os.path.join(part_dir, "data.parquet"), compression="snappy"
    )
    return arrow.num_rows


def observed_from_output(output_dir: str) -> dict[tuple[str, str], set]:
    """Rescan the written parquet for the distinct dictionary-covered values.

    Lets `build_dicionario` be re-run after a label change without downloading
    and cleaning every quarter again.
    """
    observed: dict[tuple[str, str], set] = {}
    for table, columns in DICTIONARY_COLUMNS.items():
        if not columns:
            continue
        pattern = os.path.join(output_dir, table, "**", "*.parquet")
        for path in sorted(glob.glob(pattern, recursive=True)):
            arrow = pq.ParquetFile(path).read(columns=columns)
            for column in columns:
                values = arrow.column(column).unique().to_pylist()
                observed.setdefault((table, column), set()).update(
                    v for v in values if v is not None
                )
    return observed


# --------------------------------------------------------------------------
# End-to-end
# --------------------------------------------------------------------------
def clean_all(
    quarters: list[tuple[int, int]],
    input_dir: str,
    output_dir: str,
    keep_zip: bool = False,
    session: requests.Session | None = None,
) -> dict[str, int]:
    """Download and clean each quarter in turn, then write the dictionary.

    One ZIP is held on disk at a time unless `keep_zip`, so peak disk stays near
    a single quarter.
    """
    session = session or make_session()
    observed: dict[tuple[str, str], set] = {}
    totals: dict[str, int] = {}
    for year, quarter in quarters:
        zip_path = os.path.join(input_dir, f"{year}q{quarter}.zip")
        if not os.path.exists(zip_path):
            zip_path = download_quarter(
                year, quarter, input_dir, session=session
            )
        counts = clean_quarter(
            zip_path, year, quarter, output_dir, observed=observed
        )
        for table, count in counts.items():
            totals[table] = totals.get(table, 0) + count
        print(
            f"{year}Q{quarter}: "
            + ", ".join(f"{t}={c:,}" for t, c in sorted(counts.items())),
            flush=True,
        )
        if not keep_zip:
            os.remove(zip_path)
    totals["dicionario"] = build_dicionario(output_dir, observed)
    return totals


def reset_partition(output_dir: str, year: int, quarter: int) -> None:
    """Drop a release quarter's parquet across all tables, for a clean re-run."""
    for table in constants.SOURCE_FILES.value.values():
        path = os.path.join(
            output_dir, table, f"year={year}", f"quarter={quarter}"
        )
        shutil.rmtree(path, ignore_errors=True)
