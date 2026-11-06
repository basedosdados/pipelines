"""Download + cleaning transform for the us_bls_cex LABSTAT layer.

Pure functions (no Prefect), shared by the future recurring pipeline and the
one-shot bootstrap in ``models/us_bls_cex/code/clean_labstat.py``. Column order
comes from the architecture CSVs, the single schema source of truth.

Staging parquet is all-STRING (see ``.claude/rules/bigquery-conventions.md``):
values are kept as BLS prints them, stripped of padding, with ``-`` and blanks
turned into NULL. Every INT64/FLOAT64 column is checked to parse before writing,
so a malformed value fails here rather than becoming a silent NULL in dbt.
"""

import csv
import logging
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_bls_cex.constants import constants

log = logging.getLogger("us_bls_cex")

_ARCH = constants.ARCHITECTURE_DIR.value


# ── download ────────────────────────────────────────────────────────────────
def download_labstat(input_dir: Path) -> Path:
    """Fetch the cx LABSTAT flat files into ``input_dir``.

    Raises:
        requests.HTTPError: If any file fails to download.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": constants.USER_AGENT.value}
    for name in constants.LABSTAT_FILES.value:
        r = requests.get(
            f"{constants.BASE_URL.value}/{name}", headers=headers, timeout=600
        )
        r.raise_for_status()
        (input_dir / name).write_bytes(r.content)
    log.info(f"downloaded {len(constants.LABSTAT_FILES.value)} files")
    return input_dir


def latest_published_year() -> int:
    """Latest year BLS has published in the cx database, via the public API.

    Raises:
        requests.HTTPError: On an HTTP failure.
        ValueError: If the API does not report success or returns no data.
    """
    r = requests.get(
        constants.API_LATEST_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=60,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(
            f"BLS API: {body.get('status')} {body.get('message')}"
        )
    data = body["Results"]["series"][0]["data"]
    if not data:
        raise ValueError("BLS API returned no data for the headline series")
    return int(data[0]["year"])


# ── schema + writing (shared with the PUMD bootstrap) ───────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV, one dict per column in order."""
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def string_schema(table: str) -> pa.Schema:
    """All-STRING arrow schema in architecture column order."""
    return pa.schema(
        [pa.field(a["name"], pa.string()) for a in read_arch(table)]
    )


def reset_dir(path: Path) -> None:
    """Delete and recreate ``path`` so no stale partition survives a rerun."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def check_numeric(at: pa.Table, table: str) -> dict[str, int]:
    """Count non-null values that do not parse as numbers, per numeric column.

    Returns:
        ``{column: n_bad}`` for INT64/FLOAT64 columns with at least one
        unparseable value. Empty when everything casts.
    """
    bad = {}
    for a in read_arch(table):
        if a["bigquery_type"] not in ("INT64", "FLOAT64"):
            continue
        col = at.column(a["name"])
        if col.null_count == len(col):
            continue
        num = pd.to_numeric(col.to_pandas(), errors="coerce")
        n = int((num.isna() & col.is_valid().to_pandas()).sum())
        if n:
            bad[a["name"]] = n
    return bad


# ── code normalization (shared by data and dicionario) ──────────────────────
# UCCs are 6-digit codes whose leading zeros are part of the code (010110).
UNPADDED_EXCEPTIONS = {"ucc"}
_DIGITS = "^[0-9]+$"


def normalize_code(value: str | None) -> str | None:
    """Drop leading zeros from an all-digit code: ``"01"`` -> ``"1"``.

    BLS writes the same code zero-padded in some years and not in others
    (``educa`` is ``06`` until 2012 and ``6`` after). Anything that is not
    purely ASCII digits is returned unchanged; ``"0"`` and ``"000"`` give ``"0"``.
    Must agree with :func:`normalize_code_array`.
    """
    if value is None:
        return None
    if value and all("0" <= ch <= "9" for ch in value):
        return str(int(value))
    return value


def normalize_code_array(arr):
    """Vectorised :func:`normalize_code` for an arrow string array."""
    digits = pc.match_substring_regex(arr, _DIGITS)  # pyrefly: ignore
    stripped = pc.replace_substring_regex(arr, "^0+([0-9])", "\\1")  # pyrefly: ignore
    return pc.if_else(digits, stripped, arr)  # pyrefly: ignore


def normalized_columns(table: str) -> list[str]:
    """Dictionary-covered columns whose all-digit values are unpadded."""
    return [
        a["name"]
        for a in read_arch(table)
        if a["covered_by_dictionary"] == "yes"
        and a["name"] not in UNPADDED_EXCEPTIONS
    ]


def to_staging(at: pa.Table, table: str) -> pa.Table:
    """Select architecture columns in order and cast every one to string.

    Columns missing from ``at`` are added as all-NULL; dictionary-covered codes
    are unpadded by :func:`normalize_code_array`. The cast runs in arrow,
    so a NULL stays NULL (``astype(str)`` would write the literal ``"nan"``).
    """
    schema = string_schema(table)
    norm = set(normalized_columns(table))
    cols = []
    for f in schema:
        if f.name in at.column_names:
            c = pc.cast(at.column(f.name), pa.string())
            cols.append(normalize_code_array(c) if f.name in norm else c)
        else:
            cols.append(pa.nulls(len(at), pa.string()))
    return pa.Table.from_arrays(cols, schema=schema)


def write_table(
    at: pa.Table, table: str, output_dir: Path, partition: str | None = None
) -> Path:
    """Write ``at`` as all-STRING snappy parquet, optionally hive-partitioned.

    The table directory is deleted first. Unpartitioned tables land in
    ``<output_dir>/<table>/data.parquet``; partitioned ones in
    ``<output_dir>/<table>/<partition>=<value>/data.parquet``.
    """
    at = to_staging(at, table)
    bad = check_numeric(at, table)
    if bad:
        raise ValueError(f"{table}: unparseable numeric values {bad}")
    tdir = output_dir / table
    reset_dir(tdir)
    if partition is None:
        pq.write_table(at, tdir / "data.parquet", compression="snappy")
    else:
        keys = pc.unique(at.column(partition)).to_pylist()  # pyrefly: ignore
        for k in sorted(keys):
            sub = at.filter(pc.equal(at.column(partition), k))  # pyrefly: ignore
            pdir = tdir / f"{partition}={k}"
            pdir.mkdir()
            pq.write_table(sub, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {at.num_rows:,} rows -> {tdir}")
    return tdir


# ── reading ─────────────────────────────────────────────────────────────────
def read_labstat(path: Path) -> pa.Table:
    """Read a tab-delimited LABSTAT file as all-string, whitespace-stripped.

    LABSTAT right-pads ``series_id`` and left-pads ``value``; both are stripped.
    Header names are stripped too. Empty strings and BLS's ``-`` placeholder
    become NULL.
    """
    with open(path, encoding="latin-1") as fh:
        names = [h.strip() for h in fh.readline().rstrip("\r\n").split("\t")]
    at = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            column_names=names, skip_rows=1, encoding="latin-1"
        ),
        parse_options=pacsv.ParseOptions(delimiter="\t", quote_char=False),
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in names},
            strings_can_be_null=False,
        ),
    )
    cols = []
    for n in names:
        c = pc.utf8_trim_whitespace(at.column(n))  # pyrefly: ignore
        empty = pc.or_(pc.equal(c, ""), pc.equal(c, "-"))  # pyrefly: ignore
        cols.append(pc.if_else(empty, pa.scalar(None, pa.string()), c))  # pyrefly: ignore
    return pa.Table.from_arrays(cols, names=names)


def _pandas(path: Path) -> pd.DataFrame:
    return read_labstat(path).to_pandas()


# ── transform ───────────────────────────────────────────────────────────────
def build_series(input_dir: Path) -> pa.Table:
    """One row per LABSTAT series with its category, item and demographic labels.

    Raises:
        ValueError: If any series fails to match a dimension row.
    """
    s = _pandas(input_dir / "cx.series")
    cat = _pandas(input_dir / "cx.category")[
        ["category_code", "category_text"]
    ]
    sub = _pandas(input_dir / "cx.subcategory")[
        ["subcategory_code", "subcategory_text"]
    ]
    item = _pandas(input_dir / "cx.item")[
        ["subcategory_code", "item_code", "item_text", "display_level"]
    ]
    dem = _pandas(input_dir / "cx.demographics")[
        ["demographics_code", "demographics_text"]
    ]
    char = _pandas(input_dir / "cx.characteristics")[
        ["demographics_code", "characteristics_code", "characteristics_text"]
    ]
    n = len(s)
    df = (
        s.merge(cat, on="category_code", how="left", validate="many_to_one")
        .merge(sub, on="subcategory_code", how="left", validate="many_to_one")
        .merge(
            item,
            on=["subcategory_code", "item_code"],
            how="left",
            validate="many_to_one",
        )
        .merge(dem, on="demographics_code", how="left", validate="many_to_one")
        .merge(
            char,
            on=["demographics_code", "characteristics_code"],
            how="left",
            validate="many_to_one",
        )
    )
    assert len(df) == n
    for c in [
        "category_text",
        "subcategory_text",
        "item_text",
        "demographics_text",
        "characteristics_text",
    ]:
        miss = df[c].isna().sum()
        if miss:
            raise ValueError(f"series: {miss} rows without {c}")
    df = df.rename(
        columns={
            "category_code": "category_id",
            "category_text": "category_name",
            "subcategory_code": "subcategory_id",
            "subcategory_text": "subcategory_name",
            "item_code": "item_id",
            "item_text": "item_name",
            "display_level": "item_display_level",
            "demographics_code": "demographics_id",
            "demographics_text": "demographics_name",
            "characteristics_code": "characteristics_id",
            "characteristics_text": "characteristics_name",
            "process_code": "statistic",
        }
    )
    df = df.sort_values("series_id", kind="stable")
    return pa.Table.from_pandas(df, preserve_index=False)


def build_annual(input_dir: Path) -> pa.Table:
    """Published means left-joined with their aspects, pivoted wide.

    Raises:
        ValueError: If a period other than ``A01`` appears, if an aspect key
            is duplicated, or if the join changes the row count.
    """
    data = read_labstat(input_dir / "cx.data.1.AllData")
    periods = set(pc.unique(data.column("period")).to_pylist())  # pyrefly: ignore
    if periods != {"A01"}:
        raise ValueError(f"annual: unexpected periods {periods}")
    data = data.rename_columns(
        ["series_id", "year", "period", "mean", "footnote_codes"]
    )
    n = data.num_rows
    keys = ["series_id", "year", "period"]
    if data.group_by(keys).aggregate([]).num_rows != n:
        raise ValueError("annual: duplicate (series_id, year, period)")

    asp = read_labstat(input_dir / "cx.aspect").select(
        ["series_id", "year", "period", "aspect_type", "value"]
    )
    types = pc.value_counts(asp.column("aspect_type")).to_pylist()  # pyrefly: ignore
    log.info(f"aspect types: {types}")
    unknown = {t["values"] for t in types} - set(
        constants.ASPECT_COLUMNS.value
    )
    if unknown:
        raise ValueError(f"annual: unmapped aspect types {unknown}")
    out = data
    for code, name in constants.ASPECT_COLUMNS.value.items():
        a = asp.filter(pc.equal(asp.column("aspect_type"), code)).select(  # pyrefly: ignore
            [*keys, "value"]
        )
        if a.group_by(keys).aggregate([]).num_rows != a.num_rows:
            raise ValueError(f"annual: duplicate aspect {code} keys")
        a = a.rename_columns([*keys, name])
        out = out.join(a, keys=keys, join_type="left outer")
    if out.num_rows != n:
        raise ValueError(f"annual: join changed rows {n} -> {out.num_rows}")
    out = out.sort_by([("year", "ascending"), ("series_id", "ascending")])
    return out


def labstat_dictionary_rows(input_dir: Path) -> list[tuple]:
    """``dicionario`` rows for the coded LABSTAT columns.

    ``statistic`` labels come from cx.process and ``footnote_codes`` labels from
    cx.footnote; rows are emitted only for the columns the architecture marks
    as dictionary-covered.
    """
    proc = _pandas(input_dir / "cx.process")
    foot = _pandas(input_dir / "cx.footnote")
    labels = {
        "statistic": list(
            zip(proc.process_code, proc.process_text, strict=True)
        ),
        "footnote_codes": list(
            zip(foot.footnote_code, foot.footnote_text, strict=True)
        ),
    }
    rows = []
    for t in constants.LABSTAT_TABLES.value:
        for c in normalized_columns(t):
            rows += [
                (t, c, normalize_code(k), None, v)
                for k, v in labels.get(c, [])
            ]
    return rows


def clean_labstat(input_dir: Path, output_dir: Path, tables=None) -> dict:
    """Build the LABSTAT tables into ``output_dir``.

    Args:
        input_dir: Directory holding the cx.* flat files.
        output_dir: Root output directory.
        tables: Optional subset of ``["series", "annual"]``.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year"`` (the
        latest year in ``annual``) when ``annual`` was built.
    """
    tables = tables or constants.LABSTAT_TABLES.value
    result = {}
    if "series" in tables:
        result["series"] = write_table(
            build_series(input_dir), "series", output_dir
        )
    if "annual" in tables:
        at = build_annual(input_dir)
        result["annual"] = write_table(
            at, "annual", output_dir, partition="year"
        )
        result["max_year"] = pc.max(at.column("year")).as_py()  # pyrefly: ignore
    return result
