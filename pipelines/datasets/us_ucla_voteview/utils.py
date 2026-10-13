"""Download and cleaning transform for us_ucla_voteview.

Pure functions, no Prefect. The one-shot onboarding
(``models/us_ucla_voteview/code/clean_data.py``) and the recurring pipeline
(``tasks.py``) both call :func:`download_all` and :func:`clean_all`, so the
transform lives in one place.

Output is all-STRING Snappy parquet with the architecture's column order. The
dbt models ``safe_cast`` every column to its real type.
"""

import csv
import json
import logging
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_ucla_voteview.constants import constants

log = logging.getLogger("us_ucla_voteview")

KEY = ["congress", "chamber", "rollnumber"]


def source_url(table: str) -> str:
    return f"{constants.BASE_URL.value}/{table}/HSall_{table}.csv"


def download_all(input_dir: Path) -> Path:
    """Stream the four ``HSall_*.csv`` files to ``input_dir/<table>.csv``.

    Downloads to a ``.part`` file first, so an interrupted run never leaves a
    truncated CSV that looks complete.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    for table in constants.SOURCE_TABLES.value:
        dest = input_dir / f"{table}.csv"
        part = dest.with_suffix(".csv.part")
        with requests.get(source_url(table), stream=True, timeout=600) as r:
            r.raise_for_status()
            expected = int(r.headers.get("Content-Length", 0))
            with open(part, "wb") as f:
                shutil.copyfileobj(r.raw, f, length=1 << 20)
        size = part.stat().st_size
        if expected and size != expected:
            raise OSError(
                f"{table}: got {size:,} bytes, expected {expected:,}"
            )
        part.replace(dest)
        log.info(f"downloaded {table}: {size:,} bytes")
    return input_dir


def read_arch(table: str) -> list[dict]:
    with open(constants.ARCHITECTURE_DIR.value / f"{table}.csv") as f:
        return list(csv.DictReader(f))


def read_source(path: Path) -> pa.Table:
    """Read a Voteview CSV with every column as string; empty cells are NULL."""
    with open(path) as f:
        names = next(csv.reader(f))
    return pacsv.read_csv(
        path,
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in names},
            strings_can_be_null=True,
        ),
    )


def normalise(tbl: pa.Table) -> pa.Table:
    """Strip whitespace, and turn "100.0" into "100" in integer-valued columns.

    pyarrow.compute's functions are generated at runtime, so Pyrefly cannot see
    them; the ``pyrefly: ignore`` markers on ``pc.*`` calls follow the repo
    convention.
    """
    for i, name in enumerate(tbl.column_names):
        c = pc.utf8_trim_whitespace(tbl.column(i))  # pyrefly: ignore [missing-attribute]
        c = pc.if_else(pc.equal(c, ""), pa.scalar(None, pa.string()), c)  # pyrefly: ignore [missing-attribute]
        if name in constants.INTEGER_LIKE.value:
            c = pc.replace_substring_regex(c, r"^(-?\d+)\.0+$", r"\1")  # pyrefly: ignore [missing-attribute]
        tbl = tbl.set_column(i, name, c)
    return tbl


def add_vote_date(votes: pa.Table, rollcalls: pa.Table) -> pa.Table:
    """Attach the roll call's date to each vote.

    The votes file has no date; the date is what the BD Pro window filters on.
    Every vote must find its roll call, or the window would silently drop it.
    """
    dates = rollcalls.select([*KEY, "date"])
    out = votes.join(dates, keys=KEY, join_type="left outer")
    missing = out.num_rows - pc.count(out["date"]).as_py()  # pyrefly: ignore [missing-attribute]
    if out.num_rows != votes.num_rows or missing:
        raise ValueError(
            f"votes date join: {votes.num_rows:,} rows in, {out.num_rows:,} out, "
            f"{missing:,} without a roll call date"
        )
    return out


def add_state_id(members: pa.Table) -> pa.Table:
    """Add the state's FIPS code, the key of the US state directory.

    Every abbreviation except presidents' "USA" must map, so a new code at the
    source stops the run instead of silently losing its directory link.
    """
    fips = constants.STATE_FIPS.value
    abbrev = members["state_abbrev"].to_pylist()
    unknown = sorted(
        {a for a in abbrev if a not in fips and a not in (None, "USA")}
    )
    if unknown:
        raise ValueError(
            f"members: state_abbrev without a FIPS code: {unknown}"
        )
    return members.append_column(
        "state_id", pa.array([fips.get(a) for a in abbrev], pa.string())
    )


def to_string_table(tbl: pa.Table, table: str) -> pa.Table:
    """Order the columns as in the architecture and check none is missing."""
    order = [a["name"] for a in read_arch(table)]
    missing = [c for c in order if c not in tbl.column_names]
    extra = [c for c in tbl.column_names if c not in order]
    if missing or extra:
        raise ValueError(f"{table}: missing {missing}, unexpected {extra}")
    return tbl.select(order).cast(pa.schema([(c, pa.string()) for c in order]))


def drop_exact_duplicates(
    tbl: pa.Table, keys: list[str], label: str
) -> pa.Table:
    """Drop rows that repeat another row exactly; fail if a key repeats otherwise.

    The 115th Congress votes file lists 152 of the president's (icpsr 99912)
    votes twice, identically. Identical copies are dropped; copies that disagree
    on any column would mean the source is ambiguous, so they raise.
    """
    n_keys = tbl.group_by(keys).aggregate([]).num_rows
    if n_keys == tbl.num_rows:
        return tbl
    out = tbl.group_by(tbl.column_names).aggregate([]).select(tbl.column_names)
    if out.num_rows != n_keys:
        raise ValueError(
            f"{label}: {out.num_rows - n_keys:,} conflicting {keys} rows"
        )
    log.warning(
        f"{label}: dropped {tbl.num_rows - out.num_rows:,} exact duplicate rows"
    )
    return out


def write_table(tbl: pa.Table, table: str, output_dir: Path) -> int:
    """Write a table and its row-count manifest; return the rows written.

    Partitioned tables go to ``<table>/congress=<n>/data.parquet`` with the
    partition column dropped from the files, as BigQuery hive partitioning
    expects. The table directory is replaced whole, so a congress removed at the
    source cannot survive from an earlier run. Votes are de-duplicated one
    congress at a time, which keeps the memory cost to one partition.
    """
    tdir = output_dir / table
    if tdir.exists():
        shutil.rmtree(tdir)
    tdir.mkdir(parents=True)
    part = constants.PARTITION_COL.value
    if table in constants.PARTITIONED_TABLES.value:
        keys = tbl[part]
        rest = tbl.drop_columns([part])
        rows = 0
        for value in sorted(pc.unique(keys).to_pylist(), key=int):  # pyrefly: ignore [missing-attribute]
            pdir = tdir / f"{part}={value}"
            pdir.mkdir()
            sub = rest.filter(pc.equal(keys, value))  # pyrefly: ignore [missing-attribute]
            if table == "votes":
                sub = drop_exact_duplicates(
                    sub,
                    ["chamber", "rollnumber", "icpsr"],
                    f"votes congress={value}",
                )
            pq.write_table(sub, pdir / "data.parquet", compression="snappy")
            rows += sub.num_rows
    else:
        pq.write_table(tbl, tdir / "data.parquet", compression="snappy")
        rows = tbl.num_rows
    (output_dir / f"_manifest_{table}.json").write_text(
        json.dumps({"rows": rows})
    )
    log.info(f"{table}: {rows:,} rows -> {tdir}")
    return rows


def check_unique(tbl: pa.Table, keys: list[str], table: str) -> None:
    n = tbl.group_by(keys).aggregate([]).num_rows
    if n != tbl.num_rows:
        raise ValueError(
            f"{table}: {tbl.num_rows - n:,} duplicate {keys} rows"
        )


def build_dictionary() -> pa.Table:
    return normalise(read_source(constants.DICTIONARY_CSV.value))


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean every table; return ``{table: rows}`` and the latest roll call date."""
    output_dir.mkdir(parents=True, exist_ok=True)
    src = {
        t: normalise(read_source(input_dir / f"{t}.csv"))
        for t in constants.SOURCE_TABLES.value
    }

    check_unique(src["members"], ["congress", "chamber", "icpsr"], "members")
    check_unique(
        src["parties"], ["congress", "chamber", "party_code"], "parties"
    )
    check_unique(src["rollcalls"], KEY, "rollcalls")

    src["members"] = add_state_id(src["members"])
    rows = {}
    for table in ["members", "parties", "rollcalls"]:
        rows[table] = write_table(
            to_string_table(src[table], table), table, output_dir
        )

    votes = add_vote_date(src.pop("votes"), src["rollcalls"])
    rows["votes"] = write_table(
        to_string_table(votes, "votes"), "votes", output_dir
    )
    del votes

    rows["dictionary"] = write_table(
        to_string_table(build_dictionary(), "dictionary"),
        "dictionary",
        output_dir,
    )
    max_date = pc.max(src["rollcalls"]["date"]).as_py()  # pyrefly: ignore [missing-attribute]
    log.info(f"latest roll call: {max_date}")
    return {"rows": rows, "max_date": max_date}
