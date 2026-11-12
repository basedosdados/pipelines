"""Pure download and cleaning functions for us_ssa_baby_names (no Prefect).

The national archive holds one ``yob<YYYY>.txt`` per year with rows
``name,sex,count``. The state and territory archives hold one ``<XX>.TXT`` per
state or territory with rows ``state,sex,year,name,count``. None has a header.
SSA omits any name with fewer than 5 occurrences in a sex-year (and, in the
state files, in a state-sex-year) to protect privacy.
"""

import csv
import json
import logging
import re
import shutil
import time
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from pipelines.datasets.us_ssa_baby_names.constants import constants

log = logging.getLogger(__name__)

SEXES = {"F", "M"}


def download_all(input_dir: Path) -> list[Path]:
    """Fetch the three SSA archives into ``input_dir``.

    www.ssa.gov sits behind Akamai, which returns HTTP 403 to requests, urllib
    and plain curl regardless of headers; curl_cffi's Chrome TLS fingerprint is
    admitted (as in us_ssa_beneficiaries).
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for name, path in constants.SOURCE_PATHS.value.items():
        dest = input_dir / name
        if dest.exists() and zipfile.is_zipfile(dest):
            log.info(f"{name} already downloaded, skipping")
            written.append(dest)
            continue
        _download_ranged(constants.BASE_URL.value + path, dest)
        if not zipfile.is_zipfile(dest):
            raise ValueError(f"{dest} is not a valid zip")
        log.info(f"downloaded {name} ({dest.stat().st_size:,} bytes)")
        written.append(dest)
    return written


def _download_ranged(url: str, dest: Path, chunk: int = 512 * 1024) -> None:
    """Download ``url`` to ``dest`` in HTTP Range chunks, resuming a ``.part`` file.

    The Akamai edge drops a long-running response after a couple of minutes, so
    a single GET of a multi-megabyte archive fails on a slow link. Each chunk is
    retried on its own and progress survives a restart.
    """
    from curl_cffi import requests as cr

    part = dest.with_name(dest.name + ".part")
    head = cr.get(
        url, impersonate="chrome", timeout=120, headers={"Range": "bytes=0-0"}
    )
    if head.status_code != 206:
        raise ValueError(f"{url}: no Range support (HTTP {head.status_code})")
    total = int(head.headers["content-range"].rsplit("/", 1)[1])
    done = part.stat().st_size if part.exists() else 0
    with part.open("ab") as f:
        while done < total:
            end = min(done + chunk, total) - 1
            for attempt in range(1, 6):
                try:
                    r = cr.get(
                        url,
                        impersonate="chrome",
                        timeout=600,
                        headers={"Range": f"bytes={done}-{end}"},
                    )
                    if (
                        r.status_code != 206
                        or len(r.content) != end - done + 1
                    ):
                        raise ValueError(
                            f"HTTP {r.status_code}, {len(r.content)} bytes"
                        )
                    break
                except (cr.exceptions.RequestException, ValueError) as e:
                    if attempt == 5:
                        raise
                    log.warning(
                        f"{dest.name} bytes {done}-{end}: {e}; retrying"
                    )
                    time.sleep(10 * attempt)
            f.write(r.content)
            f.flush()
            done = end + 1
            log.info(f"{dest.name}: {done:,}/{total:,} bytes")
    part.replace(dest)


def read_arch(table: str) -> list[dict]:
    path = constants.ARCHITECTURE_DIR.value / f"{table}.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_member(
    zf: zipfile.ZipFile, member: str, names: list[str]
) -> pa.Table:
    opts = pacsv.ReadOptions(column_names=names)
    conv = pacsv.ConvertOptions(
        column_types={n: pa.string() for n in names if n != "count"}
        | {"count": pa.int64()}
    )
    with zf.open(member) as f:
        return pacsv.read_csv(f, read_options=opts, convert_options=conv)


def read_national(zip_path: Path) -> pa.Table:
    """Return ``year, sex, name, count`` from the national archive."""
    parts = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in sorted(zf.namelist()):
            m = re.fullmatch(r"yob(\d{4})\.txt", member)
            if not m:
                continue
            tbl = _read_member(zf, member, ["name", "sex", "count"])
            year = pa.array([int(m.group(1))] * tbl.num_rows, pa.int64())
            parts.append(tbl.append_column("year", year))
    if not parts:
        raise ValueError(f"{zip_path}: no yob<YYYY>.txt members")
    return pa.concat_tables(parts).select(["year", "sex", "name", "count"])


def read_subnational(zip_path: Path) -> pa.Table:
    """Return ``year, state_abbreviation, sex, name, count`` from a state or
    territory archive."""
    parts = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in sorted(zf.namelist()):
            if not re.fullmatch(r"[A-Z]{2}\.TXT", member):
                continue
            tbl = _read_member(
                zf,
                member,
                ["state_abbreviation", "sex", "year", "name", "count"],
            )
            parts.append(tbl)
    if not parts:
        raise ValueError(f"{zip_path}: no <XX>.TXT members")
    tbl = pa.concat_tables(parts)
    tbl = tbl.set_column(
        tbl.schema.get_field_index("year"),
        "year",
        pc.cast(tbl["year"], pa.int64()),  # pyrefly: ignore [missing-attribute]
    )
    return tbl.select(["year", "state_abbreviation", "sex", "name", "count"])


def add_state_id(tbl: pa.Table) -> pa.Table:
    """Add the FIPS ``state_id``.

    ``TR`` is SSA's aggregate of American Samoa, Guam, the Northern Mariana
    Islands and the U.S. Virgin Islands; it has no FIPS code and gets NULL.
    """
    fips = constants.STATE_FIPS.value
    agg = constants.TERRITORY_AGGREGATE.value
    abbrev = tbl["state_abbreviation"].to_pylist()
    unknown = sorted({a for a in abbrev if a not in fips and a != agg})
    if unknown:
        raise ValueError(f"state_abbreviation without a FIPS code: {unknown}")
    return tbl.add_column(
        1, "state_id", pa.array([fips.get(a) for a in abbrev], pa.string())
    )


def validate(tbl: pa.Table, keys: list[str], table: str) -> None:
    """Fail on duplicate keys, unknown sexes, blank names or counts below 5."""
    n = tbl.group_by(keys).aggregate([]).num_rows
    if n != tbl.num_rows:
        raise ValueError(
            f"{table}: {tbl.num_rows - n:,} duplicate {keys} rows"
        )
    sexes = set(pc.unique(tbl["sex"]).to_pylist())  # pyrefly: ignore [missing-attribute]
    if sexes - SEXES:
        raise ValueError(f"{table}: unexpected sex codes {sexes - SEXES}")
    if pc.min(tbl["count"]).as_py() < 5:  # pyrefly: ignore [missing-attribute]
        raise ValueError(f"{table}: a count below the SSA floor of 5")
    blank = pc.sum(pc.equal(pc.utf8_length(tbl["name"]), 0)).as_py()  # pyrefly: ignore [missing-attribute]
    if blank:
        raise ValueError(f"{table}: {blank} blank names")


def to_string_table(tbl: pa.Table, table: str) -> pa.Table:
    """Order the columns as in the architecture and cast every one to string.

    Staging is all-STRING by house convention; the dbt model safe_casts. Casting
    through arrow keeps ``year`` as ``"1880"`` and NULL as NULL.
    """
    order = [a["name"] for a in read_arch(table)]
    missing = [c for c in order if c not in tbl.column_names]
    extra = [c for c in tbl.column_names if c not in order]
    if missing or extra:
        raise ValueError(f"{table}: missing {missing}, unexpected {extra}")
    return tbl.select(order).cast(pa.schema([(c, pa.string()) for c in order]))


def write_table(tbl: pa.Table, table: str, output_dir: Path) -> int:
    """Write ``<table>/year=<y>/data.parquet`` (partition column dropped from the
    files, as BigQuery hive partitioning expects) and a row-count manifest.

    The table directory is replaced whole, so a year removed at the source
    cannot survive from an earlier run.
    """
    tdir = output_dir / table
    if tdir.exists():
        shutil.rmtree(tdir)
    tdir.mkdir(parents=True)
    part = constants.PARTITION_COL.value
    rows = 0
    if part in tbl.column_names:
        keys = tbl[part]
        rest = tbl.drop_columns([part])
        for value in sorted(pc.unique(keys).to_pylist(), key=int):  # pyrefly: ignore [missing-attribute]
            pdir = tdir / f"{part}={value}"
            pdir.mkdir()
            sub = rest.filter(pc.equal(keys, value))  # pyrefly: ignore [missing-attribute]
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


def build_dictionary() -> pa.Table:
    """The value labels of the coded columns (``sex`` in both tables)."""
    rows = [
        (table, "sex", key, label)
        for table in ["first_name", "first_name_state"]
        for key, label in [("F", "Female"), ("M", "Male")]
    ]
    return pa.table(
        {
            "table_id": [r[0] for r in rows],
            "column_name": [r[1] for r in rows],
            "key": [r[2] for r in rows],
            "temporal_coverage": [None] * len(rows),
            "value": [r[3] for r in rows],
        },
        schema=pa.schema(
            [
                ("table_id", pa.string()),
                ("column_name", pa.string()),
                ("key", pa.string()),
                ("temporal_coverage", pa.string()),
                ("value", pa.string()),
            ]
        ),
    )


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean every table; return ``{"rows": {table: n}, "max_year": y}``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = {}

    national = read_national(input_dir / constants.NATIONAL_ZIP.value)
    validate(national, ["year", "sex", "name"], "first_name")
    max_year = pc.max(national["year"]).as_py()  # pyrefly: ignore [missing-attribute]
    rows["first_name"] = write_table(
        to_string_table(national, "first_name"), "first_name", output_dir
    )
    del national

    sub = pa.concat_tables(
        [
            read_subnational(input_dir / constants.STATE_ZIP.value),
            read_subnational(input_dir / constants.TERRITORY_ZIP.value),
        ]
    )
    sub = add_state_id(sub)
    validate(
        sub, ["year", "state_abbreviation", "sex", "name"], "first_name_state"
    )
    rows["first_name_state"] = write_table(
        to_string_table(sub, "first_name_state"),
        "first_name_state",
        output_dir,
    )
    del sub

    rows["dictionary"] = write_table(
        to_string_table(build_dictionary(), "dictionary"),
        "dictionary",
        output_dir,
    )
    log.info(f"latest year: {max_year}")
    return {"rows": rows, "max_year": max_year}
