"""Pure download and cleaning functions for us_census_lodes.

No Prefect imports here. The one-shot onboarding bootstrap under
``models/us_census_lodes/code/`` and the recurring flow in ``flows.py`` both call
these, so the transform is defined once.

The unit of work is one (state, year) pair: up to six RAC files and six WAC
files are fetched, cleaned into one parquet each, and the raw files deleted.
Peak disk therefore stays at roughly a dozen gzipped CSVs rather than the 13 GB
the full download would occupy.
"""

from __future__ import annotations

import concurrent.futures as cf
import csv
import gzip
import io
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.us_census_lodes.constants import (
    ARCHITECTURE_DIR,
    JOB_TYPES,
    RAC_COUNTS,
    SEGMENT,
    STATES,
    WAC_COUNTS,
    XWALK_COLUMNS,
    YEARS,
    is_sentinel,
    rac_url,
    wac_url,
    xwalk_url,
)

USER_AGENT = (
    "Mozilla/5.0 (compatible; basedosdados/1.0; rdahis@basedosdados.org)"
)

PA_TYPES = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "DATE": pa.date32(),
}

# Columns the source only began publishing in a given year. Outside that window
# the files carry literal zeroes, which would read as "no such workers" rather
# than "not collected"; they are converted to NULL and the zero is asserted.
DEMOGRAPHIC_FROM_YEAR = 2009
FIRM_FROM_YEAR = 2011
FIRM_JOB_TYPE = "JT02"

_DEMOGRAPHIC_PREFIXES = (
    "jobs_race_",
    "jobs_ethnicity_",
    "jobs_education_",
    "jobs_sex_",
)
_FIRM_PREFIXES = ("jobs_firm_age_", "jobs_firm_size_")


# --------------------------------------------------------------------------
# Architecture (schema source of truth)
# --------------------------------------------------------------------------
def read_arch(table: str) -> list[dict]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def arch_order(table: str) -> list[str]:
    return [a["name"] for a in read_arch(table)]


def typed_schema(table: str) -> pa.Schema:
    return pa.schema(
        [
            pa.field(a["name"], PA_TYPES[a["bigquery_type"]])
            for a in read_arch(table)
        ]
    )


def string_schema(table: str) -> pa.Schema:
    return pa.schema(
        [pa.field(a["name"], pa.string()) for a in read_arch(table)]
    )


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
MAX_ATTEMPTS = 5


def download(
    url: str,
    dest: Path,
    *,
    missing_ok: bool = True,
    attempts: int = MAX_ATTEMPTS,
) -> Path | None:
    """Fetch ``url`` to ``dest``. Returns None on 404 when ``missing_ok``.

    A 404 is expected and meaningful here: LODES simply does not publish a file
    for a state-year-jobtype combination that has no data (see the coverage
    table in LODESTechDoc8.4). **Every other failure is retried and then
    raised**, so a slow server is never silently recorded as a coverage gap --
    which would quietly drop real state-years from the dataset.

    The download goes to a ``.part`` file and is renamed only once the transfer
    completes, so an interrupted run leaves no truncated file for the next run
    to mistake for a finished one. The whole body is verified to be readable
    gzip before the rename: lehd.ces.census.gov intermittently closes a
    connection mid-body, which otherwise yields a short but plausible file.
    """
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                declared = resp.headers.get("Content-Length")
                with part.open("wb") as fh:
                    shutil.copyfileobj(resp, fh)
            size = part.stat().st_size
            if declared is not None and size != int(declared):
                raise OSError(
                    f"short read: got {size} bytes, Content-Length {declared}"
                )
            with gzip.open(part, "rb") as fh:  # cheap integrity check
                while fh.read(1 << 20):
                    pass
            part.replace(dest)
            return dest
        except urllib.error.HTTPError as exc:
            part.unlink(missing_ok=True)
            if exc.code == 404 and missing_ok:
                return None
            last = exc
        except (OSError, urllib.error.URLError, EOFError) as exc:
            part.unlink(missing_ok=True)
            last = exc
        if attempt < attempts:
            time.sleep(min(2**attempt, 30))
    raise RuntimeError(
        f"failed to download {url} after {attempts} attempts"
    ) from last


def read_gz_csv(path: Path, *, dtype=str) -> pd.DataFrame:
    with gzip.open(path, "rb") as fh:
        return pd.read_csv(io.BytesIO(fh.read()), dtype=dtype)


# --------------------------------------------------------------------------
# Cleaning: RAC / WAC
# --------------------------------------------------------------------------
def _null_unavailable(
    df: pd.DataFrame, year: int, job_type: str, table: str
) -> None:
    """Replace structural zeroes with NULL, asserting they really are zero.

    LODES reserves space for every variable in every year and fills the
    unavailable ones with 0. Publishing that 0 would make ``sum(jobs_sex_female)``
    over 2002-2008 return 0 rather than "not collected", so the cells become
    NULL. The assertion is the point: if the Census Bureau ever backfills these,
    the run fails loudly instead of discarding real data.
    """
    groups: list[tuple[tuple[str, ...], bool]] = [
        (_DEMOGRAPHIC_PREFIXES, year < DEMOGRAPHIC_FROM_YEAR),
    ]
    if table == "workplace_jobs":
        groups.append(
            (
                _FIRM_PREFIXES,
                year < FIRM_FROM_YEAR or job_type != FIRM_JOB_TYPE,
            )
        )
    for prefixes, unavailable in groups:
        if not unavailable:
            continue
        cols = [c for c in df.columns if c.startswith(prefixes)]
        nonzero = [c for c in cols if df[c].max() > 0]
        if nonzero:
            raise ValueError(
                f"{table} {year} {job_type}: columns expected to be structurally "
                f"unavailable carry non-zero values: {nonzero}. The source layout "
                f"has changed; revisit DEMOGRAPHIC_FROM_YEAR / FIRM_FROM_YEAR."
            )
        df[cols] = pd.NA


def clean_rac_wac(
    path: Path,
    table: str,
    year: int,
    job_type: str,
    geo: pd.DataFrame,
) -> pd.DataFrame:
    """Clean one RAC or WAC file into the architecture's column order.

    ``geo`` is the state's block-to-geography lookup from
    :func:`load_block_geography`. County and tract come from **the crosswalk,
    not from slicing the block code**, because the two genuinely disagree:

    * Connecticut replaced counties with planning regions in 2022. The 2020
      tabulation block GEOID still carries the legacy county (``09001``-``09015``)
      while the crosswalk carries the planning region (``09110``-``09190``) --
      measured at 100% of CT blocks, for both county and tract.
    * Even outside CT, LODES reassigns a few blocks: 60 Vermont blocks sit in a
      different crosswalk tract than their prefix implies, and some in a
      different county.

    Slicing the prefix would therefore make these tables contradict
    ``geography_crosswalk`` inside the same dataset, and would not join to
    ``br_bd_diretorios_us``, which follows the current delineation.
    """
    counts = RAC_COUNTS if table == "residence_jobs" else WAC_COUNTS
    geocode = "h_geocode" if table == "residence_jobs" else "w_geocode"

    raw = read_gz_csv(path)
    missing = [src for src, _ in counts if src not in raw.columns]
    if missing or geocode not in raw.columns:
        raise ValueError(
            f"{path.name}: missing source columns {missing or [geocode]}"
        )

    block = raw[geocode].str.zfill(15)
    joined = geo.reindex(block.to_numpy())
    out = pd.DataFrame(
        {
            "year": year,
            "state_id": joined["state_id"].to_numpy(),
            "county_id": joined["county_id"].to_numpy(),
            "census_tract_id": joined["census_tract_id"].to_numpy(),
            "block_id": block,
            "job_type": job_type,
        }
    )
    for src, dest in counts:
        out[dest] = pd.to_numeric(raw[src], errors="raise").astype("Int64")
    out["date_created"] = pd.to_datetime(
        raw["createdate"], format="%Y%m%d", errors="coerce"
    ).dt.date

    _null_unavailable(out, year, job_type, table)
    return out[arch_order(table)]


# --------------------------------------------------------------------------
# Cleaning: geography crosswalk
# --------------------------------------------------------------------------
def clean_xwalk(path: Path) -> pd.DataFrame:
    raw = read_gz_csv(path)
    out = pd.DataFrame(index=raw.index)
    for src, dest in XWALK_COLUMNS:
        if src not in raw.columns:
            raise ValueError(f"{path.name}: missing crosswalk column {src}")
        out[dest] = raw[src]

    out["block_id"] = out["block_id"].str.zfill(15)
    for col, width in (
        ("state_id", 2),
        ("county_id", 5),
        ("census_tract_id", 11),
        ("block_group_id", 12),
    ):
        out[col] = out[col].str.zfill(width)

    # LODES fills an inapplicable geography with an all-nines code and an empty
    # name. Both become NULL so a join never matches a placeholder.
    id_cols = [
        d for _, d in XWALK_COLUMNS if d.endswith("_id") and d != "block_id"
    ]
    for col in id_cols:
        mask = out[col].fillna("").map(is_sentinel)
        out.loc[mask, col] = pd.NA
        base = col[:-3]
        name_col = f"{base}_name"
        if name_col in out.columns:
            out.loc[mask, name_col] = pd.NA

    for col in out.columns:
        if col.endswith("_name") or col == "state_abbreviation":
            out[col] = out[col].replace("", pd.NA)

    for col in ("latitude", "longitude"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date_created"] = pd.to_datetime(
        out["date_created"], format="%Y%m%d", errors="coerce"
    ).dt.date
    return out[arch_order("geography_crosswalk")]


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------
def write_parquet(df: pd.DataFrame, table: str, dest: Path) -> int:
    """Write ``df`` as all-STRING Snappy parquet at ``dest``.

    Staging is all-STRING by Data Basis convention: the dbt model ``safe_cast``s
    every column, and ``pipelines.utils.gcs.dump_header`` stringifies the header
    BigQuery infers the staging schema from, so typed parquet is rejected there.
    Both this bootstrap and the recurring pipeline write through this function,
    which is what keeps the two upload paths' schemas identical.

    Values pass through the architecture's real types first, so ``year``
    serializes as ``"2022"`` rather than ``"2022.0"``, and are then cast to
    string via arrow -- never ``astype(str)``, which renders NULL as the literal
    ``"nan"`` and would defeat ``safe_cast``.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    table_arrow = pa.Table.from_pandas(
        df[arch_order(table)], schema=typed_schema(table), preserve_index=False
    )
    table_arrow = table_arrow.cast(string_schema(table))
    pq.write_table(table_arrow, dest, compression="snappy")
    return table_arrow.num_rows


def load_block_geography(
    state: str, input_dir: Path, output_dir: Path
) -> pd.DataFrame:
    """Block-to-geography lookup for one state, indexed by ``block_id``.

    Reads the state's already-written crosswalk parquet when it exists, so a
    re-run does not re-download it; otherwise it builds the crosswalk first.
    """
    path = output_dir / "geography_crosswalk" / f"{state}.parquet"
    if not path.exists():
        clean_crosswalk(state, input_dir, output_dir)
    df = (
        pq.ParquetFile(path)
        .read(columns=["block_id", "state_id", "county_id", "census_tract_id"])
        .to_pandas()
    )
    return df.set_index("block_id")


def clean_state_year(
    state: str,
    year: int,
    input_dir: Path,
    output_dir: Path,
    *,
    geo: pd.DataFrame | None = None,
    job_types: list[str] | None = None,
    keep_input: bool = False,
) -> dict[str, int]:
    """Download, clean and write one state-year for both RAC and WAC.

    Returns ``{table: rows_written}``; a table absent from the mapping had no
    published file for this state-year, which is a real coverage gap.
    """
    job_types = job_types or JOB_TYPES
    written: dict[str, int] = {}
    if geo is None:
        geo = load_block_geography(state, input_dir, output_dir)

    # Fetch the (at most twelve) files for this state-year concurrently. The
    # server is the bottleneck and this is the natural unit of work: peak disk
    # stays at a dozen gzipped CSVs however many states are being processed.
    jobs = [
        (table, job_type, url_fn(state, job_type, year))
        for table, url_fn in (
            ("residence_jobs", rac_url),
            ("workplace_jobs", wac_url),
        )
        for job_type in job_types
    ]
    with cf.ThreadPoolExecutor(min(len(jobs), 6)) as ex:
        paths = list(
            ex.map(
                lambda j: download(j[2], input_dir / state / Path(j[2]).name),
                jobs,
            )
        )

    for table in ("residence_jobs", "workplace_jobs"):
        frames = []
        for (tbl, job_type, _url), path in zip(jobs, paths):
            if tbl != table or path is None:
                continue
            frames.append(clean_rac_wac(path, table, year, job_type, geo))
            if not keep_input:
                path.unlink(missing_ok=True)
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        # A job block absent from the state's crosswalk gets NULL geography.
        # That should never happen -- the crosswalk is the same release -- so
        # it is worth failing on rather than shipping unjoinable rows.
        orphans = int(df["state_id"].isna().sum())
        if orphans:
            raise ValueError(
                f"{table} {state} {year}: {orphans:,} of {len(df):,} rows have a "
                f"block_id absent from the {state} geography crosswalk"
            )
        dest = output_dir / table / f"year={year}" / f"{state}.parquet"
        written[table] = write_parquet(df, table, dest)
    return written


def clean_crosswalk(
    state: str, input_dir: Path, output_dir: Path, *, keep_input: bool = False
) -> int:
    url = xwalk_url(state)
    path = download(url, input_dir / state / Path(url).name, missing_ok=False)
    assert path is not None
    df = clean_xwalk(path)
    rows = write_parquet(
        df,
        "geography_crosswalk",
        output_dir / "geography_crosswalk" / f"{state}.parquet",
    )
    if not keep_input:
        path.unlink(missing_ok=True)
    return rows


def clean_all(
    input_dir: Path,
    output_dir: Path,
    *,
    states: list[str] | None = None,
    years: list[int] | None = None,
    keep_input: bool = False,
    log=print,
) -> dict[str, int]:
    """Full transform. Returns total rows per table."""
    states = states or STATES
    years = years or YEARS
    totals: dict[str, int] = {}
    for state in states:
        totals["geography_crosswalk"] = totals.get(
            "geography_crosswalk", 0
        ) + clean_crosswalk(
            state, input_dir, output_dir, keep_input=keep_input
        )
        # Loaded once per state and reused across its years: the fact tables
        # take county and tract from here, not from the block prefix.
        geo = load_block_geography(state, input_dir, output_dir)
        for year in years:
            got = clean_state_year(
                state,
                year,
                input_dir,
                output_dir,
                geo=geo,
                keep_input=keep_input,
            )
            for table, rows in got.items():
                totals[table] = totals.get(table, 0) + rows
            log(
                f"{state} {year}: "
                + (
                    ", ".join(f"{k}={v:,}" for k, v in got.items())
                    or "no data"
                )
            )
    return totals


def latest_source_year(log=print) -> int:
    """Highest data year LODES8 publishes, read from the directory listing.

    Probes a large state's RAC listing rather than trusting a hardcoded value,
    so the recurring pipeline detects a new release without a code change.
    """
    import re

    url = "https://lehd.ces.census.gov/data/lodes/LODES8/ca/rac/"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        html = resp.read().decode("utf-8", "replace")
    years = {
        int(m)
        for m in re.findall(rf"_{SEGMENT}_JT00_(\d{{4}})\.csv\.gz", html)
    }
    if not years:
        raise ValueError(f"could not parse any data year from {url}")
    log(f"LODES8 latest data year: {max(years)}")
    return max(years)
