"""Download and cleaning functions for the Census Business Dynamics Statistics.

Pure functions with no Prefect dependency, shared by the one-shot onboarding
scripts under ``models/us_census_bds/code/`` and the recurring pipeline.
"""

from __future__ import annotations

import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_census_bds.constants import constants

HEADERS = constants.HEADERS.value
FILE_RE = re.compile(r'href="(bds(?:pr)?(\d{4})[a-z0-9_]*\.csv)"')


def latest_release() -> int:
    """Return the most recent release year published on the Census server."""
    html = requests.get(
        constants.BASE_URL.value, headers=HEADERS, timeout=60
    ).text
    return max(int(y) for y in re.findall(r'href="(\d{4})/"', html))


def list_files(release: int) -> list[str]:
    """List the CSV table files of a release."""
    url = f"{constants.BASE_URL.value}{release}/"
    html = requests.get(url, headers=HEADERS, timeout=60).text
    return sorted(
        {name for name, year in FILE_RE.findall(html) if int(year) == release}
    )


def _content_length(url: str) -> int:
    """Return a file's size, retrying because the server drops connections."""
    for attempt in range(8):
        try:
            resp = requests.head(url, headers=HEADERS, timeout=60)
            resp.raise_for_status()
            return int(resp.headers["content-length"])
        except requests.RequestException:
            if attempt == 7:
                raise
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def _fetch_range(url: str, start: int, end: int, part: Path) -> None:
    """Fetch one byte range into a part file, skipping it if already complete."""
    size = end - start + 1
    if part.exists() and part.stat().st_size == size:
        return
    for attempt in range(8):
        try:
            resp = requests.get(
                url,
                headers={**HEADERS, "Range": f"bytes={start}-{end}"},
                timeout=600,
            )
            resp.raise_for_status()
            if len(resp.content) != size:
                raise OSError(f"short range {len(resp.content)} != {size}")
            part.write_bytes(resp.content)
            return
        except (requests.RequestException, OSError):
            if attempt == 7:
                raise
            time.sleep(2**attempt)


def _reassemble(
    name: str, input_dir: Path, parts_dir: Path, size: int
) -> None:
    """Concatenate a file's downloaded ranges and check its final size."""
    parts = sorted(parts_dir.glob(f"{name}.*"))
    tmp = input_dir / f"{name}.tmp"
    with tmp.open("wb") as out:
        for part in parts:
            out.write(part.read_bytes())
    if tmp.stat().st_size != size:
        raise OSError(f"{name}: size mismatch after reassembly")
    tmp.rename(input_dir / name)
    for part in parts:
        part.unlink()


def download_release(release: int, input_dir: Path) -> list[Path]:
    """Download every CSV of a release into ``input_dir``.

    Files are split into byte ranges fetched concurrently and reassembled;
    a file already present with the server's content length is skipped.
    """
    base = f"{constants.BASE_URL.value}{release}/"
    input_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = input_dir / ".parts"
    parts_dir.mkdir(exist_ok=True)
    chunk = constants.CHUNK_BYTES.value

    files = list_files(release)
    sizes = {}
    with ThreadPoolExecutor(constants.WORKERS.value) as pool:
        heads = {pool.submit(_content_length, base + f): f for f in files}
        for fut in as_completed(heads):
            sizes[heads[fut]] = fut.result()

    # Smallest first, so most tables are complete long before the largest
    # crosses arrive.
    todo = [
        f
        for f in sorted(files, key=sizes.get)
        if not (
            (input_dir / f).exists()
            and (input_dir / f).stat().st_size == sizes[f]
        )
    ]
    jobs = {}
    remaining = {}
    with ThreadPoolExecutor(constants.WORKERS.value) as pool:
        for f in todo:
            starts = range(0, sizes[f], chunk)
            remaining[f] = len(starts)
            for i, start in enumerate(starts):
                end = min(start + chunk, sizes[f]) - 1
                part = parts_dir / f"{f}.{i:05d}"
                fut = pool.submit(_fetch_range, base + f, start, end, part)
                jobs[fut] = f
        for fut in as_completed(jobs):
            fut.result()
            f = jobs[fut]
            remaining[f] -= 1
            if remaining[f] == 0:
                _reassemble(f, input_dir, parts_dir, sizes[f])
                print(f"downloaded {f}", flush=True)

    return [input_dir / f for f in files]


# Source dimension variable -> output column. Fine and coarse versions of a
# classification share one column; the ``breakdown`` column says which.
DIMENSIONS = {
    "metro": "metro_status",
    "sector": "naics_id",
    "vcnaics3": "naics_id",
    "vcnaics4": "naics_id",
    "fage": "firm_age",
    "fagecoarse": "firm_age",
    "fsize": "firm_size",
    "fsizecoarse": "firm_size",
    "ifsize": "initial_firm_size",
    "ifsizecoarse": "initial_firm_size",
    "eage": "establishment_age",
    "eagecoarse": "establishment_age",
    "esize": "establishment_size",
    "esizecoarse": "establishment_size",
    "iesize": "initial_establishment_size",
    "iesizecoarse": "initial_establishment_size",
}
GEOGRAPHY = ("st", "cty", "msa")
# Canonical order of source variables inside a ``breakdown`` value, so that
# ``met_st_ea`` and ``st_met_ea`` style file names map to one value.
BREAKDOWN_ORDER = [*GEOGRAPHY, *DIMENSIONS]

# Source measure -> output column, in source order.
MEASURES = {
    "firms": "firms",
    "estabs": "establishments",
    "emp": "employment",
    "denom": "dhs_denominator",
    "estabs_entry": "establishment_entries",
    "estabs_entry_rate": "establishment_entry_rate",
    "estabs_exit": "establishment_exits",
    "estabs_exit_rate": "establishment_exit_rate",
    "job_creation": "job_creation",
    "job_creation_births": "job_creation_births",
    "job_creation_continuers": "job_creation_continuers",
    "job_creation_rate_births": "job_creation_rate_births",
    "job_creation_rate": "job_creation_rate",
    "job_destruction": "job_destruction",
    "job_destruction_deaths": "job_destruction_deaths",
    "job_destruction_continuers": "job_destruction_continuers",
    "job_destruction_rate_deaths": "job_destruction_rate_deaths",
    "job_destruction_rate": "job_destruction_rate",
    "net_job_creation": "net_job_creation",
    "net_job_creation_rate": "net_job_creation_rate",
    "reallocation_rate": "reallocation_rate",
    "firmdeath_firms": "firm_death_firms",
    "firmdeath_estabs": "firm_death_establishments",
    "firmdeath_emp": "firm_death_employment",
}
# D disclosure, S data quality, X structurally missing/zero, N rate not
# available. Each becomes NULL in its measure and is kept in suppression_flags.
FLAGS = {"D", "S", "X", "N"}

# The NAICS 2017 directory keys the hyphenated sectors by their first code.
SECTOR_TO_NAICS = {"31-33": "31", "44-45": "44", "48-49": "48"}


def table_for(dims: list[str]) -> str:
    """Return the output table a file belongs to, from its dimension columns."""
    if "cty" in dims:
        return "county"
    if "msa" in dims:
        return "metropolitan_area"
    if "st" in dims:
        return "state"
    return "country"


def breakdown_of(dims: list[str]) -> str:
    """Return the canonical breakdown value for a file's dimension columns."""
    unknown = set(dims) - set(BREAKDOWN_ORDER)
    if unknown:
        raise ValueError(f"unknown BDS dimension(s): {sorted(unknown)}")
    ordered = [d for d in BREAKDOWN_ORDER if d in dims]
    return ",".join(ordered) if ordered else "total"


def read_dimensions(path: Path) -> list[str]:
    """Return a BDS file's dimension columns, read from its header."""
    with path.open() as f:
        header = f.readline().strip().split(",")
    return header[1 : header.index("firms")]


def clean_frame(raw: pd.DataFrame, name: str) -> tuple[str, pd.DataFrame]:
    """Clean one BDS CSV into the long layout shared by every output table.

    Args:
        raw: Rows of a ``bds<release>[_dims].csv`` or
            ``bdspr<release>[_dims].csv`` file, read as strings.
        name: The file name, for error messages.

    Returns:
        The output table slug and an all-string frame with that table's
        columns (in architecture order) for the rows of this file.
    """
    header = list(raw.columns)
    if header[header.index("firms") :] != list(MEASURES):
        raise ValueError(f"{name}: unexpected measure columns")
    dims = header[1 : header.index("firms")]
    table = table_for(dims)

    out = pd.DataFrame({"year": raw["year"]})
    out["breakdown"] = breakdown_of(dims)
    if "st" in dims:
        out["state_id"] = raw["st"]
    if "cty" in dims:
        # 999 is the statewide pseudo-county for establishments without a
        # fixed location; it has no county, so the directory key is NULL.
        out["county_id"] = (raw["st"] + raw["cty"]).where(raw["cty"] != "999")
    if "msa" in dims:
        out["cbsa_id"] = raw["msa"]
    for src in dims:
        if src in DIMENSIONS:
            value = raw[src]
            if src == "sector":
                value = value.replace(SECTOR_TO_NAICS)
            out[DIMENSIONS[src]] = value

    flags = pd.Series("", index=raw.index)
    for src, name in MEASURES.items():
        value = raw[src].str.strip()
        flagged = value.isin(FLAGS)
        bad = (
            ~flagged
            & (value != "")
            & pd.to_numeric(value, errors="coerce").isna()
        )
        if bad.any():
            raise ValueError(
                f"{name}:{src}: non-numeric {value[bad].unique()[:5]}"
            )
        flags = flags.where(~flagged, flags + name + "=" + value + ";")
        out[name] = value.where(~flagged & (value != ""))
    out["suppression_flags"] = flags.str.rstrip(";").replace("", None)
    return table, out


def clean_file(
    path: Path, output: Path, chunksize: int = 500_000
) -> tuple[str, int]:
    """Clean one BDS CSV in chunks and write it as partitioned parquet.

    Returns:
        The output table slug and the number of rows written.
    """
    rows = 0
    table = table_for(read_dimensions(path))
    for k, raw in enumerate(
        pd.read_csv(
            path, dtype=str, keep_default_na=False, chunksize=chunksize
        )
    ):
        table, frame = clean_frame(raw, path.name)
        rows += write_partitioned(frame, table, f"{path.stem}-{k:03d}", output)
    return table, rows


# Labels of the source dimension variables, for the breakdown dictionary.
DIMENSION_LABELS = {
    "st": "state",
    "cty": "county",
    "msa": "metropolitan or micropolitan area",
    "metro": "metro status",
    "sector": "NAICS sector",
    "vcnaics3": "3-digit NAICS industry",
    "vcnaics4": "4-digit NAICS industry",
    "fage": "firm age",
    "fagecoarse": "firm age (coarse)",
    "fsize": "firm size",
    "fsizecoarse": "firm size (coarse)",
    "ifsize": "initial firm size",
    "ifsizecoarse": "initial firm size (coarse)",
    "eage": "establishment age",
    "eagecoarse": "establishment age (coarse)",
    "esize": "establishment size",
    "esizecoarse": "establishment size (coarse)",
    "iesize": "initial establishment size",
    "iesizecoarse": "initial establishment size (coarse)",
}
METRO_LABELS = {
    "M": "Metropolitan or micropolitan statistical area",
    "N": "Outside metropolitan and micropolitan statistical areas",
    "SW": "Statewide: establishments without a fixed location",
}


def build_dictionary(paths: list[Path], coverage: str) -> pd.DataFrame:
    """Build the dictionary rows for breakdown and metro_status."""
    rows = set()
    for path in paths:
        dims = read_dimensions(path)
        table = table_for(dims)
        breakdown = breakdown_of(dims)
        if breakdown == "total":
            label = "Economy-wide total"
        else:
            label = " by ".join(
                DIMENSION_LABELS[d] for d in breakdown.split(",")
            )
            label = label[0].upper() + label[1:]
        rows.add((table, "breakdown", breakdown, label))
        if "metro" in dims:
            for key, value in METRO_LABELS.items():
                rows.add((table, "metro_status", key, value))
    frame = pd.DataFrame(
        sorted(rows), columns=["table_id", "column_name", "key", "value"]
    )
    frame.insert(3, "temporal_coverage", coverage)
    return frame


def table_columns(table: str) -> list[str]:
    """Read a table's column order from its architecture CSV."""
    path = constants.ARCHITECTURE_DIR.value / f"{table}.csv"
    with path.open() as f:
        return [row["name"] for row in csv.DictReader(f)]


def write_partitioned(
    frame: pd.DataFrame, table: str, stem: str, output: Path
) -> int:
    """Write one source file's rows as all-STRING parquet, one file per year.

    Staging is all-STRING by house convention: the dbt model casts each
    column. Columns this file lacks are added as NULL so every partition
    file of a table shares one schema.
    """
    columns = table_columns(table)
    extra = set(frame.columns) - set(columns)
    if extra:
        raise ValueError(f"{stem}: columns not in {table}: {sorted(extra)}")
    frame = frame.reindex(columns=columns)
    schema = pa.schema([(c, pa.string()) for c in columns if c != "year"])
    for year, part in frame.groupby("year", sort=True):
        target = output / table / f"year={year}"
        target.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pandas(
            part.drop(columns="year"), schema=schema, preserve_index=False
        )
        pq.write_table(arrow, target / f"{stem}.parquet", compression="snappy")
    return len(frame)
