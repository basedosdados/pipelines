"""Download + cleaning transform for us_bls_employment (shared by the recurring
pipeline and the one-shot bootstrap in models/us_bls_employment/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema and column order come from the architecture CSVs,
the single source of truth.

Four BLS programs share one shape: a `.series` catalogue carrying the decomposed
series-id dimensions, and `.data.*` files carrying (series_id, year, period,
value, footnote_codes). The series id is never string-split — the catalogue
already holds every component as its own field, which is what the program
documentation says it is for.
"""

import csv
import logging
import re
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_bls_employment.constants import constants

log = logging.getLogger("us_bls_employment")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
PROGRAMS = constants.PROGRAMS.value
OBS_KEY = constants.OBS_KEY.value
_ARCH = constants.ARCHITECTURE_DIR.value

# A single clean NAICS code; ce.industry also carries compound values
# ("21221,3,9") and partial ones ("part 238") that resolve to no single code.
_NAICS_RE = re.compile(r"^\d{2,6}$")


# ── download ────────────────────────────────────────────────────────────────
def list_remote(prog: str, session: requests.Session) -> dict[str, int]:
    """List a program's directory on the BLS flat-file server.

    Args:
        prog: BLS program directory (``ce``, ``sm``, ``la``, ``jt``).
        session: Session carrying the browser User-Agent header.

    Returns:
        Mapping of file name to its size in bytes, as advertised by the listing.
    """
    r = session.get(f"{constants.BASE_URL.value}/{prog}/", timeout=(30, 300))
    r.raise_for_status()
    return {
        m.group(2): int(m.group(1))
        for m in re.finditer(r"(\d+) <A HREF=\"[^\"]*/([^\"/]+)\">", r.text)
    }


def _data_files(prog: str, names) -> list[str]:
    """Select the observation files that make a complete history for a program.

    Args:
        prog: BLS program directory.
        names: Every file name advertised in that directory.

    Returns:
        The subset to download, in a deterministic order.
    """
    spec = constants.DATA_FILES.value[prog]
    if isinstance(spec, list):
        return [n for n in spec if n in names]
    return sorted(n for n in names if re.match(spec, n))


def download_flatfiles(input_dir: Path) -> Path:
    """Fetch every dimension and observation file for the four programs.

    A browser-like User-Agent carrying a contact email is mandatory:
    download.bls.gov returns 403 without one. Each download is size-checked
    against the directory listing, because a dropped connection otherwise looks
    like a successful short file, and re-fetched up to four times.

    Args:
        input_dir: Directory to download into; created if absent.

    Returns:
        The same ``input_dir``, for chaining.

    Raises:
        RuntimeError: If a file still fails after four attempts.
    """
    session = requests.Session()
    session.headers["User-Agent"] = constants.USER_AGENT.value
    base = constants.BASE_URL.value
    for prog in PROGRAMS.values():
        d = input_dir / prog
        d.mkdir(parents=True, exist_ok=True)
        sizes = list_remote(prog, session)
        names = [f"{prog}.{s}" for s in constants.DIM_FILES.value[prog]]
        names += _data_files(prog, sizes)
        for name in names:
            path = d / name
            want = sizes.get(name)
            if path.exists() and want and path.stat().st_size == want:
                continue
            for attempt in range(4):
                try:
                    got = 0
                    tmp = path.with_suffix(path.suffix + ".part")
                    with session.get(
                        f"{base}/{prog}/{name}", timeout=(30, 600), stream=True
                    ) as r:
                        r.raise_for_status()
                        with open(tmp, "wb") as fh:
                            for chunk in r.iter_content(1 << 20):
                                fh.write(chunk)
                                got += len(chunk)
                    if want and got != want:
                        raise OSError(f"short read {got} != {want}")
                    tmp.rename(path)
                    break
                except Exception as exc:
                    log.warning(f"{prog}/{name} attempt {attempt + 1}: {exc}")
            else:
                raise RuntimeError(f"failed to download {prog}/{name}")
        log.info(f"{prog}: {len(names)} files -> {d}")
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the raw files, so
    the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (e.g. ``"laus"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def read_tsv(path: Path) -> pd.DataFrame:
    """Read a BLS tab-delimited file, stripping the padding BLS writes.

    Every field in these files is space-padded to a fixed width, headers
    included, so both are stripped here rather than at each call site.

    Args:
        path: File to read.

    Returns:
        The file as strings, with no NaN coercion and no residual padding.
    """
    df = pd.read_csv(path, sep="\t", dtype=str, na_filter=False)
    df.columns = [c.strip() for c in df.columns]
    return df.apply(lambda s: s.str.strip())


# ── series catalogues (one per program) ─────────────────────────────────────
def _naics_map(input_dir: Path) -> dict[str, str]:
    """Map CES industry codes to a NAICS code, where exactly one applies.

    ``ce.industry`` ships a ``naics_code`` column alongside the CES 8-digit
    industry code. Most rows carry a single NAICS code at some level (2 to 6
    digits), but CES aggregates carry either ``-`` (no NAICS equivalent, e.g.
    "Total nonfarm"), a comma list of sibling codes ("21221,3,9"), or a "part
    NNN" marker for a partial industry. Only the single-code rows are mapped;
    everything else yields NULL, so the NAICS foreign key never points at a code
    that does not exist.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        Mapping of CES industry code to a single NAICS code.
    """
    df = read_tsv(input_dir / "ce" / "ce.industry")
    return {
        r["industry_code"]: r["naics_code"]
        for _, r in df.iterrows()
        if _NAICS_RE.match(r["naics_code"])
    }


def series_ces_national(input_dir: Path) -> pd.DataFrame:
    """Build the CES national series catalogue with its dimensions decomposed.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per series: identity plus supersector, CES industry, the NAICS
        code where one applies, data type, seasonal adjustment, and the unit the
        data type fixes.
    """
    df = read_tsv(input_dir / "ce" / "ce.series")
    naics = _naics_map(input_dir)
    return pd.DataFrame(
        {
            "series_id": df["series_id"],
            "seasonal_adjustment": df["seasonal"],
            "supersector_id": df["supersector_code"],
            "industry_id": df["industry_code"],
            "naics_id": df["industry_code"].map(naics),
            "data_type_id": df["data_type_code"],
            "measurement_unit": df["data_type_code"].map(
                constants.CE_UNITS.value
            ),
        }
    )


def series_ces_state_metro(input_dir: Path) -> pd.DataFrame:
    """Build the CES state/metro series catalogue with its dimensions decomposed.

    ``state_code`` ``00`` means "All States" and ``area_code`` ``00000`` means
    "Statewide"; both are aggregates rather than places, so they become NULL in
    the foreign-key columns while the raw code is kept in ``area_id``.

    ``area_code`` is not uniformly a CBSA code. Alongside the metropolitan and
    micropolitan areas it carries metropolitan *divisions*, which OMB numbers in
    the same range but which are not CBSAs, plus two BLS-specific codes. The
    divisions are identified from their own names in ``sm.area`` rather than
    from a hard-coded list, so a division added in a later release is excluded
    automatically; the two special codes are named explicitly. Everything
    excluded here is still readable from ``area_id``.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per series, with state and CBSA identifiers alongside the CES
        industry dimensions and the benchmark year.
    """
    df = read_tsv(input_dir / "sm" / "sm.series")
    naics = _naics_map(input_dir)
    areas = read_tsv(input_dir / "sm" / "sm.area")
    non_cbsa = set(constants.SM_NON_CBSA_AREAS.value) | {
        r["area_code"]
        for _, r in areas.iterrows()
        if "Metropolitan Division" in r["area_name"]
    }
    return pd.DataFrame(
        {
            "series_id": df["series_id"],
            "seasonal_adjustment": df["seasonal"],
            "state_id": df["state_code"].where(df["state_code"] != "00"),
            "cbsa_id": df["area_code"].where(~df["area_code"].isin(non_cbsa)),
            "area_id": df["area_code"],
            "supersector_id": df["supersector_code"],
            "industry_id": df["industry_code"],
            "naics_id": df["industry_code"].map(naics),
            "data_type_id": df["data_type_code"],
            "benchmark_year": pd.to_numeric(
                df["benchmark_year"], errors="coerce"
            ).astype("Int64"),
            "measurement_unit": df["data_type_code"].map(
                constants.SM_UNITS.value
            ),
        }
    )


def series_laus(input_dir: Path) -> pd.DataFrame:
    """Build the LAUS series catalogue with its dimensions decomposed.

    LAUS packs geography into a 15-character ``area_code`` whose first two
    characters name the area kind. Three identifiers are read out of it:

    - ``state_id`` from the catalogue's own ``srd_code``, which is a state FIPS
      code except for ``80`` ("Census Regions and Divisions"), which is NULL.
    - ``county_id`` for area type F (counties), where characters 3-7 are the
      5-digit county FIPS code.
    - ``cbsa_id`` for area types B and D (metropolitan and micropolitan areas),
      where characters 5-9 are the CBSA code. Metropolitan *divisions* (type C)
      and combined areas (type E) sit in the same position but are not CBSA
      codes, so they stay NULL and are readable only from ``area_id``.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per series, with geography, area type, and measure.
    """
    df = read_tsv(input_dir / "la" / "la.series")
    area, atype = df["area_code"], df["area_type_code"]
    return pd.DataFrame(
        {
            "series_id": df["series_id"],
            "seasonal_adjustment": df["seasonal"],
            "state_id": df["srd_code"].where(df["srd_code"] != "80"),
            "county_id": area.str.slice(2, 7).where(atype == "F"),
            "cbsa_id": area.str.slice(4, 9).where(atype.isin(["B", "D"])),
            "area_type_id": atype,
            "area_id": area,
            "measure_id": df["measure_code"],
            "measurement_unit": df["measure_code"].map(
                constants.LA_UNITS.value
            ),
        }
    )


def series_jolts(input_dir: Path) -> pd.DataFrame:
    """Build the JOLTS series catalogue with its dimensions decomposed.

    ``state_code`` does double duty: it carries a state FIPS code, ``00`` for
    the national total, and the four census regions as letter codes (``MW``,
    ``NE``, ``SO``, ``WE``). Those are split apart here — ``state_id`` keeps only
    real state codes and ``region_id`` the regions — because a region silently
    sitting in a state column is a code system collision, and JOLTS publishes
    nothing else that would reveal it: ``area_code`` is the constant ``00000``
    for every series, so it carries no information and is not kept.

    The unemployed-per-job-opening series (data element ``UO``) is a
    dimensionless ratio rather than the rate its ``ratelevel_code`` claims, so
    its unit is overridden.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per series, with geography, industry, size class, data element,
        and rate-or-level.
    """
    df = read_tsv(input_dir / "jt" / "jt.series")
    unit = df["ratelevel_code"].map(constants.JT_UNITS.value)
    state = df["state_code"]
    is_region = state.isin(constants.JT_REGIONS.value)
    return pd.DataFrame(
        {
            "series_id": df["series_id"],
            "seasonal_adjustment": df["seasonal"],
            "state_id": state.where(~is_region & (state != "00")),
            "region_id": state.where(is_region),
            "industry_id": df["industry_code"],
            "sizeclass_id": df["sizeclass_code"],
            "dataelement_id": df["dataelement_code"],
            "ratelevel_id": df["ratelevel_code"],
            "measurement_unit": unit.where(
                df["dataelement_code"] != "UO", "ratio"
            ),
        }
    )


SERIES_BUILDERS = {
    "ces_national": series_ces_national,
    "ces_state_metro": series_ces_state_metro,
    "laus": series_laus,
    "jolts": series_jolts,
}


# ── observations: shard by year, then dedup + join per year ─────────────────
_RAW_SCHEMA = pa.schema(
    [
        pa.field("series_id", pa.string()),
        pa.field("period", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("footnote_id", pa.string()),
    ]
)


def shard_by_year(input_dir: Path, prog: str, stage_dir: Path) -> int:
    """Pass 1 — split every observation file for a program into per-year shards.

    The programs are too large to hold at once (LAUS alone is 1.6 GB of text
    spanning 1976 forward), and their observation files overlap: the per-state
    files repeat the statewide and region series, and for some states the
    history is split across several files by year range. Sharding by year first
    means the deduplication in pass 2 only ever needs one year in memory, and
    catches an overlap wherever it falls.

    BLS writes ``-`` for a missing observation. That is NULL, not zero and not a
    negative number.

    Args:
        input_dir: Root of the downloaded files.
        prog: BLS program directory.
        stage_dir: Scratch directory for the shards; cleared first.

    Returns:
        Number of observation rows read, before deduplication.
    """
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True)
    names = _data_files(prog, {p.name for p in (input_dir / prog).iterdir()})
    total = 0
    for i, name in enumerate(names):
        df = read_tsv(input_dir / prog / name)
        df = df.rename(columns={"footnote_codes": "footnote_id"})
        # `-` and the empty string are both "not published". Masking them is
        # equivalent to replacing them with None and types cleanly, where a
        # dict-with-None does not.
        value = df["value"]
        df["value"] = pd.to_numeric(
            value.where(~value.isin(["-", ""])), errors="coerce"
        )
        footnote = df["footnote_id"]
        df["footnote_id"] = footnote.where(footnote != "")
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("int64")
        total += len(df)
        for year, g in df.groupby("year", sort=False):
            # `year` is int64 by the line above; the groupby key is untyped.
            # pyrefly: ignore [bad-argument-type]
            d = stage_dir / f"year={int(year)}"
            d.mkdir(exist_ok=True)
            pq.write_table(
                pa.Table.from_pandas(
                    g[["series_id", "period", "value", "footnote_id"]],
                    schema=_RAW_SCHEMA,
                    preserve_index=False,
                ),
                d / f"part-{i:03d}.parquet",
                compression="snappy",
            )
        log.info(f"{prog}: sharded {name} ({len(df):,} rows)")
    return total


def build_year(
    shard_dir: Path, series: pd.DataFrame, table: str
) -> tuple[pd.DataFrame, int]:
    """Pass 2 — assemble one year of one table from its shards.

    Deduplicates on (series_id, year, period) before the dimension join, then
    derives the calendar columns. ``month`` is 1-12 for the monthly periods and
    NULL for ``M13``, the annual average BLS publishes in the same files; the
    raw period code is kept in ``period_id`` so the two are never conflated.

    Args:
        shard_dir: One ``year=<YYYY>`` directory written by :func:`shard_by_year`.
        series: The program's series catalogue.
        table: Table slug, used for the architecture column order.

    Returns:
        The year's rows in architecture column order, and how many distinct
        observations that year held before the join — the difference is the
        number dropped for having no series record.
    """
    year = int(shard_dir.name.split("=")[1])
    df = pq.read_table(shard_dir).to_pandas()
    df = df.drop_duplicates(["series_id", "period"], ignore_index=True)
    n_dedup = len(df)
    df = df.merge(series, on="series_id", how="inner", validate="many_to_one")
    df["year"] = year
    df["period_id"] = df["period"]
    df["month"] = pd.to_numeric(
        df["period"]
        .str.slice(1)
        .where(df["period"].str.match(r"^M(0[1-9]|1[0-2])$")),
        errors="coerce",
    ).astype("Int64")
    order = [a["name"] for a in read_arch(table)]
    missing = [c for c in order if c not in df.columns]
    if missing:
        raise KeyError(f"{table}: architecture columns not built: {missing}")
    return df[order], n_dedup


def write_year(sub: pd.DataFrame, table: str, output_dir: Path) -> None:
    """Write one year of a table as all-STRING Snappy Parquet.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file BigQuery infers the staging schema from. Typed
    parquet is rejected against that schema.

    Values still pass through the architecture's real types first, so ``year``
    serializes as ``"1976"`` rather than ``"1976.0"``, and only then cast to
    string via arrow — never ``astype(str)``, which renders a NULL as the
    literal ``"nan"`` and defeats the dbt ``safe_cast``. ``value`` has genuine
    NULLs wherever BLS printed ``-``.

    Args:
        sub: One year of rows, from :func:`build_year`.
        table: Table slug.
        output_dir: Root output directory.
    """
    arch = read_arch(table)
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    d = output_dir / table / f"year={int(sub['year'].iloc[0])}"
    d.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pandas(sub, schema=typed, preserve_index=False)
    pq.write_table(at.cast(strings), d / "data.parquet", compression="snappy")


def build_program(
    input_dir: Path, output_dir: Path, stage_root: Path, table: str
) -> dict:
    """Clean one program end to end, from flat files to partitioned parquet.

    Args:
        input_dir: Root of the downloaded files.
        output_dir: Root output directory.
        stage_root: Scratch root for the per-year shards.
        table: Table slug.

    Returns:
        Row count, year range, and the latest ``YYYY-MM`` present in the table.
    """
    prog = PROGRAMS[table]
    series = SERIES_BUILDERS[table](input_dir).drop_duplicates(
        "series_id", ignore_index=True
    )
    stage_dir = stage_root / prog
    raw = shard_by_year(input_dir, prog, stage_dir)
    rows, deduped, latest = 0, 0, (0, 0)
    for d in sorted(stage_dir.glob("year=*"), key=lambda p: int(p.name[5:])):
        sub, n_dedup = build_year(d, series, table)
        deduped += n_dedup
        write_year(sub, table, output_dir)
        rows += len(sub)
        months = sub.loc[sub["month"].notna(), "month"]
        if len(months):
            latest = max(latest, (int(sub["year"].iloc[0]), int(months.max())))
    shutil.rmtree(stage_dir)
    years = sorted(
        int(p.name[5:]) for p in (output_dir / table).glob("year=*")
    )
    # An observation whose series_id is absent from the catalogue is dropped by
    # the inner join. That is silent loss, so it is counted and reported rather
    # than inferred from a row count that looks plausible.
    orphaned = deduped - rows
    log.info(
        f"{table}: {rows:,} rows, {years[0]}-{years[-1]}, "
        f"latest {latest[0]}-{latest[1]:02d} "
        f"(read {raw:,} raw, {raw - deduped:,} duplicate, "
        f"{orphaned:,} without a series record)"
    )
    return {
        "table": table,
        "rows": rows,
        "raw_rows": raw,
        "duplicate_rows": raw - deduped,
        "orphaned_rows": orphaned,
        "year_min": years[0],
        "year_max": years[-1],
        "max_year_month": f"{latest[0]}-{latest[1]:02d}",
    }


# ── dictionary ──────────────────────────────────────────────────────────────
# Coded column -> (program, lookup file, code field, label field). Every entry
# is a column the architecture marks covered_by_dictionary.
_DICT_SOURCES = {
    "ces_national": [
        (
            "seasonal_adjustment",
            "ce",
            "seasonal",
            "seasonal_code",
            "seasonal_text",
        ),
        (
            "period_id",
            "ce",
            "period",
            "period",
            "month",
        ),  # ce/sm label column is `month`
        (
            "supersector_id",
            "ce",
            "supersector",
            "supersector_code",
            "supersector_name",
        ),
        ("industry_id", "ce", "industry", "industry_code", "industry_name"),
        ("data_type_id", "ce", "datatype", "data_type_code", "data_type_text"),
        ("footnote_id", "ce", "footnote", "footnote_code", "footnote_text"),
    ],
    "ces_state_metro": [
        (
            "seasonal_adjustment",
            "sm",
            "seasonal",
            "seasonal_code",
            "seasonal_text",
        ),
        ("period_id", "sm", "period", "period", "month"),
        ("area_id", "sm", "area", "area_code", "area_name"),
        (
            "supersector_id",
            "sm",
            "supersector",
            "supersector_code",
            "supersector_name",
        ),
        ("industry_id", "sm", "industry", "industry_code", "industry_name"),
        (
            "data_type_id",
            "sm",
            "data_type",
            "data_type_code",
            "data_type_text",
        ),
        ("footnote_id", "sm", "footnote", "footnote_code", "footnote_text"),
    ],
    "laus": [
        (
            "seasonal_adjustment",
            "la",
            "seasonal",
            "seasonal_code",
            "seasonal_text",
        ),
        ("period_id", "la", "period", "period", "period_name"),
        ("area_type_id", "la", "area_type", "area_type_code", "areatype_text"),
        ("area_id", "la", "area", "area_code", "area_text"),
        ("measure_id", "la", "measure", "measure_code", "measure_text"),
        ("footnote_id", "la", "footnote", "footnote_code", "footnote_text"),
    ],
    "jolts": [
        (
            "seasonal_adjustment",
            "jt",
            "seasonal",
            "seasonal_code",
            "seasonal_text",
        ),
        ("period_id", "jt", "period", "period", "period_name"),
        ("industry_id", "jt", "industry", "industry_code", "industry_text"),
        (
            "sizeclass_id",
            "jt",
            "sizeclass",
            "sizeclass_code",
            "sizeclass_text",
        ),
        (
            "dataelement_id",
            "jt",
            "dataelement",
            "dataelement_code",
            "dataelement_text",
        ),
        (
            "ratelevel_id",
            "jt",
            "ratelevel",
            "ratelevel_code",
            "ratelevel_text",
        ),
        ("footnote_id", "jt", "footnote", "footnote_code", "footnote_text"),
    ],
}

# JOLTS census regions, which BLS stores in the same field as the state codes.
_REGION_LABELS = {
    "MW": "Midwest region",
    "NE": "Northeast region",
    "SO": "South region",
    "WE": "West region",
}

# The measurement unit is derived here rather than published by BLS, so its
# labels are written out rather than read from a lookup file.
_UNIT_LABELS = {
    "thousand_person": "Thousands of persons",
    "person": "Persons",
    "hour": "Hours per week",
    "thousand_hour": "Thousands of hours",
    "USD": "US dollars",
    "thousand_USD": "Thousands of US dollars",
    "percent": "Percent",
    "index": "Index number",
    "ratio": "Ratio",
}


def _observed(output_dir: Path, table: str, column: str) -> list[str]:
    """Read the distinct non-null values a built table holds in one column.

    Args:
        output_dir: Root output directory.
        table: Table slug.
        column: Column to scan.

    Returns:
        The distinct values, or an empty list if the table has not been built.
    """
    files = sorted((output_dir / table).glob("year=*/data.parquet"))
    if not files:
        return []
    col = (
        ds.dataset([str(f) for f in files], format="parquet")
        .to_table(columns=[column])
        .column(column)
        .combine_chunks()
    )
    # pyrefly: ignore [missing-attribute]
    return pc.unique(col.drop_null()).to_pylist()


def build_dicionario(input_dir: Path, output_dir: Path) -> Path:
    """Build the ``dicionario`` table mapping every coded column to its labels.

    Column names stay Portuguese (``id_tabela``, ``nome_coluna``, ``chave``,
    ``cobertura_temporal``, ``valor``) even though this dataset is English: the
    platform's dictionary renderer expects that schema.

    One BLS field is not a single code. ``footnote_codes`` can carry several
    codes at once, comma-separated (``"E,F"``), which a one-key-one-label
    dictionary cannot represent by reading the lookup file alone. The composite
    values actually present in the built tables are therefore read back and
    given a composite label built from their parts, so every stored value
    resolves. Requires the fact tables to have been built first, which
    :func:`clean_all` guarantees.

    Args:
        input_dir: Root of the downloaded files.
        output_dir: Root output directory, holding the already-built tables.

    Returns:
        The dictionary's output directory.
    """
    rows = []
    for table, sources in _DICT_SOURCES.items():
        for column, prog, fname, code_col, label_col in sources:
            df = read_tsv(input_dir / prog / f"{prog}.{fname}")
            for _, r in df.iterrows():
                rows.append((table, column, r[code_col], None, r[label_col]))
            if column != "footnote_id":
                continue
            labels = dict(zip(df[code_col], df[label_col], strict=True))
            for value in _observed(output_dir, table, column):
                if "," in value:
                    rows.append(
                        (
                            table,
                            column,
                            value,
                            None,
                            "; ".join(
                                labels.get(part, part).rstrip(".")
                                for part in value.split(",")
                            ),
                        )
                    )
        for code, label in _UNIT_LABELS.items():
            rows.append((table, "measurement_unit", code, None, label))
        if table == "jolts":
            for code, label in _REGION_LABELS.items():
                rows.append((table, "region_id", code, None, label))
    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    ).drop_duplicates(["id_tabela", "nome_coluna", "chave"], ignore_index=True)
    schema = pa.schema(
        [
            pa.field(a["name"], PA[a["bigquery_type"]])
            for a in read_arch("dicionario")
        ]
    )
    d = output_dir / "dicionario"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        d / "data.parquet",
        compression="snappy",
    )
    log.info(f"dicionario: {len(df):,} rows -> {d}")
    return d


def clean_all(
    input_dir: Path, output_dir: Path, tables: list[str] | None = None
) -> dict:
    """Build every table from the downloaded flat files.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.us_bls_employment.tasks.clean_employment`) and the
    one-shot bootstrap in ``models/us_bls_employment/code/``.

    Args:
        input_dir: Root of the downloaded files.
        output_dir: Root output directory.
        tables: Data tables to build; defaults to all four.

    Returns:
        Per-table summaries, plus ``"max_year_month"`` — the latest ``YYYY-MM``
        across the programs, used to poll whether BLS has published a new month.
    """
    stage_root = output_dir / "_shards"
    result = {}
    for table in tables or constants.DATA_TABLES.value:
        result[table] = build_program(input_dir, output_dir, stage_root, table)
    result["dicionario"] = {
        "path": str(build_dicionario(input_dir, output_dir))
    }
    if stage_root.exists():
        shutil.rmtree(stage_root)
    months = [
        v["max_year_month"] for v in result.values() if "max_year_month" in v
    ]
    # pyrefly: ignore [unsupported-operation]
    result["max_year_month"] = max(months) if months else None
    return result
