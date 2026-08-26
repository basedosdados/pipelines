"""Pure download and cleaning functions for world_wil_wid (WID.world).

No Prefect imports live here: ``tasks.py`` wraps these, and the one-shot
bootstrap under ``models/world_wil_wid/code/`` imports them directly, so the
transform exists in exactly one place.

The source is a single ~882 MB zip holding 848 ``;``-delimited CSVs:

* ``WID_data_<GEO>.csv``     -- the fact rows, one file per geography
* ``WID_metadata_<GEO>.csv`` -- one row per (geography, variable) series
* ``WID_countries.csv``      -- a geography dimension that covers only 346 of
  the 410 geographies actually present in the data

Four source traps are handled here and each one is silent if you skip it; see
the inline TRAP comments and models/world_wil_wid/ONBOARDING_PLAN.md.

Output is all-STRING Snappy Parquet, hive-partitioned by ``year`` for the
``indicator`` table. Staging is all-STRING by Data Basis convention -- the dbt
model ``safe_cast``s every column to its real type, and
``pipelines.utils.gcs.dump_header`` stringifies the header file BigQuery infers
the staging schema from, so typed parquet is rejected outright. See
[[project_dump_header_parquet_bug]].
"""

from __future__ import annotations

import csv
import re
import shutil
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import requests

from pipelines.datasets.world_wil_wid.constants import constants
from pipelines.utils.utils import log

# --------------------------------------------------------------------------- #
# Source markup
# --------------------------------------------------------------------------- #

# The metadata `source` and `method` fields carry a bespoke hyperlink markup.
# Both tag orders occur in the archive: [URL_LINK] first (the common case) and
# [URL_TEXT] first (67 occurrences). Tags are always balanced, so each [URL]
# block is parsed independently rather than by one fixed-order pattern.
_URL_BLOCK = re.compile(r"\[URL\](.*?)\[/URL\]", re.DOTALL)
_URL_LINK = re.compile(r"\[URL_LINK\](.*?)\[/URL_LINK\]", re.DOTALL)
_URL_TEXT = re.compile(r"\[URL_TEXT\](.*?)\[/URL_TEXT\]", re.DOTALL)


def strip_url_markup(text: str | None) -> str | None:
    """Render WID's ``[URL]…[/URL]`` markup as plain text followed by the URL.

    ``[URL][URL_LINK]https://x[/URL_LINK][URL_TEXT] Chancel (2025) [/URL_TEXT][/URL]``
    becomes ``Chancel (2025) (https://x)``. A block carrying only one of the two
    inner tags degrades to whichever is present rather than being dropped.

    Args:
        text: Raw field value, or None.

    Returns:
        The field with every URL block rewritten, whitespace collapsed.
    """
    if not text:
        return text

    def _one(match: re.Match[str]) -> str:
        inner = match.group(1)
        link_m = _URL_LINK.search(inner)
        text_m = _URL_TEXT.search(inner)
        link = link_m.group(1).strip() if link_m else ""
        label = text_m.group(1).strip() if text_m else ""
        rendered = f"{label} ({link})" if label and link else (label or link)
        # WID's own rendering relies on the anchor tag for separation, so some
        # blocks butt straight up against the next word ("…2020-19/]Technote").
        # Re-insert the separator the markup was standing in for.
        tail = match.string[match.end() : match.end() + 1]
        if rendered and tail and (tail.isalnum() or tail in "([{"):
            rendered += " "
        return rendered

    out = _URL_BLOCK.sub(_one, text)
    return re.sub(r"\s+", " ", out).strip() or None


# --------------------------------------------------------------------------- #
# Architecture (the schema source of truth)
# --------------------------------------------------------------------------- #


def read_arch(table: str) -> list[dict[str, str]]:
    """Read one architecture CSV as an ordered list of column specs.

    Args:
        table: Table slug, e.g. ``indicator``.

    Returns:
        One dict per column, in the architecture's own order.
    """
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def arch_columns(table: str) -> list[str]:
    """Column names for a table, in architecture order."""
    return [spec["name"] for spec in read_arch(table)]


def string_schema(table: str) -> pa.Schema:
    """All-STRING arrow schema for a table, in architecture order."""
    return pa.schema(
        [pa.field(name, pa.string()) for name in arch_columns(table)]
    )


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #


def source_last_modified() -> str | None:
    """Return the bulk archive's ``Last-Modified`` date as ``YYYY-MM-DD``.

    WID publishes no release calendar and no version field. The archive's HTTP
    ``Last-Modified`` is the only release signal the source exposes, and it is
    what the pipeline's poll compares against.

    Returns:
        ISO date string, or None when the header is absent.
    """
    response = requests.head(
        constants.BULK_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        allow_redirects=True,
        timeout=120,
    )
    response.raise_for_status()
    raw = response.headers.get("Last-Modified")
    if not raw:
        return None
    from email.utils import parsedate_to_datetime

    return parsedate_to_datetime(raw).date().isoformat()


def download_bulk(input_dir: Path) -> Path:
    """Stream the full WID archive to ``<input_dir>/wid_all_data.zip``.

    Args:
        input_dir: Directory to write into; created if absent.

    Returns:
        Path to the downloaded zip.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / "wid_all_data.zip"
    with requests.get(
        constants.BULK_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        stream=True,
        timeout=600,
    ) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8 << 20):
                handle.write(chunk)
    log(f"downloaded {target} ({target.stat().st_size / 1e6:.0f} MB)")
    return target


def extract_members(
    zip_path: Path, input_dir: Path
) -> tuple[Path, Path, Path]:
    """Extract the archive into ``data/``, ``metadata/`` and the countries file.

    TRAP 1 -- ``WID_data_Al.csv`` and ``WID_metadata_Al.csv`` are header-only
    artifacts dated 2024-02-14 whose names differ from Albania's real
    ``WID_data_AL.csv`` and ``WID_metadata_AL.csv`` only by case. On a
    case-insensitive filesystem (macOS/APFS, the default) a plain
    ``unzip``/``extractall`` overwrites both of Albania's files with the stubs
    and the country vanishes without any error. Junk members are skipped by
    name, and each member is written under its own geography code so no two
    targets collide.

    Args:
        zip_path: The downloaded archive.
        input_dir: Directory to extract into.

    Returns:
        ``(data_dir, metadata_dir, countries_csv)``.
    """
    data_dir = input_dir / "data"
    meta_dir = input_dir / "metadata"
    for directory in (data_dir, meta_dir):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)

    countries_path = input_dir / constants.COUNTRIES_MEMBER.value
    data_prefix = constants.DATA_PREFIX.value
    meta_prefix = constants.METADATA_PREFIX.value
    junk = constants.JUNK_MEMBERS.value

    n_data = n_meta = n_skipped = 0
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if name in junk:
                n_skipped += 1
                log(
                    f"skipping known junk member {name} (TRAP 1: collides with AL)"
                )
                continue
            if name == constants.COUNTRIES_MEMBER.value:
                with (
                    archive.open(name) as src,
                    countries_path.open("wb") as dst,
                ):
                    shutil.copyfileobj(src, dst)
            elif name.startswith(data_prefix) and name.endswith(".csv"):
                geo = name[len(data_prefix) : -len(".csv")]
                with (
                    archive.open(name) as src,
                    (data_dir / f"{geo}.csv").open("wb") as dst,
                ):
                    shutil.copyfileobj(src, dst)
                n_data += 1
            elif name.startswith(meta_prefix) and name.endswith(".csv"):
                geo = name[len(meta_prefix) : -len(".csv")]
                with (
                    archive.open(name) as src,
                    (meta_dir / f"{geo}.csv").open("wb") as dst,
                ):
                    shutil.copyfileobj(src, dst)
                n_meta += 1

    log(
        f"extracted {n_data} data files, {n_meta} metadata files, {n_skipped} skipped"
    )
    if n_data != n_meta:
        raise ValueError(
            f"data/metadata file counts disagree: {n_data} vs {n_meta}. "
            "The archive layout changed; re-check the member naming."
        )
    return data_dir, meta_dir, countries_path


# --------------------------------------------------------------------------- #
# CSV reading
# --------------------------------------------------------------------------- #

_PARSE = pacsv.ParseOptions(
    delimiter=";", quote_char='"', newlines_in_values=True
)

# TRAP 5 -- pyarrow's default null-value list includes "NA", "N/A", "null",
# "nan" and friends, so Namibia's ISO code `NA` is read as NULL and the country
# disappears from every table without a single error. Only the empty field is a
# null in this source; every other literal is data.
_NULL_VALUES = [""]


def _read_csv_all_string(path: Path, columns: list[str]) -> pa.Table:
    """Read one `;`-delimited WID CSV as an all-STRING table with fixed columns.

    TRAP 2 -- the 76 regional-aggregate files (``WO`` and the ``O*``/``Q*``/``X*``
    prefixes) ship a header without the trailing ``data_quality`` /
    ``data_quality_score`` column. Reading every file against a pinned
    8-column schema and swallowing the mismatch discards all 36.6M aggregate
    rows, the World series included. Here the header is honoured as written and
    any absent column is materialised as nulls, so the union is by name.

    Empty fields become nulls rather than empty strings, which is what the dbt
    ``safe_cast`` expects downstream -- but *only* empty fields, see TRAP 5 on
    ``_NULL_VALUES``.

    Args:
        path: The CSV to read.
        columns: Full expected column list, in source order.

    Returns:
        A table with exactly ``columns``, every field ``pa.string()``.
    """
    table = pacsv.read_csv(
        path,
        parse_options=_PARSE,
        convert_options=pacsv.ConvertOptions(
            column_types={name: pa.string() for name in columns},
            strings_can_be_null=True,
            null_values=_NULL_VALUES,
        ),
    )
    present = set(table.column_names)
    unknown = present - set(columns)
    if unknown:
        raise ValueError(f"{path.name}: unexpected columns {sorted(unknown)}")
    arrays = [
        table.column(name)
        if name in present
        else pa.nulls(table.num_rows, type=pa.string())
        for name in columns
    ]
    return pa.table(
        arrays, schema=pa.schema([pa.field(c, pa.string()) for c in columns])
    )


def _assert_no_nulls(
    table: pa.Table, columns: tuple[str, ...], where: str
) -> None:
    """Fail loudly if a key column picked up nulls.

    The guard exists because of TRAP 5: a CSV reader that silently converts a
    literal like ``NA`` to null empties a key column without raising, and the
    damage only surfaces much later as a missing country. Cheap to check, so it
    is checked on every file.

    Args:
        table: The table to check.
        columns: Key columns that must be fully populated.
        where: Source file name, for the error message.

    Raises:
        ValueError: If any of ``columns`` holds a null.
    """
    for name in columns:
        nulls = table.column(name).null_count
        if nulls:
            raise ValueError(
                f"{where}: {nulls:,} null values in key column {name!r}. "
                "A literal was most likely swallowed as a null sentinel."
            )


def _derive_code_parts(
    variable: pa.ChunkedArray,
) -> tuple[pa.ChunkedArray, ...]:
    """Split a WID variable code into its series type and concept.

    TRAP 3 -- in the bulk CSVs the code reads ``[type][concept][pop][age]``
    (``sptincj992``, ``accmhni992``), not the ``[type][concept][age][pop]``
    order documented in WID's own codes dictionary. Only the first two
    components are derived here; ``pop`` and ``age`` are taken from their own
    source columns, which the archive already provides and which were verified
    to agree with the code suffix on all 395,977 series.

    Args:
        variable: The full variable code column.

    Returns:
        ``(series_type, concept)``.
    """
    # pyrefly: ignore [missing-attribute]
    series_type = pc.utf8_slice_codeunits(variable, 0, 1)
    # pyrefly: ignore [missing-attribute]
    concept = pc.utf8_slice_codeunits(variable, 1, 6)
    return series_type, concept


# --------------------------------------------------------------------------- #
# indicator
# --------------------------------------------------------------------------- #


def build_indicator(data_dir: Path, output_dir: Path) -> int:
    """Build the ``indicator`` table as year-partitioned all-STRING Parquet.

    Two passes, both streaming, so peak memory stays at one geography's file
    rather than the 142M-row whole. Pass one rewrites each geography CSV as a
    single Parquet fragment with the architecture's columns; pass two rewrites
    those fragments as one hive-partitioned dataset keyed on ``year``, which is
    the BigQuery partition column.

    Args:
        data_dir: Directory of ``<GEO>.csv`` fact files.
        output_dir: Root output directory.

    Returns:
        Total row count written.
    """
    columns = arch_columns("indicator")
    schema = string_schema("indicator")
    source_columns = constants.DATA_COLUMNS.value

    staged = output_dir / "_indicator_by_geography"
    if staged.exists():
        shutil.rmtree(staged)
    staged.mkdir(parents=True)

    total = 0
    files = sorted(data_dir.glob("*.csv"))
    for index, path in enumerate(files, start=1):
        raw = _read_csv_all_string(path, source_columns)
        if raw.num_rows == 0:
            continue
        series_type, concept = _derive_code_parts(raw.column("variable"))
        by_name = {
            "year": raw.column("year"),
            "country_code": raw.column("country"),
            "variable": raw.column("variable"),
            "series_type": series_type,
            "concept": concept,
            "pop": raw.column("pop"),
            "age": raw.column("age"),
            "percentile": raw.column("percentile"),
            "value": raw.column("value"),
            "data_quality": raw.column("data_quality"),
        }
        table = pa.table([by_name[name] for name in columns], schema=schema)
        _assert_no_nulls(
            table,
            ("year", "country_code", "variable", "percentile"),
            path.name,
        )
        pq.write_table(
            table, staged / f"{path.stem}.parquet", compression="snappy"
        )
        total += table.num_rows
        if index % 50 == 0:
            log(f"indicator: {index}/{len(files)} geographies, {total:,} rows")

    target = output_dir / "indicator"
    if target.exists():
        shutil.rmtree(target)
    dataset = pads.dataset(staged, format="parquet")
    pads.write_dataset(
        dataset,
        target,
        format="parquet",
        partitioning=pads.partitioning(
            pa.schema([pa.field("year", pa.string())]), flavor="hive"
        ),
        existing_data_behavior="delete_matching",
        basename_template="data_{i}.parquet",
        max_open_files=1024,
        file_options=pads.ParquetFileFormat().make_write_options(
            compression="snappy"
        ),
    )
    shutil.rmtree(staged)
    log(f"indicator: {total:,} rows -> {target}")
    return total


# --------------------------------------------------------------------------- #
# series
# --------------------------------------------------------------------------- #

# metadata column -> architecture column
_SERIES_RENAME = {
    "country": "country_code",
    "variable": "variable",
    "pop": "pop",
    "age": "age",
    "countryname": "country_name",
    "shortname": "name",
    "simpledes": "simple_description",
    "technicaldes": "technical_description",
    "shorttype": "type_name",
    "longtype": "type_description",
    "shortpop": "pop_name",
    "longpop": "pop_description",
    "shortage": "age_name",
    "longage": "age_description",
    "unit": "unit",
    "source": "source",
    "method": "method",
    "extrapolation": "extrapolation",
    "data_points": "data_points",
    "data_quality_score": "data_quality_score",
}

_MARKUP_COLUMNS = ("source", "method")


def build_series(meta_dir: Path, output_dir: Path) -> pa.Table:
    """Build the ``series`` catalogue from the per-geography metadata files.

    The returned table is also the input to :func:`build_country` and
    :func:`build_dicionario`, so the 422 metadata files are parsed once rather
    than once per output table.

    Args:
        meta_dir: Directory of ``<GEO>.csv`` metadata files.
        output_dir: Root output directory.

    Returns:
        The written table.
    """
    columns = arch_columns("series")
    schema = string_schema("series")
    source_columns = constants.METADATA_COLUMNS.value

    tables = []
    for path in sorted(meta_dir.glob("*.csv")):
        raw = _read_csv_all_string(path, source_columns)
        if raw.num_rows == 0:
            continue
        series_type, concept = _derive_code_parts(raw.column("variable"))
        by_name: dict[str, object] = {
            "series_type": series_type,
            "concept": concept,
        }
        for source_name, target_name in _SERIES_RENAME.items():
            column = raw.column(source_name)
            if source_name in _MARKUP_COLUMNS:
                column = pa.chunked_array(
                    [
                        pa.array(
                            [strip_url_markup(v) for v in column.to_pylist()],
                            type=pa.string(),
                        )
                    ]
                )
            by_name[target_name] = column
        tables.append(
            pa.table([by_name[name] for name in columns], schema=schema)
        )

    combined = pa.concat_tables(tables)
    _assert_no_nulls(combined, ("country_code", "variable"), "series")
    target = output_dir / "series"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    pq.write_table(combined, target / "data.parquet", compression="snappy")
    log(f"series: {combined.num_rows:,} rows -> {target}")
    return combined


# --------------------------------------------------------------------------- #
# country
# --------------------------------------------------------------------------- #

_SUBNATIONAL_PARENTS = ("US-", "DE-", "CN-")
_CONVERSIONS = ("MER", "PPP")


def _classify_geography(
    code: str, has_region: bool
) -> tuple[str, str, str | None, str | None]:
    """Classify a WID geography code.

    Classification follows WID's own ``WID_countries.csv`` rather than a guess
    from the code's shape: a geography that WID assigns a continental ``region``
    to is a country, and everything else is a subnational unit or an aggregate.
    Guessing from the prefix gets the historical entities wrong -- ``XC`` is
    former Czechoslovakia, a country, while ``XL`` and ``XR`` are WID regional
    groupings, and both start with ``X``.

    Args:
        code: A WID geography code, e.g. ``BR``, ``US-CA``, ``QE-PPP``, ``WO``.
        has_region: Whether ``WID_countries.csv`` gives the code a region.

    Returns:
        ``(base_code, geography_type, conversion, country_iso2)``.
    """
    base, conversion = code, None
    for suffix in _CONVERSIONS:
        if code.endswith(f"-{suffix}"):
            base, conversion = code[: -(len(suffix) + 1)], suffix
            break

    if has_region:
        return base, "country", conversion, base
    if conversion is None and code.startswith(_SUBNATIONAL_PARENTS):
        return base, "subnational", None, None
    return base, "region", conversion, None


def build_country(
    countries_csv: Path, series: pa.Table, output_dir: Path
) -> int:
    """Build the geography dimension, covering every geography in the data.

    TRAP 4 -- ``WID_countries.csv`` names only 346 of the 410 geographies that
    actually appear in the data. The 67 missing ones are exactly the regional
    aggregates, the World series among them. Their names are recovered from the
    ``country_name`` column of the series catalogue, which does cover them.

    Args:
        countries_csv: The archive's ``WID_countries.csv``.
        series: The catalogue returned by :func:`build_series`.
        output_dir: Root output directory.

    Returns:
        Row count written.
    """
    published: dict[str, dict[str, str | None]] = {}
    with countries_csv.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            published[row["alpha2"]] = {
                "title_name": row.get("titlename") or None,
                "short_name": row.get("shortname") or None,
                "region": row.get("region") or None,
                "region2": row.get("region2") or None,
            }

    # Every geography that actually has series, with the name WID gives it there.
    observed: dict[str, str | None] = dict(
        zip(
            series.column("country_code").to_pylist(),
            series.column("country_name").to_pylist(),
            strict=True,
        )
    )

    recovered = 0
    rows = []
    for code in sorted(set(published) | set(observed)):
        entry = published.get(code)
        if entry is None:
            entry = {
                "title_name": observed.get(code),
                "short_name": observed.get(code),
                "region": None,
                "region2": None,
            }
            recovered += 1
        base, geography_type, conversion, iso2 = _classify_geography(
            code, bool(entry["region"])
        )
        rows.append(
            {
                "country_code": code,
                "base_code": base,
                "country_iso2": iso2,
                "title_name": entry["title_name"],
                "short_name": entry["short_name"],
                "region": entry["region"],
                "region2": entry["region2"],
                "geography_type": geography_type,
                "conversion": conversion,
            }
        )

    columns = arch_columns("country")
    schema = string_schema("country")
    table = pa.table(
        [
            pa.array([row[name] for row in rows], type=pa.string())
            for name in columns
        ],
        schema=schema,
    )
    target = output_dir / "country"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    pq.write_table(table, target / "data.parquet", compression="snappy")
    log(
        f"country: {table.num_rows:,} rows -> {target} "
        f"({recovered} names recovered from metadata, TRAP 4)"
    )
    return table.num_rows


# --------------------------------------------------------------------------- #
# dicionario
# --------------------------------------------------------------------------- #


def build_dicionario(series: pa.Table, output_dir: Path) -> int:
    """Build the code dictionary from WID's own metadata labels.

    Labels are read out of the archive rather than transcribed from the website,
    so the dictionary cannot drift from the data. ``data_quality`` is
    deliberately absent: WID publishes no codebook for it, and inventing labels
    would be worse than leaving the column undocumented.

    Args:
        series: The catalogue returned by :func:`build_series`.
        output_dir: Root output directory.

    Returns:
        Row count written.
    """
    labels: dict[tuple[str, str], str] = {}
    for column, label_column in constants.DICTIONARY_SOURCES.value.items():
        keys = series.column(column).to_pylist()
        values = series.column(label_column).to_pylist()
        for key, value in zip(keys, values, strict=True):
            if key and value and (column, key) not in labels:
                labels[(column, key)] = value

    rows = [
        {
            "id_tabela": table,
            "nome_coluna": column,
            "chave": key,
            "cobertura_temporal": None,
            "valor": value,
        }
        for (column, key), value in sorted(labels.items())
        for table in ("indicator", "series")
    ]

    columns = arch_columns("dicionario")
    schema = string_schema("dicionario")
    table_out = pa.table(
        [
            pa.array([row[name] for row in rows], type=pa.string())
            for name in columns
        ],
        schema=schema,
    )
    target = output_dir / "dicionario"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    pq.write_table(table_out, target / "data.parquet", compression="snappy")
    log(f"dicionario: {table_out.num_rows:,} rows -> {target}")
    return table_out.num_rows


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def clean_all(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Extract the archive and build all four tables.

    Args:
        input_dir: Directory holding ``wid_all_data.zip``.
        output_dir: Root output directory for the partitioned Parquet.

    Returns:
        Row count per table slug.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir, meta_dir, countries_csv = extract_members(
        input_dir / "wid_all_data.zip", input_dir
    )
    series = build_series(meta_dir, output_dir)
    return {
        "indicator": build_indicator(data_dir, output_dir),
        "series": series.num_rows,
        "country": build_country(countries_csv, series, output_dir),
        "dicionario": build_dicionario(series, output_dir),
    }
