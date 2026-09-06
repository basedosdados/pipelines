"""Pure download and cleaning functions for us_fema_openfema.

No Prefect imports here: ``tasks.py`` wraps these, and the one-shot onboarding
under ``models/us_fema_openfema/code/`` imports them, so the transform exists
in exactly one place.

The output is hive-partitioned parquet, ``year=<YYYY>/data.parquet``, with
**every column typed STRING**. That is not laziness: ``upload_to_gcs`` builds
the staging table from a one-row header that ``gcs.py::dump_header``
stringifies, so typed parquet is rejected on read. The dbt model ``safe_cast``s
each column back to its architecture type. The cast runs through arrow rather
than ``astype(str)``, which would render NULL as the literal ``"nan"``.
"""

from __future__ import annotations

import csv
import shutil
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests  # pyrefly: ignore [missing-attribute]

from pipelines.datasets.us_fema_openfema.constants import constants

# --------------------------------------------------------------------------
# Architecture: the CSVs are the single source of truth for name, order, type
# --------------------------------------------------------------------------


def read_architecture(table: str) -> list[dict[str, str]]:
    """Return the architecture rows for `table`, in column order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open() as handle:
        return list(csv.DictReader(handle))


def architecture_columns(table: str) -> list[str]:
    return [row["name"] for row in read_architecture(table)]


def architecture_types(table: str) -> dict[str, str]:
    return {
        row["name"]: row["bigquery_type"] for row in read_architecture(table)
    }


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def download_table(table: str, input_dir: Path) -> Path:
    """Stream the source parquet for `table` to `input_dir`, return its path.

    Written to a `.part` file and renamed on success, so an interrupted
    download is never mistaken for a complete one.
    """
    _, _, url = constants.SOURCES.value[table]
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / f"{table}.parquet"
    partial = target.with_suffix(".parquet.part")

    for attempt in range(1, constants.DOWNLOAD_ATTEMPTS.value + 1):
        done = partial.stat().st_size if partial.exists() else 0
        headers = {"Range": f"bytes={done}-"} if done else {}
        with requests.get(
            url,
            stream=True,
            timeout=constants.REQUEST_TIMEOUT.value,
            headers=headers,
        ) as response:
            if done and response.status_code == 200:
                # The server ignored the range and restarted the whole file.
                done = 0
            elif done and response.status_code != 206:
                response.raise_for_status()
            else:
                response.raise_for_status()
            # requests does not decode Content-Encoding on `.raw`; copyfileobj
            # on it would write compressed bytes to disk.
            response.raw.decode_content = True
            with partial.open("ab" if done else "wb") as handle:
                shutil.copyfileobj(response.raw, handle, length=1024 * 1024)

        # A truncated stream still returns HTTP 200 and the 3.7 GB policy file
        # does stall part-way, so the file is only accepted once parquet can
        # read its footer. Anything else is resumed from where it stopped.
        try:
            pq.ParquetFile(partial)
        except Exception as error:  # any read failure means resume
            if attempt == constants.DOWNLOAD_ATTEMPTS.value:
                raise RuntimeError(
                    f"{table}: {partial.stat().st_size:,} bytes downloaded but "
                    f"the parquet footer is unreadable after {attempt} "
                    f"attempts ({error})"
                ) from error
            print(
                f"  {table}: incomplete at {partial.stat().st_size:,} bytes, "
                f"resuming (attempt {attempt + 1})"
            )
            continue
        partial.replace(target)
        return target

    raise RuntimeError(f"{table}: download did not complete")


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------


def _pad(array: pa.Array, width: int) -> pa.Array:
    """Left-pad with zeros to `width`. Empty strings become null first."""
    array = pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.equal(array, ""),  # pyrefly: ignore [missing-attribute]
        pa.nulls(len(array), pa.string()),
        array,  # pyrefly: ignore [missing-attribute]
    )
    return pc.utf8_lpad(array, width, padding="0")  # pyrefly: ignore [missing-attribute]


def _blank_to_null(array: pa.Array) -> pa.Array:
    return pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.equal(array, ""),  # pyrefly: ignore [missing-attribute]
        pa.nulls(len(array), pa.string()),
        array,  # pyrefly: ignore [missing-attribute]
    )


def _county_id(state: pa.Array, county: pa.Array) -> pa.Array:
    """state(2) + county(3), null wherever either side is missing."""
    from pipelines.datasets.us_fema_openfema import spec

    county = pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.is_in(county, value_set=pa.array(sorted(spec.NO_COUNTY))),  # pyrefly: ignore [missing-attribute]
        pa.nulls(len(county), pa.string()),
        county,
    )
    return pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.or_(pc.is_null(state), pc.is_null(county)),  # pyrefly: ignore [missing-attribute]
        pa.nulls(len(county), pa.string()),
        pc.binary_join_element_wise(state, county, ""),  # pyrefly: ignore [missing-attribute]
    )


def _set(batch: pa.Table, name: str, array: pa.Array) -> pa.Table:
    return batch.set_column(batch.schema.get_field_index(name), name, array)


def _derive(table: str, batch: pa.Table) -> pa.Table:
    """Add the columns computed during cleaning, per table."""
    from pipelines.datasets.us_fema_openfema import spec

    if table == "disaster_declaration":
        state = _pad(batch["state_id"], 2)
        county_id = _county_id(state, _pad(batch["fips_county_code"], 3))
        return _set(batch, "state_id", state).append_column(
            "county_id", county_id
        )

    if table == "public_assistance_project":
        # Neither code is zero-padded at source (state runs 1-4 characters,
        # county 1-3), and two Pacific territories carry a FEMA-internal code
        # rather than their FIPS code.
        state = batch["state_id"].combine_chunks()
        state = _blank_to_null(state)
        for wrong, right in spec.PA_STATE_CODE_FIX.items():
            state = pc.if_else(pc.equal(state, wrong), right, state)  # pyrefly: ignore [missing-attribute]
        state = _pad(state, 2)
        county_id = _county_id(state, _pad(batch["county_code"], 3))
        return _set(batch, "state_id", state).append_column(
            "county_id", county_id
        )

    if table in ("nfip_claim", "nfip_policy"):
        geoid = _blank_to_null(batch["census_block_group_id"])
        batch = _set(batch, "census_block_group_id", geoid)
        # The block group id is state(2) + county(3) + tract(6) + group(1), so
        # anything not exactly 12 characters cannot be sliced safely.
        well_formed = pc.equal(pc.utf8_length(geoid), 12)  # pyrefly: ignore [missing-attribute]
        nulls = pa.nulls(len(geoid), pa.string())
        tract = pc.if_else(  # pyrefly: ignore [missing-attribute]
            well_formed,
            pc.utf8_slice_codeunits(geoid, 0, 11),  # pyrefly: ignore [missing-attribute]
            nulls,  # pyrefly: ignore [missing-attribute]
        )
        batch = batch.append_column("census_tract_id", tract)
        if table == "nfip_policy":
            county = pc.if_else(  # pyrefly: ignore [missing-attribute]
                well_formed,
                pc.utf8_slice_codeunits(geoid, 0, 5),  # pyrefly: ignore [missing-attribute]
                nulls,  # pyrefly: ignore [missing-attribute]
            )
            batch = batch.append_column(
                "county_id",
                _county_id(
                    pc.utf8_slice_codeunits(county, 0, 2),  # pyrefly: ignore [missing-attribute]
                    pc.utf8_slice_codeunits(county, 2, 5),  # pyrefly: ignore [missing-attribute]
                ),
            )
        return batch

    raise ValueError(f"no derivation rule for {table!r}")


def _check_geography(table: str, batch: pa.Table) -> None:
    """Fail loudly if a geographic key came out the wrong width.

    A silently malformed county id joins to nothing, and the directory
    relationship test would only catch it much later.
    """
    for column, width in (
        ("state_id", 2),
        ("county_id", 5),
        ("census_tract_id", 11),
        ("census_block_group_id", 12),
    ):
        if column not in batch.column_names:
            continue
        array = batch[column].combine_chunks()
        bad = pc.sum(  # pyrefly: ignore [missing-attribute]
            pc.cast(
                pc.and_(  # pyrefly: ignore [missing-attribute]
                    pc.is_valid(array),  # pyrefly: ignore [missing-attribute]
                    pc.not_equal(pc.utf8_length(array), width),  # pyrefly: ignore [missing-attribute]
                ),
                pa.int64(),
            )
        ).as_py()
        if bad:
            sample = array.filter(
                pc.and_(  # pyrefly: ignore [missing-attribute]
                    pc.is_valid(array),  # pyrefly: ignore [missing-attribute]
                    pc.not_equal(pc.utf8_length(array), width),  # pyrefly: ignore [missing-attribute]
                )
            )[:5].to_pylist()
            raise ValueError(
                f"{table}.{column}: {bad:,} values are not {width} characters "
                f"(e.g. {sample})"
            )


def _normalise(table: str, batch: pa.Table) -> pa.Table:
    """Source-specific cleanups that are not pure renames."""
    if "reported_zip_code" in batch.column_names:
        zip_code = _blank_to_null(batch["reported_zip_code"])
        # 167 values arrive as ZIP+4; truncate to the documented 5 digits,
        # which is both the stated grain and the more conservative choice.
        zip_code = pc.utf8_slice_codeunits(zip_code, 0, 5)  # pyrefly: ignore [missing-attribute]
        zip_code = _pad(zip_code, 5)
        batch = batch.set_column(
            batch.schema.get_field_index("reported_zip_code"),
            "reported_zip_code",
            zip_code,
        )
    for column in ("state_abbreviation", "county_id", "census_block_group_id"):
        if column in batch.column_names:
            idx = batch.schema.get_field_index(column)
            batch = batch.set_column(
                idx, column, _blank_to_null(batch[column])
            )
    return batch


def _to_string_table(batch: pa.Table, columns: list[str]) -> pa.Table:
    """Project to `columns` in order and cast every one of them to STRING."""
    arrays = []
    for name in columns:
        array = batch[name].combine_chunks()
        if pa.types.is_timestamp(array.type) and array.type.tz is not None:
            # A tz-aware timestamp stringifies with a trailing 'Z', which
            # safe_cast(... as DATETIME) silently turns into NULL.
            array = pc.cast(array, pa.timestamp("us"))
        if array.type != pa.string():
            array = pc.cast(array, pa.string())
        arrays.append(array)
    return pa.Table.from_arrays(arrays, names=columns)


def clean_table(
    table: str,
    input_path: Path,
    output_dir: Path,
    batch_rows: int | None = None,
) -> dict[int, int]:
    """Clean one source parquet into `output_dir/<table>/year=YYYY/data.parquet`.

    Streams by record batch and keeps one open writer per year, so peak memory
    is a batch rather than the whole file — the policy file is 74.3M rows.

    Returns {year: row count}.
    """
    from pipelines.datasets.us_fema_openfema import (
        spec,  # local, avoids a cycle
    )

    cfg = spec.TABLES[table]
    rename = {**spec.HARMONISE, **cfg["rename"]}
    partition_source = cfg["partition_source"]
    columns = architecture_columns(table)
    body = [c for c in columns if c != cfg["partition"]]

    target = output_dir / table
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    writers: dict[int, pq.ParquetWriter] = {}
    counts: dict[int, int] = {}
    dropped_no_year = 0
    schema = pa.schema([pa.field(c, pa.string()) for c in columns])

    parquet = pq.ParquetFile(input_path)
    try:
        for raw in parquet.iter_batches(
            batch_size=batch_rows or constants.BATCH_ROWS.value
        ):
            batch = pa.Table.from_batches([raw])
            batch = _rename_and_drop(batch, rename, spec.DROP)
            year = _partition_year(batch, partition_source)
            batch = _derive(table, batch)
            batch = _normalise(table, batch)
            _check_geography(table, batch)
            batch = _to_string_table(batch, body)
            batch = batch.append_column(
                cfg["partition"], pc.cast(year, pa.string())
            )
            batch = batch.select(columns)

            valid = pc.is_valid(year)  # pyrefly: ignore [missing-attribute]
            dropped_no_year += (
                len(year) - pc.sum(pc.cast(valid, pa.int64())).as_py()  # pyrefly: ignore [missing-attribute]
            )
            batch = batch.filter(valid)
            years = year.filter(valid)
            if batch.num_rows == 0:
                continue

            for value in pc.unique(years).to_pylist():  # pyrefly: ignore [missing-attribute]
                part = batch.filter(pc.equal(years, value))  # pyrefly: ignore [missing-attribute]
                if value not in writers:
                    directory = target / f"{cfg['partition']}={value}"
                    directory.mkdir(parents=True, exist_ok=True)
                    writers[value] = pq.ParquetWriter(
                        directory / "data.parquet",
                        schema,
                        compression="snappy",
                    )
                    counts[value] = 0
                writers[value].write_table(part.select(columns).cast(schema))
                counts[value] += part.num_rows
    finally:
        for writer in writers.values():
            writer.close()

    if dropped_no_year:
        print(
            f"  {table}: dropped {dropped_no_year:,} rows with no "
            f"{partition_source} (cannot be partitioned)"
        )
    return dict(sorted(counts.items()))


def _rename_and_drop(
    batch: pa.Table, rename: dict[str, str], drop: set[str]
) -> pa.Table:
    names, keep = [], []
    for name in batch.column_names:
        snake = to_snake(name)
        if snake in drop:
            continue
        keep.append(name)
        names.append(rename.get(snake, snake))
    return batch.select(keep).rename_columns(names)


def _partition_year(batch: pa.Table, source: str) -> pa.Array:
    array = batch[source].combine_chunks()
    if pa.types.is_integer(array.type):
        return pc.cast(array, pa.int64())
    return pc.cast(pc.year(array), pa.int64())  # pyrefly: ignore [missing-attribute]


def to_snake(name: str) -> str:
    import re

    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


def clean_all(
    input_dir: Path, output_dir: Path, only: Iterator[str] | None = None
) -> dict[str, dict[int, int]]:
    result = {}
    for table in constants.SOURCES.value:
        if only is not None and table not in only:
            continue
        path = input_dir / f"{table}.parquet"
        counts = clean_table(table, path, output_dir)
        result[table] = counts
        print(
            f"{table:<28} {sum(counts.values()):>12,} rows  "
            f"{len(counts):>3} partitions  "
            f"{min(counts)}-{max(counts)}"
        )
    return result


def write_dicionario(output_dir: Path) -> int:
    """Copy the committed dictionary CSV to `output_dir` as all-STRING parquet.

    The dictionary is static — it is generated from the source's own field
    dictionary by ``models/us_fema_openfema/code/build_dicionario.py`` and
    committed, so it is read rather than rebuilt at pipeline time. It carries
    no date column and is therefore not partitioned.
    """
    source = Path(constants.ARCHITECTURE_DIR.value).parent / "dicionario.csv"
    columns = architecture_columns("dicionario")
    with source.open() as handle:
        rows = list(csv.DictReader(handle))
    table = pa.Table.from_arrays(
        [pa.array([row[c] for row in rows], pa.string()) for c in columns],
        names=columns,
    )
    target = output_dir / "dicionario"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    pq.write_table(table, target / "data.parquet", compression="snappy")
    return table.num_rows
