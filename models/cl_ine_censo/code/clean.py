"""Clean the INE Chile Censo 2024 sources into partitioned, all-STRING parquet.

Six tables:

  persona          18,480,432 rows   microdata, person level
  hogar             6,622,597 rows   microdata, household level
  vivienda          7,664,466 rows   microdata, dwelling level
  manzana_entidad     244,756 rows   block/entity aggregates + GEOGRAPHY
  zona_localidad       14,647 rows   zone/locality aggregates + GEOGRAPHY
  dicionario            1,554 rows   value -> label for every coded column

Staging is all-STRING by house convention: the dbt model safe_casts each column
to its architecture type.

MEMORY: every table is streamed in Arrow record batches and never materialised
whole. The first version of this script did ``pq.read_table(...).to_pandas()``
and then built a second full Arrow copy to stringify it; on ``persona``
(18,480,432 rows x 63 columns) that needed well over 30 GB and killed a 16 GB
machine. Two things fixed it:

  * Stream: peak memory is now one batch (~250k rows), not one table.
  * Stay in Arrow: the pandas round-trip was what turned nullable int32 columns
    into float64, which is why the old code needed float->int detection to stop
    ``1959`` serialising as ``1959.0``. Arrow keeps int32 with native nulls, so
    the problem cannot arise.

Usage::

    python clean.py                 # all tables
    python clean.py persona hogar   # a subset
"""

from __future__ import annotations

import shutil
import sys
from collections.abc import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from constants import (
    CARTOGRAPHY_DIR,
    CARTOGRAPHY_LEVEL_NAMES,
    CARTOGRAPHY_NAME_RENAMES,
    CARTOGRAPHY_UNIONS,
    CENSUS_YEAR,
    CUT_WIDTHS,
    GEOGRAPHY_RENAMES,
    GEOMETRY_COLUMN,
    MICRODATA_DIR,
    OUTPUT_DIR,
    QUANTITY_COLUMNS,
    SENTINEL_ANONYMISED,
    SENTINEL_NO_RESPONSE,
    WKT_ROUNDING_PRECISION,
)
from dictionary import (
    dictionary_rows,
    load_redatam_dictionary,
    unlabelled_codes,
)
from shapely import from_wkb, to_wkt

MICRODATA_FILES = {
    "persona": "personas_censo2024.parquet",
    "hogar": "hogares_censo2024.parquet",
    "vivienda": "viviendas_censo2024.parquet",
}

EXPECTED_ROWS = {
    "persona": 18_480_432,
    "hogar": 6_622_597,
    "vivienda": 7_664_466,
    "manzana_entidad": 244_756,
    "zona_localidad": 14_647,
    "dicionario": 1_554,
}

PARTITION_COLUMNS = ("ano", "id_region")

# Rows per streamed batch. 250k x 63 narrow columns is a few hundred MB at the
# string-cast stage - the whole point is that this number, not the table size,
# sets peak memory. Geometry batches are smaller because a single WKT cell can
# reach 7.5 MB.
BATCH_ROWS = 250_000
GEOMETRY_BATCH_ROWS = 20_000

# Dropped from the cartography.
#
# OBJECTID / SHAPE_bbox are ArcGIS bookkeeping with no analytical meaning.
#
# SHAPE_Length / SHAPE_Area are dropped too, and that needs justifying: they look
# like a free perimeter and area, but ArcGIS computed them in the layer's own
# GEOGRAPHIC CRS, so their units are DEGREES and SQUARE DEGREES, not metres
# (measured: shape_area median 5.3e-4, shape_length median 0.13). A square degree
# is not a fixed area, and Chile spans 17S to 56S, so the same value means very
# different things in Arica and in Magallanes. Shipping them as "area" would
# invite exactly the wrong comparison. Users who want area should call
# ST_AREA(geometria) in BigQuery, which returns true square metres on GEOGRAPHY.
CARTOGRAPHY_DROP = ("OBJECTID", "SHAPE_bbox", "SHAPE_Length", "SHAPE_Area")


# --------------------------------------------------------------------------
# arrow helpers - all operate on one batch at a time
# --------------------------------------------------------------------------
def stringify(column: pa.Array | pa.ChunkedArray) -> pa.Array:
    """Cast one column to STRING, preserving NULL as NULL.

    Arrow's cast leaves nulls as nulls. The thing to avoid is a pandas round
    trip, where a nullable integer becomes float64 and then serialises with a
    trailing ``.0`` (and NULL becomes the literal ``"nan"``, which safe_cast will
    not turn back into NULL).
    """
    if pa.types.is_string(column.type):
        return column
    if pa.types.is_floating(column.type):
        # Only reachable for genuinely fractional source columns; integral data
        # arrives as int32/int64 and never passes through here.
        return pc.cast(column, pa.string())
    return pc.cast(column, pa.string())


def pad_cut(column: pa.Array, width: int) -> pa.Array:
    """Zero-pad a territorial code to its CUT width, as a string."""
    return pc.ascii_lpad(
        pc.cast(column, pa.string()), width=width, padding="0"
    )


def null_sentinels(column: pa.Array) -> pa.Array:
    """Map the -99 / -66 sentinels to NULL.

    Applied to genuine quantity columns only. Coded columns keep their sentinels
    as literal values, because the dicionario labels them ("No respuesta",
    "Valor suprimido por anonimizacion") and that information is worth keeping.
    In a quantity the same codes would poison every mean and sum, so they become
    NULL and the loss is recorded in the architecture `observations`.
    """
    mask = pc.is_in(
        column, value_set=pa.array([SENTINEL_NO_RESPONSE, SENTINEL_ANONYMISED])
    )
    return pc.if_else(mask, pa.scalar(None, type=column.type), column)


def write_stream(
    batches: Iterator[pa.RecordBatch],
    schema: pa.Schema,
    table: str,
    partition_columns: tuple[str, ...],
) -> int:
    """Write a stream of all-STRING batches as hive-partitioned parquet."""
    destination = OUTPUT_DIR / table
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    counter = {"rows": 0}

    def counted() -> Iterator[pa.RecordBatch]:
        for batch in batches:
            counter["rows"] += batch.num_rows
            print(f"    {counter['rows']:>12,} rows", end="\r", flush=True)
            yield batch

    reader = pa.RecordBatchReader.from_batches(schema, counted())
    partitioning = (
        pads.partitioning(
            pa.schema([(c, pa.string()) for c in partition_columns]),
            flavor="hive",
        )
        if partition_columns
        else None
    )
    pads.write_dataset(
        reader,
        base_dir=str(destination),
        format="parquet",
        partitioning=partitioning,
        basename_template="data-{i}.parquet",
        existing_data_behavior="overwrite_or_ignore",
        file_options=pads.ParquetFileFormat().make_write_options(
            compression="snappy"
        ),
        max_rows_per_group=64_000,
    )
    print(f"    wrote {counter['rows']:,} rows -> {destination}        ")
    return counter["rows"]


# --------------------------------------------------------------------------
# microdata
# --------------------------------------------------------------------------
def microdata_schema(table: str) -> pa.Schema:
    """The all-STRING output schema, in final column order."""
    source = pq.ParquetFile(MICRODATA_DIR / MICRODATA_FILES[table])
    names = [GEOGRAPHY_RENAMES.get(n, n) for n in source.schema_arrow.names]
    ordered = ["ano", "id_region", "id_provincia", "id_comuna"]
    ordered += [n for n in names if n.startswith("id_") and n not in ordered]
    ordered += [n for n in names if n not in ordered]
    return pa.schema([(n, pa.string()) for n in ordered])


def microdata_batches(
    table: str, schema: pa.Schema
) -> Iterator[pa.RecordBatch]:
    source = pq.ParquetFile(MICRODATA_DIR / MICRODATA_FILES[table])
    quantities = set(QUANTITY_COLUMNS[table])

    for batch in source.iter_batches(batch_size=BATCH_ROWS):
        columns: dict[str, pa.Array] = {}
        for name in batch.schema.names:
            column = batch.column(name)
            renamed = GEOGRAPHY_RENAMES.get(name, name)

            if name in quantities:
                column = null_sentinels(column)

            if renamed in CUT_WIDTHS:
                # INE ships these as integers, so Iquique's comuna reads 1101
                # rather than the five-digit CUT 01101 that br_bd_diretorios_cl
                # keys on. Padding is what makes the directory FKs resolve.
                columns[renamed] = pad_cut(column, CUT_WIDTHS[renamed])
                continue

            column = stringify(column)
            if pa.types.is_string(column.type):
                column = pc.utf8_trim_whitespace(column)
            columns[renamed] = column

        columns["ano"] = pa.array(
            [str(CENSUS_YEAR)] * batch.num_rows, pa.string()
        )
        yield pa.RecordBatch.from_arrays(
            [columns[n] for n in schema.names], schema=schema
        )


def clean_microdata(table: str) -> int:
    print(f"[{table}] streaming {MICRODATA_FILES[table]}")
    schema = microdata_schema(table)
    return write_stream(
        microdata_batches(table, schema), schema, table, PARTITION_COLUMNS
    )


# --------------------------------------------------------------------------
# cartography
# --------------------------------------------------------------------------
def cartography_layer_path(layer: str):
    return CARTOGRAPHY_DIR / f"Cartografia_censo2024_Pais_{layer}.parquet"


def cartography_schema(table: str) -> pa.Schema:
    """Output schema for a cartography union, in final column order.

    Built from the WIDE layer: the narrow layer is a strict subset, which
    ``clean_cartography`` verifies rather than assumes.
    """
    wide_layer, narrow_layer = CARTOGRAPHY_UNIONS[table]
    wide = pq.ParquetFile(
        cartography_layer_path(wide_layer)
    ).schema_arrow.names
    narrow = pq.ParquetFile(
        cartography_layer_path(narrow_layer)
    ).schema_arrow.names

    extra = set(narrow) - set(wide)
    if extra:
        raise ValueError(
            f"{narrow_layer} has columns {sorted(extra)} absent from {wide_layer}; "
            "the union is no longer safe, re-check INE's layer schemas."
        )

    names = [
        CARTOGRAPHY_NAME_RENAMES.get(
            n.lower(),
            {
                "cut": "id_comuna",
                "cod_region": "id_region",
                "cod_provincia": "id_provincia",
            }.get(n.lower(), n.lower()),
        )
        for n in wide
        if n not in CARTOGRAPHY_DROP and n != "SHAPE"
    ]
    lead = [
        "ano",
        "id_region",
        "id_provincia",
        "id_comuna",
        "nivel_geografico",
    ]
    ordered = lead + [n for n in names if n not in lead]
    ordered += [GEOMETRY_COLUMN]
    return pa.schema([(n, pa.string()) for n in ordered])


def cartography_batches(
    table: str, schema: pa.Schema
) -> Iterator[pa.RecordBatch]:
    renames = {
        "cut": "id_comuna",
        "cod_region": "id_region",
        "cod_provincia": "id_provincia",
        **CARTOGRAPHY_NAME_RENAMES,
    }
    for layer in CARTOGRAPHY_UNIONS[table]:
        print(f"\n[{table}] {layer}")
        source = pq.ParquetFile(cartography_layer_path(layer))
        level = pa.scalar(CARTOGRAPHY_LEVEL_NAMES[layer], pa.string())

        for batch in source.iter_batches(batch_size=GEOMETRY_BATCH_ROWS):
            columns: dict[str, pa.Array] = {}
            for name in batch.schema.names:
                if name in CARTOGRAPHY_DROP:
                    continue
                column = batch.column(name)
                if name == "SHAPE":
                    # WKB -> WKT. The cast to GEOGRAPHY happens in the dbt model.
                    columns[GEOMETRY_COLUMN] = pa.array(
                        [
                            None
                            if g is None
                            else to_wkt(
                                g, rounding_precision=WKT_ROUNDING_PRECISION
                            )
                            for g in from_wkb(
                                column.to_numpy(zero_copy_only=False)
                            )
                        ],
                        pa.string(),
                    )
                    continue

                renamed = renames.get(name.lower(), name.lower())
                if renamed in CUT_WIDTHS:
                    columns[renamed] = pad_cut(column, CUT_WIDTHS[renamed])
                    continue
                column = stringify(column)
                if pa.types.is_string(column.type):
                    column = pc.utf8_trim_whitespace(column)
                columns[renamed] = column

            columns["ano"] = pa.array(
                [str(CENSUS_YEAR)] * batch.num_rows, pa.string()
            )
            columns["nivel_geografico"] = pa.array(
                [level] * batch.num_rows, pa.string()
            )

            # The narrow layer lacks some of the wide layer's columns; fill them
            # with nulls so every batch matches the union schema.
            for name in schema.names:
                if name not in columns:
                    columns[name] = pa.nulls(batch.num_rows, pa.string())

            yield pa.RecordBatch.from_arrays(
                [columns[n] for n in schema.names], schema=schema
            )


def clean_cartography(table: str) -> int:
    print(f"[{table}] streaming {' + '.join(CARTOGRAPHY_UNIONS[table])}")
    schema = cartography_schema(table)
    return write_stream(
        cartography_batches(table, schema), schema, table, PARTITION_COLUMNS
    )


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------
def clean_dictionary() -> int:
    variables = load_redatam_dictionary()

    table_columns: dict[str, list[str]] = {}
    observed: dict[tuple[str, str], set[str]] = {}
    for table, filename in MICRODATA_FILES.items():
        source = pq.ParquetFile(MICRODATA_DIR / filename)
        coded = [
            c
            for c in source.schema_arrow.names
            if c in variables
            and variables[c].is_coded
            and c not in QUANTITY_COLUMNS[table]
        ]
        table_columns[table] = coded
        seen: dict[str, set[str]] = {c: set() for c in coded}
        # Streamed for the same reason as everything else: collecting distinct
        # codes needs one batch at a time, not the whole column.
        for batch in source.iter_batches(batch_size=BATCH_ROWS, columns=coded):
            for column in coded:
                values = pc.unique(batch.column(column)).drop_null()
                seen[column].update(
                    v if isinstance(v, str) else str(v)
                    for v in values.to_pylist()
                )
        for column in coded:
            observed[(table, column)] = seen[column]

    # A code in the data with no label is a defect. Fail loudly rather than
    # shipping a dictionary that silently fails custom_dictionary_coverage later.
    gaps = unlabelled_codes(variables, table_columns, observed)
    if gaps:
        raise ValueError(
            "codes present in the data with no label: "
            + "; ".join(
                f"{t}.{c}={sorted(m)}" for (t, c), m in sorted(gaps.items())
            )
        )

    frame = pd.DataFrame(dictionary_rows(variables, table_columns, observed))
    schema = pa.schema([(c, pa.string()) for c in frame.columns])
    batch = pa.RecordBatch.from_pandas(
        frame, schema=schema, preserve_index=False
    )
    return write_stream(iter([batch]), schema, "dicionario", ())


# --------------------------------------------------------------------------
def main(requested: list[str]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    targets = requested or [
        "dicionario",
        "zona_localidad",
        "manzana_entidad",
        "hogar",
        "vivienda",
        "persona",
    ]
    counts: dict[str, int] = {}

    for table in targets:
        if table in MICRODATA_FILES:
            counts[table] = clean_microdata(table)
        elif table in CARTOGRAPHY_UNIONS:
            counts[table] = clean_cartography(table)
        elif table == "dicionario":
            counts[table] = clean_dictionary()
        else:
            raise SystemExit(f"unknown table: {table}")

    print("\n=== row counts ===")
    failures = []
    for table, count in counts.items():
        expected = EXPECTED_ROWS.get(table)
        flag = ""
        if expected is not None:
            if count == expected:
                flag = " OK"
            else:
                flag = f" MISMATCH (expected {expected:,})"
                failures.append(table)
        print(f"  {table:18s} {count:>12,}{flag}")
    if failures:
        raise SystemExit(f"row-count mismatch in: {', '.join(failures)}")


if __name__ == "__main__":
    main(sys.argv[1:])
