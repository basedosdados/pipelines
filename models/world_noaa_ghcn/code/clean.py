"""Pure cleaning transforms for world_noaa_ghcn.

No Prefect imports here — the recurring pipeline imports these same functions
(see .claude/rules/prefect-pipeline-conventions.md, "DRY with the onboarding
code"), so the transform lives in exactly one place.

All parquet written by this module is **all-STRING** with a stable column
order, per .claude/rules/bigquery-conventions.md.  The cast to string goes
through arrow, never `astype(str)`, so NULL stays NULL rather than becoming the
literal "nan" that `safe_cast` cannot undo.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

try:
    from pipelines.datasets.world_noaa_ghcn import constants as c
except ImportError:  # running from models/<ds>/code/ during onboarding
    import constants as c


# --------------------------------------------------------------------------
# all-STRING helpers
# --------------------------------------------------------------------------
def to_all_string(table: pa.Table, column_order: list[str]) -> pa.Table:
    """Cast every column to string, preserving NULLs, in a fixed order.

    Passing the real types through first matters: a float `year` would
    serialise as "1950.0" and `safe_cast(year as int64)` would then yield NULL.
    """
    arrays = []
    for name in column_order:
        col = table.column(name)
        arrays.append(col.cast(pa.string()))
    return pa.Table.from_arrays(
        arrays, schema=pa.schema([(n, pa.string()) for n in column_order])
    )


def write_parquet(table: pa.Table, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    return table.num_rows


# --------------------------------------------------------------------------
# station
# --------------------------------------------------------------------------
STATION_COLUMNS = [
    "station_id",
    "country_code",
    "country_name",
    "network_code",
    "state_code",
    "state_name",
    "station_name",
    "latitude",
    "longitude",
    "elevation",
    "gsn_flag",
    "hcn_crn_flag",
    "wmo_id",
]


def _fixed_width(line: str, start: int, end: int) -> str:
    """1-indexed inclusive slice, per the readme's column tables."""
    return line[start - 1 : end].strip()


def read_code_table(
    path: Path, code_end: int, name_start: int
) -> dict[str, str]:
    out = {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            out[line[:code_end].strip()] = line[name_start - 1 :].strip()
    return out


def clean_stations(input_dir: Path, output_dir: Path) -> int:
    countries = read_code_table(input_dir / "ghcnd-countries.txt", 2, 4)
    states = read_code_table(input_dir / "ghcnd-states.txt", 2, 4)

    cols: dict[str, list] = {k: [] for k in STATION_COLUMNS}
    with open(
        input_dir / "ghcnd-stations.txt", encoding="utf-8", errors="replace"
    ) as fh:
        for line in fh:
            if not line.strip():
                continue
            sid = _fixed_width(line, 1, 11)
            country = sid[:2]
            state = _fixed_width(line, 39, 40)
            elev = float(_fixed_width(line, 32, 37) or "nan")
            cols["station_id"].append(sid)
            cols["country_code"].append(country)
            cols["country_name"].append(countries.get(country))
            cols["network_code"].append(sid[2])
            cols["state_code"].append(state or None)
            cols["state_name"].append(states.get(state))
            cols["station_name"].append(_fixed_width(line, 42, 71) or None)
            cols["latitude"].append(float(_fixed_width(line, 13, 20)))
            cols["longitude"].append(float(_fixed_width(line, 22, 30)))
            cols["elevation"].append(
                None if elev != elev or elev <= c.MISSING_ELEVATION else elev
            )
            cols["gsn_flag"].append(_fixed_width(line, 73, 75) or None)
            cols["hcn_crn_flag"].append(_fixed_width(line, 77, 79) or None)
            cols["wmo_id"].append(_fixed_width(line, 81, 85) or None)

    table = pa.table(
        {
            k: pa.array(
                v,
                type=pa.float64()
                if k in ("latitude", "longitude", "elevation")
                else pa.string(),
            )
            for k, v in cols.items()
        }
    )
    table = to_all_string(table, STATION_COLUMNS)
    return write_parquet(table, output_dir / "station" / "data.parquet")


# --------------------------------------------------------------------------
# station_element_inventory
# --------------------------------------------------------------------------
INVENTORY_COLUMNS = ["station_id", "element", "first_year", "last_year"]


def clean_inventory(input_dir: Path, output_dir: Path) -> int:
    sid, elem, first, last = [], [], [], []
    with open(
        input_dir / "ghcnd-inventory.txt", encoding="utf-8", errors="replace"
    ) as fh:
        for line in fh:
            if not line.strip():
                continue
            sid.append(_fixed_width(line, 1, 11))
            elem.append(_fixed_width(line, 32, 35))
            first.append(int(_fixed_width(line, 37, 40)))
            last.append(int(_fixed_width(line, 42, 45)))
    table = pa.table(
        {
            "station_id": pa.array(sid, pa.string()),
            "element": pa.array(elem, pa.string()),
            "first_year": pa.array(first, pa.int64()),
            "last_year": pa.array(last, pa.int64()),
        }
    )
    table = to_all_string(table, INVENTORY_COLUMNS)
    return write_parquet(
        table, output_dir / "station_element_inventory" / "data.parquet"
    )


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------
DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def build_dicionario(output_dir: Path, elements_present: set[str]) -> int:
    rows: list[tuple[str, str, str, str, str]] = []

    def add(table_id: str, column: str, mapping: dict[str, str]) -> None:
        for key, value in mapping.items():
            rows.append((table_id, column, key, "", value))

    # Both tables carry the same element universe, so both get the same labels.
    # The label states the unit, which varies by element, and marks the codes
    # whose value is not a measurement at all.
    elem_desc = {}
    for e in sorted(elements_present):
        unit = c.ELEMENT_UNITS[e][0]
        label = c.ELEMENT_DESCRIPTIONS[e]
        elem_desc[e] = (
            f"{label} ({unit})" if unit else f"{label} (not a quantity)"
        )
    add("observation", "element", elem_desc)
    add("station_element_inventory", "element", elem_desc)

    for table_id in ("observation",):
        add(table_id, "measurement_flag", c.MEASUREMENT_FLAGS)
        add(table_id, "quality_flag", c.QUALITY_FLAGS)
        add(table_id, "source_flag", c.SOURCE_FLAGS)
    add("station", "network_code", c.NETWORK_CODES)
    add("station", "gsn_flag", c.GSN_FLAGS)
    add("station", "hcn_crn_flag", c.HCN_CRN_FLAGS)

    table = pa.table(
        {
            name: pa.array([r[i] for r in rows], pa.string())
            for i, name in enumerate(DICIONARIO_COLUMNS)
        }
    )
    return write_parquet(table, output_dir / "dicionario" / "data.parquet")


# --------------------------------------------------------------------------
# observation
# --------------------------------------------------------------------------
OBSERVATION_COLUMNS = [
    "year",
    "station_id",
    "date",
    "element",
    "value",
    "measurement_unit",
    "measurement_flag",
    "quality_flag",
    "source_flag",
    "observation_time",
]


def clean_year(year: int, raw_path: Path, output_dir: Path) -> int:
    """Read one by_year csv.gz and write output/observation/year=<y>/data.parquet.

    Keeps every element, converts the raw integer to its standard unit, and
    drops only the -9999 missing sentinel.

    Two things are deliberately NOT filtered:

    * Rows whose ``quality_flag`` is non-blank are KEPT — they failed QC and
      the flag says so; filtering is the user's decision, not ours.
    * The 28 elements in ``NON_QUANTITY_ELEMENTS`` (HHMM clock times and
      weather-type occurrence indicators) are kept with a **null**
      ``measurement_unit``, because their stored value is not a measurement.

    An element code absent from ``ELEMENT_UNITS`` raises rather than silently
    producing a null value — GHCN adds elements between versions, and a new one
    must be mapped, not dropped.
    """
    read_opts = pacsv.ReadOptions(
        column_names=list(c.RAW_COLUMNS), block_size=1 << 26
    )
    parse_opts = pacsv.ParseOptions(delimiter=",")
    convert_opts = pacsv.ConvertOptions(
        column_types={
            "station_id": pa.string(),
            "date": pa.string(),
            "element": pa.string(),
            "value": pa.float64(),
            "measurement_flag": pa.string(),
            "quality_flag": pa.string(),
            "source_flag": pa.string(),
            "observation_time": pa.string(),
        },
        strings_can_be_null=True,
    )
    with gzip.open(raw_path, "rb") as fh:
        table = pacsv.read_csv(fh, read_opts, parse_opts, convert_opts)

    import pyarrow.compute as pc

    # pyrefly: ignore [missing-attribute]  # pyarrow.compute builds these at runtime
    keep = pc.not_equal(table["value"], c.MISSING_VALUE)
    table = table.filter(keep)
    if table.num_rows == 0:
        return 0

    element = table["element"]
    # Fail loud on an element GHCN has added since ELEMENT_UNITS was written:
    # an unmapped code would otherwise take a null divisor and silently null
    # out every one of its values.
    # pyrefly: ignore [missing-attribute]  # pyarrow.compute builds these at runtime
    seen = set(pc.unique(element).to_pylist())
    unknown = sorted(e for e in seen if e not in c.ELEMENT_UNITS)
    if unknown:
        raise ValueError(
            f"{year}: element codes absent from constants.ELEMENT_UNITS: "
            f"{unknown}. Map them in constants.py before loading this year."
        )

    # per-element divisor and unit, looked up by element code
    known = sorted(c.ELEMENT_UNITS)
    divisor = pa.array([c.ELEMENT_UNITS[e][1] for e in known])
    unit = pa.array([c.ELEMENT_UNITS[e][0] for e in known], pa.string())
    # pyrefly: ignore [missing-attribute]  # pyarrow.compute builds these at runtime
    idx = pc.index_in(element, value_set=pa.array(known))
    # pyrefly: ignore [missing-attribute]  # pyarrow.compute builds these at runtime
    value = pc.divide(
        pc.cast(table["value"], pa.float64()),
        pc.cast(pc.take(divisor, idx), pa.float64()),
    )
    measurement_unit = pc.take(unit, idx)

    date_str = table["date"]
    # pyrefly: ignore [missing-attribute]  # pyarrow.compute builds these at runtime
    date = pc.strptime(date_str, format="%Y%m%d", unit="s")
    out = pa.table(
        {
            "year": pa.array([year] * table.num_rows, pa.int64()),
            "station_id": table["station_id"],
            # pyrefly: ignore [missing-attribute]
            "date": pc.strftime(date, format="%Y-%m-%d"),
            "element": element,
            "value": value,
            "measurement_unit": measurement_unit,
            "measurement_flag": table["measurement_flag"],
            "quality_flag": table["quality_flag"],
            "source_flag": table["source_flag"],
            "observation_time": table["observation_time"],
        }
    )
    out = to_all_string(out, OBSERVATION_COLUMNS)
    path = output_dir / "observation" / f"year={year}" / "data.parquet"
    return write_parquet(out, path)
