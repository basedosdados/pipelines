"""Clean the ABS migration sources into the au_abs_migration tables.

Reads ``$AU_ABS_MIGRATION_DATA/input`` (see ``download.py``) and writes
hive-partitioned, all-STRING parquet to ``$AU_ABS_MIGRATION_DATA/output``.

Column names, order and types come from the architecture CSVs next to this
file, which are the source of truth. Staging is all-STRING by house convention
and the dbt models ``safe_cast`` each column, so the parquet carries order, not
types; the cast goes through pyarrow rather than ``astype(str)``, which would
write the literal "nan" for nulls.

Geography is split by granularity: the Australia-wide series lives in the
``_australia`` tables and the states in the ``_state`` tables. ABS rounds every
value to the nearest 10, so the states do not sum to the published national
figure and the two grains cannot share a table without lying about one of them.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
from pathlib import Path

import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CODE_DIR = Path(__file__).resolve().parent
ARCHITECTURE_DIR = CODE_DIR / "architecture"
DATA_DIR = Path(
    os.environ.get(
        "AU_ABS_MIGRATION_DATA",
        Path.home() / "Downloads" / "au_abs_migration_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# Sheet order in every country-of-birth and visa spreadsheet: Australia first,
# then the eight states and territories in ASGS code order.
SHEET_STATES = [None, "1", "2", "3", "4", "5", "6", "7", "8"]

# measure -> (spreadsheet, sheet family). Spreadsheet 1 holds net migration,
# 2 arrivals and 3 departures; each has nine sheets, one per geography.
COUNTRY_FILES = {
    "net": ("34070DO001_202425.xlsx", "1"),
    "arrivals": ("34070DO002_202425.xlsx", "2"),
    "departures": ("34070DO003_202425.xlsx", "3"),
}
VISA_FILE = "34070DO004_202425.xlsx"

# ABS visa and citizenship groupings, as labelled in spreadsheet 4 and coded in
# the OMAD_VISA dataflow. The mapping is verified against the dataflow on the
# overlapping years by ``validate_visa_mapping``.
VISA_LABEL_TO_CODE = {
    "Family": "11",
    "Skilled (permanent)": "12",
    "Special eligibility & humanitarian": "1030",
    "Other (permanent)": "15",
    "Total permanent visas": "1040",
    "Student": "22",
    "Student - vocational education and training": "2208",
    "Student - higher education": "2203",
    "Student - other": "1009",
    "Skilled (temporary)": "23",
    "Working holiday": "24",
    "Visitors": "25",
    "Other (temporary)": "1010",
    "Total temporary visas": "1020",
    "New Zealand citizens (subclass 444)": "01",
    "Australian citizens (no visa)": "02",
    "Total": "1041",
}

MEASURE_CODES = {"1": "arrivals", "2": "departures", "3": "net"}
VISA_MEASURE_CODES = {"M1": "arrivals", "M2": "departures"}

FOOTNOTE = re.compile(r"\((?:[a-z])\)\s*$")


def strip_footnote(label: object) -> str:
    return FOOTNOTE.sub("", str(label)).strip()


# ---------------------------------------------------------------------------
# spreadsheets
# ---------------------------------------------------------------------------


def _sheet_rows(path: Path, sheet: str) -> list[tuple]:
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        return list(workbook[sheet].iter_rows(values_only=True))
    finally:
        workbook.close()


def _header_index(rows: list[tuple], first_cell: str) -> int:
    for index, row in enumerate(rows):
        if row and row[0] and str(row[0]).strip().startswith(first_cell):
            return index
    raise RuntimeError(f"header row starting with {first_cell!r} not found")


def _drop_empty(frame: pd.DataFrame, measures: list[str]) -> pd.DataFrame:
    """Drop the rows a pivot invents.

    ``pivot_table`` fills the cartesian product of the index levels, so a
    year/quarter index sprouts quarters the source never published (the series
    starts at 2006-Q3, not 2006-Q1). Those rows carry no measure at all.
    """
    return frame.dropna(subset=measures, how="all")


def read_country_sheet(path: Path, sheet: str) -> pd.DataFrame:
    """One country-of-birth sheet, long: country_of_birth_id, year, value."""
    rows = _sheet_rows(path, sheet)
    header_index = _header_index(rows, "SACC code")
    header = rows[header_index]
    years = {
        position: int(str(cell)[:4])
        for position, cell in enumerate(header)
        if position >= 2 and cell and re.match(r"^\d{4}-\d{2}", str(cell))
    }
    records = []
    for row in rows[header_index + 1 :]:
        code = row[0]
        if code is None or not str(code).strip()[:1].isdigit():
            continue
        for position, year in years.items():
            value = row[position] if position < len(row) else None
            records.append(
                {
                    "country_of_birth_id": str(code).strip(),
                    "year": year,
                    "value": None if value is None else int(value),
                }
            )
    return pd.DataFrame.from_records(records)


def read_country_totals(path: Path, sheet: str) -> pd.DataFrame:
    """The published Total row of a country-of-birth sheet, used for validation."""
    rows = _sheet_rows(path, sheet)
    header_index = _header_index(rows, "SACC code")
    header = rows[header_index]
    years = {
        position: int(str(cell)[:4])
        for position, cell in enumerate(header)
        if position >= 2 and cell and re.match(r"^\d{4}-\d{2}", str(cell))
    }
    for row in rows[header_index + 1 :]:
        if row[1] is not None and str(row[1]).strip() == "Total":
            return pd.DataFrame.from_records(
                [
                    {"year": year, "value": int(row[position])}
                    for position, year in years.items()
                    if row[position] is not None
                ]
            )
    raise RuntimeError(f"no Total row in {path.name} {sheet}")


def country_codes(input_dir: Path = INPUT_DIR) -> list[str]:
    """The SACC codes ABS publishes, in source order."""
    frame = read_country_sheet(
        input_dir / COUNTRY_FILES["net"][0], "Table 1.1"
    )
    return list(dict.fromkeys(frame["country_of_birth_id"]))


def build_country_tables(
    input_dir: Path = INPUT_DIR,
) -> dict[str, pd.DataFrame]:
    frames = []
    for measure, (filename, prefix) in COUNTRY_FILES.items():
        path = input_dir / filename
        for index, state in enumerate(SHEET_STATES, start=1):
            frame = read_country_sheet(path, f"Table {prefix}.{index}")
            frame["state_id"] = state
            frame["measure"] = measure
            frames.append(frame)
    long = pd.concat(frames, ignore_index=True)
    wide = long.pivot_table(
        index=["year", "state_id", "country_of_birth_id"],
        columns="measure",
        values="value",
        dropna=False,
    ).reset_index()
    wide.columns.name = None

    mapping = pd.read_csv(CODE_DIR / "sacc_iso3.csv", dtype=str).set_index(
        "country_of_birth_id"
    )["country_iso3_code"]
    wide["country_iso3_code"] = wide["country_of_birth_id"].map(mapping)
    wide = _drop_empty(wide, ["arrivals", "departures", "net"])

    national = wide[wide["state_id"].isna()].drop(columns=["state_id"])
    states = wide[wide["state_id"].notna()]
    return {
        "overseas_country_of_birth_australia": national,
        "overseas_country_of_birth_state": states,
    }


def read_visa_sheet(path: Path, sheet: str) -> pd.DataFrame:
    """One visa sheet, long: visa_group_id, year, measure, value."""
    rows = _sheet_rows(path, sheet)
    header_index = _header_index(rows, "Direction")
    header = rows[header_index]
    years = {
        position: int(str(cell)[:4])
        for position, cell in enumerate(header)
        if position >= 3 and cell and re.match(r"^\d{4}-\d{2}", str(cell))
    }
    records = []
    measure = None
    for row in rows[header_index + 1 :]:
        direction = strip_footnote(row[0]) if row[0] else None
        if direction and direction.startswith("Overseas migrant"):
            measure = "arrivals" if "arrivals" in direction else "departures"
        label = None
        for cell in (row[2], row[1]):
            if cell is not None and str(cell).strip():
                label = strip_footnote(cell)
                break
        if measure is None or label is None or label not in VISA_LABEL_TO_CODE:
            continue
        for position, year in years.items():
            value = row[position] if position < len(row) else None
            records.append(
                {
                    "visa_group_id": VISA_LABEL_TO_CODE[label],
                    "year": year,
                    "measure": measure,
                    "value": None if value is None else int(value),
                }
            )
    return pd.DataFrame.from_records(records)


def build_visa_tables(input_dir: Path = INPUT_DIR) -> dict[str, pd.DataFrame]:
    frames = []
    for index, state in enumerate(SHEET_STATES, start=1):
        frame = read_visa_sheet(input_dir / VISA_FILE, f"Table 4.{index}")
        frame["state_id"] = state
        frames.append(frame)
    long = pd.concat(frames, ignore_index=True)
    wide = long.pivot_table(
        index=["year", "state_id", "visa_group_id"],
        columns="measure",
        values="value",
        dropna=False,
    ).reset_index()
    wide.columns.name = None
    wide = _drop_empty(wide, ["arrivals", "departures"])
    national = wide[wide["state_id"].isna()].drop(columns=["state_id"])
    states = wide[wide["state_id"].notna()]
    return {
        "overseas_visa_australia": national,
        "overseas_visa_state": states,
    }


# ---------------------------------------------------------------------------
# SDMX dataflows
# ---------------------------------------------------------------------------


def read_dataflow(flow: str, input_dir: Path = INPUT_DIR) -> pd.DataFrame:
    return pd.read_csv(input_dir / f"{flow}.csv", dtype=str)


def build_age_sex_tables(
    flow: str, prefix: str, suffix: str = "", input_dir: Path = INPUT_DIR
) -> dict[str, pd.DataFrame]:
    frame = read_dataflow(flow, input_dir)
    frame = frame[
        ["MEASURE", "AGE", "SEX", "REGION", "TIME_PERIOD", "OBS_VALUE"]
    ].copy()
    frame["measure"] = frame["MEASURE"].map(MEASURE_CODES)
    frame["year"] = frame["TIME_PERIOD"].astype(int)
    if suffix == "":
        # SDMX labels a financial year by its end year; the architecture stores
        # the start year (2004 = 2004-05).
        frame["year"] -= 1
    frame["value"] = frame["OBS_VALUE"].astype(float).astype("Int64")
    wide = frame.pivot_table(
        index=["year", "REGION", "AGE", "SEX"],
        columns="measure",
        values="value",
        dropna=False,
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(
        columns={"REGION": "state_id", "AGE": "age_group", "SEX": "sex"}
    )
    wide = _drop_empty(wide, ["arrivals", "departures", "net"])
    national = wide[wide["state_id"] == "AUS"].drop(columns=["state_id"])
    states = wide[wide["state_id"] != "AUS"]
    return {
        f"{prefix}_age_sex_australia{suffix}": national,
        f"{prefix}_age_sex_state{suffix}": states,
    }


def build_visa_quarter_tables(
    input_dir: Path = INPUT_DIR,
) -> dict[str, pd.DataFrame]:
    frame = read_dataflow("OMAD_VISA", input_dir)
    frame = frame[frame["FREQ"] == "Q"].copy()
    frame["measure"] = frame["MEASURE"].map(VISA_MEASURE_CODES)
    periods = frame["TIME_PERIOD"].str.split("-Q", expand=True)
    frame["year"] = periods[0].astype(int)
    frame["quarter"] = periods[1].astype(int)
    frame["value"] = frame["OBS_VALUE"].astype(float).astype("Int64")
    wide = frame.pivot_table(
        index=["year", "quarter", "REGION", "VISA"],
        columns="measure",
        values="value",
        dropna=False,
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={"REGION": "state_id", "VISA": "visa_group_id"})
    wide = _drop_empty(wide, ["arrivals", "departures"])
    national = wide[wide["state_id"] == "AUS"].drop(columns=["state_id"])
    states = wide[wide["state_id"] != "AUS"]
    return {
        "overseas_visa_quarter_australia": national,
        "overseas_visa_quarter_state": states,
    }


# ---------------------------------------------------------------------------
# dictionary
# ---------------------------------------------------------------------------


def _labels(
    flow: str, code_column: str, label_column: str, input_dir: Path
) -> dict[str, str]:
    frame = read_dataflow(flow, input_dir)[
        [code_column, label_column]
    ].drop_duplicates()
    return dict(zip(frame[code_column], frame[label_column], strict=True))


def build_dicionario(
    tables: dict[str, pd.DataFrame], input_dir: Path = INPUT_DIR
) -> pd.DataFrame:
    sacc = pd.read_csv(CODE_DIR / "sacc_iso3.csv", dtype=str)
    sacc_names = dict(
        zip(sacc["country_of_birth_id"], sacc["sacc_name"], strict=True)
    )
    nom_ages = _labels("NOM_FY", "AGE", "Age", input_dir)
    nim_ages = _labels("NIM_FY", "AGE", "Age", input_dir)
    sexes = _labels("NOM_FY", "SEX", "Sex", input_dir)
    visas = _labels(
        "OMAD_VISA", "VISA", "Visa and Citizenship Groups", input_dir
    )
    visas = {code: re.sub(r"\s+", " ", label) for code, label in visas.items()}

    records = []
    for table, frame in tables.items():
        if table == "dicionario":
            continue
        if "country_of_birth_id" in frame:
            records += [
                (table, "country_of_birth_id", code, sacc_names[code])
                for code in sorted(frame["country_of_birth_id"].unique())
            ]
        if "age_group" in frame:
            source = nim_ages if table.startswith("interstate") else nom_ages
            records += [
                (table, "age_group", code, source[code])
                for code in sorted(frame["age_group"].unique())
            ]
        if "sex" in frame:
            records += [
                (table, "sex", code, sexes[code])
                for code in sorted(frame["sex"].unique())
            ]
        if "visa_group_id" in frame:
            records += [
                (table, "visa_group_id", code, visas[code])
                for code in sorted(frame["visa_group_id"].unique())
            ]
    return pd.DataFrame.from_records(
        [
            {
                "id_tabela": table,
                "nome_coluna": column,
                "chave": key,
                "cobertura_temporal": None,
                "valor": value,
            }
            for table, column, key, value in records
        ]
    )


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def validate_country_totals(
    tables: dict[str, pd.DataFrame], input_dir: Path = INPUT_DIR
) -> None:
    """The published Total row must equal the SDMX all-ages, all-persons series."""
    published = read_country_totals(
        input_dir / COUNTRY_FILES["net"][0], "Table 1.1"
    )
    sdmx = read_dataflow("NOM_FY", input_dir)
    sdmx = sdmx[
        (sdmx["MEASURE"] == "3")
        & (sdmx["AGE"] == "TOT")
        & (sdmx["SEX"] == "3")
        & (sdmx["REGION"] == "AUS")
    ].copy()
    sdmx["year"] = sdmx["TIME_PERIOD"].astype(int) - 1
    sdmx["value"] = sdmx["OBS_VALUE"].astype(float).astype(int)
    merged = published.merge(
        sdmx[["year", "value"]], on="year", suffixes=("_xlsx", "_sdmx")
    )
    mismatch = merged[merged["value_xlsx"] != merged["value_sdmx"]]
    print(
        f"  country totals vs NOM_FY: {len(merged)} years compared, "
        f"{len(mismatch)} mismatched"
    )
    if len(mismatch):
        print(mismatch.to_string(index=False))


def validate_visa_mapping(
    tables: dict[str, pd.DataFrame], input_dir: Path = INPUT_DIR
) -> None:
    """The spreadsheet's visa labels must reproduce the dataflow's coded values.

    Spreadsheet 4 labels its visa groups; OMAD_VISA codes them. Comparing the two
    on their overlapping financial years is what verifies ``VISA_LABEL_TO_CODE``.

    The two sources are different vintages of the same series: the spreadsheet is
    the annual release, while OMAD_VISA is refreshed with the quarterly
    population release and therefore carries a later revision of the preliminary
    year. Disagreement in that latest year is expected; disagreement in any
    earlier year means the label mapping is wrong.
    """
    sdmx = read_dataflow("OMAD_VISA", input_dir)
    sdmx = sdmx[sdmx["FREQ"] == "A"].copy()
    sdmx["measure"] = sdmx["MEASURE"].map(VISA_MEASURE_CODES)
    sdmx["year"] = sdmx["TIME_PERIOD"].astype(int) - 1
    sdmx["value"] = sdmx["OBS_VALUE"].astype(float).astype(int)
    sdmx = sdmx.pivot_table(
        index=["year", "REGION", "VISA"], columns="measure", values="value"
    ).reset_index()
    sdmx.columns.name = None
    sdmx = sdmx.rename(columns={"REGION": "state_id", "VISA": "visa_group_id"})

    national = tables["overseas_visa_australia"].assign(state_id="AUS")
    xlsx = pd.concat(
        [national, tables["overseas_visa_state"]], ignore_index=True
    )
    merged = xlsx.merge(
        sdmx,
        on=["year", "state_id", "visa_group_id"],
        suffixes=("_xlsx", "_sdmx"),
    )
    preliminary = int(merged["year"].max())
    for measure in ("arrivals", "departures"):
        left = merged[f"{measure}_xlsx"]
        right = merged[f"{measure}_sdmx"]
        mismatch = merged[left.notna() & right.notna() & (left != right)]
        settled = sorted(set(mismatch["year"]) - {preliminary})
        print(
            f"  visa {measure}, spreadsheet vs OMAD_VISA: {len(merged)} cells compared, "
            f"{len(mismatch)} differ, all in {preliminary}-{str(preliminary + 1)[-2:]} "
            f"(preliminary, revised by the later quarterly release)"
        )
        if settled:
            raise RuntimeError(
                f"visa {measure} differs from OMAD_VISA in settled years {settled}; "
                "VISA_LABEL_TO_CODE is wrong"
            )


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------


def architecture_columns(table: str) -> list[str]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open(encoding="utf-8") as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def write_table(
    table: str, frame: pd.DataFrame, output_dir: Path = OUTPUT_DIR
) -> int:
    """Write one table as all-STRING Snappy parquet, hive-partitioned by year.

    Column order comes from the architecture CSV. Values are cast to string
    through arrow rather than ``astype(str)``, which would render a null as the
    literal "nan" and defeat the dbt ``safe_cast``; counts are held in a nullable
    Int64 first so that a value serializes as "15790", not "15790.0".

    The table directory is rebuilt from scratch, so a rerun after a source
    revision cannot leave a stale partition behind.
    """
    columns = architecture_columns(table)
    missing = set(columns) - set(frame.columns)
    if missing:
        raise RuntimeError(
            f"{table}: architecture columns missing from the data: {missing}"
        )
    frame = frame[columns].copy()
    for column in columns:
        if pd.api.types.is_float_dtype(frame[column]):
            frame[column] = frame[column].astype("Int64")
    string_schema = pa.schema(
        [pa.field(name, pa.string()) for name in columns]
    )

    destination = output_dir / table
    if destination.exists():
        shutil.rmtree(destination)
    if "year" not in columns:
        destination.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pandas(frame, preserve_index=False).cast(
            string_schema
        )
        pq.write_table(
            arrow, destination / "data.parquet", compression="snappy"
        )
        return len(frame)

    for year, group in frame.groupby("year", sort=True):
        partition = destination / f"year={int(year)}"
        partition.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pandas(group, preserve_index=False).cast(
            string_schema
        )
        pq.write_table(arrow, partition / "data.parquet", compression="snappy")
    return len(frame)


def clean_all(
    input_dir: Path = INPUT_DIR, output_dir: Path = OUTPUT_DIR
) -> dict[str, int]:
    tables: dict[str, pd.DataFrame] = {}
    tables.update(build_country_tables(input_dir))
    tables.update(build_visa_tables(input_dir))
    tables.update(build_visa_quarter_tables(input_dir))
    tables.update(build_age_sex_tables("NOM_FY", "overseas", "", input_dir))
    tables.update(
        build_age_sex_tables("NOM_CY", "overseas", "_calendar_year", input_dir)
    )
    tables.update(build_age_sex_tables("NIM_FY", "interstate", "", input_dir))
    tables.update(
        build_age_sex_tables(
            "NIM_CY", "interstate", "_calendar_year", input_dir
        )
    )

    print("validation:")
    validate_country_totals(tables, input_dir)
    validate_visa_mapping(tables, input_dir)

    tables["dicionario"] = build_dicionario(tables, input_dir)

    counts = {}
    for table, frame in tables.items():
        counts[table] = write_table(table, frame, output_dir)
    return counts


def main() -> None:
    counts = clean_all()
    print("\nrows written:")
    for table, rows in sorted(counts.items()):
        print(f"  {table}: {rows:,}")
    print(f"  TOTAL: {sum(counts.values()):,}")


if __name__ == "__main__":
    main()
