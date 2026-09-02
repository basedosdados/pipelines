#!/usr/bin/env python3
"""
Clean the College Scorecard archive into partitioned Parquet for Data Basis.

Reads the extracted "All Data Files" archive and writes, under $OUTPUT_DIR:

    institution/year=YYYY/data.parquet          wide, 91 columns
    <domain>/year=YYYY/data.parquet             long, one per API namespace
    field_of_study/year=YYYY/data.parquet       wide, 178 columns
    variable/data.parquet                       variable catalogue
    dicionario/data.parquet                     value labels

Every column is written as STRING: staging is all-STRING by house convention
and the dbt models safe_cast each column to its architecture type. Raw source
strings are passed through untouched, so no float round-trip can corrupt them.

Usage:
    uv run --no-project --with pandas --with pyarrow --with pyyaml \
        --with openpyxl python models/us_ed_college_scorecard/code/clean_data.py
"""

import csv
import json
import logging
import os
import pathlib
import re
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# pyrefly: ignore [untyped-import]
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
import spec

DATA_DIR = pathlib.Path(
    os.environ.get(
        "SCORECARD_DATA_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data",
    )
)
RAW_DIR = DATA_DIR / "input" / "raw"
DICT_XLSX = DATA_DIR / "input" / "CollegeScorecardDataDictionary.xlsx"
OUTPUT_DIR = pathlib.Path(os.environ.get("OUTPUT_DIR", DATA_DIR / "output"))
CODE_DIR = pathlib.Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clean")

NUMERIC = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")
US_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
# Disclosure-avoidance interval bands published by the BBRR* columns.
BANDED = re.compile(r"^(<=|>=)?\d*\.?\d+(-\d*\.?\d+)?$")


# --------------------------------------------------------------- dictionary


def load_dictionary():
    """Return (by_source, file_years) from the archive's own data.yaml.

    data.yaml ships inside the download and is regenerated with each release,
    so it — not the XLSX, which lags by a release — is the authority for
    column descriptions and for the file -> cohort year map.
    """
    d = yaml.safe_load((RAW_DIR / "data.yaml").read_text())
    by_source = {}
    for api_name, entry in d["dictionary"].items():
        if not isinstance(entry, dict) or not entry.get("source"):
            continue
        by_source[entry["source"].upper()] = {"api_name": api_name, **entry}
    file_years = {f["name"]: f["key"] for f in d["files"]}
    return by_source, file_years


def namespace(by_source, raw_col):
    return by_source[raw_col.upper()]["api_name"].split(".")[0]


def route(by_source, raw_col):
    """Which table a raw institution column belongs to."""
    if raw_col.upper() in spec.PROMOTED_TO_WIDE:
        return "institution"
    ns = namespace(by_source, raw_col)
    if ns in spec.WIDE_NAMESPACES:
        return "institution"
    return spec.LONG_TABLES[ns]


# -------------------------------------------------------------- value logic


def split_value(raw):
    """Split a published cell into (value, value_raw).

    A plain number goes to `value` verbatim — never re-formatted, so
    '1959' cannot become '1959.0'. Anything else the source published
    (an interval band like '0.30-0.39', a date, a text label, or the
    literal 'PrivacySuppressed') is preserved in `value_raw`.
    """
    if NUMERIC.match(raw):
        return raw, None
    return None, raw


def iso_date(raw):
    m = US_DATE.match(raw or "")
    if not m:
        return None
    mo, dy, yr = m.groups()
    return f"{yr}-{int(mo):02d}-{int(dy):02d}"


# --------------------------------------------------------------- io helpers


def read_source_csv(path):
    """Read a source CSV with every column as string.

    pyarrow's reader is used rather than pandas': at 3,308 columns the pandas
    C parser takes ~210s per cohort file, which is 105 minutes across the
    archive. Empty strings are NOT treated as null here -- the sentinel
    handling downstream is explicit, so the reader must not pre-empt it.
    """
    return pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(block_size=64 << 20),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in _header(path)},
            strings_can_be_null=False,
            null_values=[],
        ),
    ).rename_columns([c.upper() for c in _header(path)])


def _header(path):
    with open(path, newline="") as fh:
        return next(csv.reader(fh))


def melt_arrow(tbl, cols, unitid, year):
    """Melt `cols` of an Arrow table to (year, unitid, variable_name, value, value_raw).

    Done in Arrow rather than pandas: at 1,367 columns a pandas melt +
    object-dtype write costs ~125s per cohort file, i.e. over an hour across
    the archive, and produces the same bytes.

    Cells holding a NULL_TOKENS sentinel produce no row at all. Everything
    else produces a row: a plain number lands in `value` verbatim (so '1959'
    can never become '1959.0'), and anything else the source published --
    an interval band, a date, a text label, or 'PrivacySuppressed' -- is
    preserved in `value_raw`. That is what keeps a withheld cell
    distinguishable from a cell that was never collected.
    """
    units, names, raws = [], [], []
    for col in cols:
        arr = tbl.column(col).combine_chunks()
        # pyrefly: ignore [missing-attribute]
        keep = pc.invert(pc.is_in(arr, value_set=pa.array(spec.NULL_TOKENS)))
        # pyrefly: ignore [missing-attribute]
        idx = pc.indices_nonzero(keep)
        if len(idx) == 0:
            continue
        units.append(unitid.take(idx))
        raws.append(arr.take(idx))
        names.append(pa.array([col] * len(idx), type=pa.string()))
    if not units:
        return None
    unit_all = pa.concat_arrays([a.cast(pa.string()) for a in units])
    name_all = pa.concat_arrays(names)
    raw_all = pa.concat_arrays([a.cast(pa.string()) for a in raws])
    # The source uses 'PS' and 'PrivacySuppressed' interchangeably across
    # vintages for the same thing; normalise so a consumer needs one test.
    # pyrefly: ignore [missing-attribute]
    raw_all = pc.if_else(pc.equal(raw_all, "PS"), "PrivacySuppressed", raw_all)
    # pyrefly: ignore [missing-attribute]
    is_number = pc.match_substring_regex(raw_all, NUMERIC.pattern)
    return pa.table(
        {
            "year": pa.array([str(year)] * len(raw_all), type=pa.string()),
            "unitid": unit_all,
            "variable_name": name_all,
            # pyrefly: ignore [missing-attribute]
            "value": pc.if_else(
                is_number, raw_all, pa.nulls(len(raw_all), pa.string())
            ),
            # pyrefly: ignore [missing-attribute]
            "value_raw": pc.if_else(
                is_number, pa.nulls(len(raw_all), pa.string()), raw_all
            ),
        }
    )


# ------------------------------------------------------------------ writers


def write_parquet(data, table, year=None):
    """Write one partition. Every column is STRING (house staging convention)."""
    path = OUTPUT_DIR / table
    if year is not None:
        path = path / f"year={year}"
    path.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        arrays = [
            pa.array(data[c], type=pa.string(), from_pandas=True)
            for c in data.columns
        ]
        data = pa.Table.from_arrays(arrays, names=list(data.columns))
    pq.write_table(data, path / "data.parquet", compression="snappy")
    return data.num_rows


# --------------------------------------------------------- institution file


def clean_institution(path, year, by_source, stats):
    log.info("institution %s -> year=%s", path.name, year)
    tbl = read_source_csv(path)
    routing = {c: route(by_source, c) for c in tbl.column_names}
    unitid = tbl.column("UNITID").combine_chunks()
    sentinels = pa.array(spec.NULL_TOKENS + spec.SUPPRESSED_TOKENS)

    # ---- wide `institution`
    wide = {"year": pa.array([str(year)] * tbl.num_rows, type=pa.string())}
    for bd_col, raw in spec.INSTITUTION_COLUMNS:
        if raw is None:
            continue
        if raw not in tbl.column_names:
            wide[bd_col] = pa.nulls(tbl.num_rows, pa.string())
            stats["missing_in_year"].append((year, raw))
            continue
        arr = tbl.column(raw).combine_chunks().cast(pa.string())
        stats["suppressed_wide"] += (
            # pyrefly: ignore [missing-attribute]
            pc.sum(
                # pyrefly: ignore [missing-attribute]
                pc.is_in(arr, value_set=pa.array(spec.SUPPRESSED_TOKENS))
            ).as_py()
            or 0
        )
        # pyrefly: ignore [missing-attribute]
        blank = pc.is_in(arr, value_set=sentinels)
        # pyrefly: ignore [missing-attribute]
        arr = pc.if_else(blank, pa.nulls(len(arr), pa.string()), arr)
        if bd_col == "title_iv_approval_date":
            arr = pa.array(
                [iso_date(v) for v in arr.to_pylist()], type=pa.string()
            )
        wide[bd_col] = arr
    stats["rows"].setdefault("institution", 0)
    stats["rows"]["institution"] += write_parquet(
        pa.table(wide), "institution", year
    )

    # ---- long tables, one per API namespace
    for table in sorted(set(spec.LONG_TABLES.values())):
        cols = [c for c, t in routing.items() if t == table]
        if not cols:
            continue
        out = melt_arrow(tbl, cols, unitid, year)
        if out is None:
            continue
        stats["rows"].setdefault(table, 0)
        stats["rows"][table] += write_parquet(out, table, year)
        raw_all = out.column("value_raw").combine_chunks()
        stats["suppressed_long"] += (
            # pyrefly: ignore [missing-attribute]
            pc.sum(
                # pyrefly: ignore [missing-attribute]
                pc.is_in(raw_all, value_set=pa.array(spec.SUPPRESSED_TOKENS))
            ).as_py()
            or 0
        )
        stats["banded"] += (
            # pyrefly: ignore [missing-attribute]
            pc.sum(pc.match_substring_regex(raw_all, BANDED.pattern)).as_py()
            or 0
        )


# ------------------------------------------------------ field-of-study file


def clean_field_of_study(path, year, stats):
    log.info("field_of_study %s -> year=%s", path.name, year)
    tbl = read_source_csv(path)
    tbl = tbl.rename_columns(
        [spec.bd_name_field_of_study(c) for c in tbl.column_names]
    )
    sentinels = pa.array(spec.NULL_TOKENS + spec.SUPPRESSED_TOKENS)
    cols = {"year": pa.array([str(year)] * tbl.num_rows, type=pa.string())}
    for name in tbl.column_names:
        arr = tbl.column(name).combine_chunks().cast(pa.string())
        stats["suppressed_fos"] += (
            # pyrefly: ignore [missing-attribute]
            pc.sum(
                # pyrefly: ignore [missing-attribute]
                pc.is_in(arr, value_set=pa.array(spec.SUPPRESSED_TOKENS))
            ).as_py()
            or 0
        )
        # pyrefly: ignore [missing-attribute]
        cols[name] = pc.if_else(
            # pyrefly: ignore [missing-attribute]
            pc.is_in(arr, value_set=sentinels),
            pa.nulls(len(arr), pa.string()),
            arr,
        )
    stats["rows"].setdefault("field_of_study", 0)
    stats["rows"]["field_of_study"] += write_parquet(
        pa.table(cols), "field_of_study", year
    )


# ------------------------------------------------------- catalogue + labels


def build_variable_table(by_source, inst_header, fos_header):
    """One row per published variable, with the table it was routed to."""
    rows = []
    for raw in inst_header:
        e = by_source[raw.upper()]
        rows.append(
            {
                "variable_name": raw.upper(),
                "source_file": "institution",
                "table_name": route(by_source, raw),
                "api_name": e["api_name"],
                "data_type": e.get("type") or "string",
                "label": (e.get("description") or "").strip(),
            }
        )
    for raw in fos_header:
        e = by_source.get("P_" + raw.upper()) or by_source[raw.upper()]
        rows.append(
            {
                "variable_name": raw.upper(),
                "source_file": "field_of_study",
                "table_name": "field_of_study",
                "api_name": e["api_name"],
                "data_type": e.get("type") or "string",
                "label": (e.get("description") or "").strip(),
            }
        )
    return pd.DataFrame(rows)


# Columns the published workbook lists value labels for, but whose cleaned
# values are ALREADY those labels rather than the codes. A dictionary entry
# for them would map codes the column never contains, so they are excluded
# here and carry covered_by_dictionary = no in the architecture.
LABEL_VALUED = {("institution", "control_peps"), ("field_of_study", "control")}

# `credential_level` is taken from the data instead of the workbook: the
# workbook ships with the October 2024 release and documents 6 levels, while
# the June 2026 field-of-study files use 9. The table carries its own label
# column, so the pairs are read straight off it and are complete by
# construction.
DATA_DERIVED = {
    ("field_of_study", "credential_level"): "credential_description"
}

# Columns resolved through a Data Basis directory rather than this dataset's
# dictionary. House rule: a directory-referenced column is never
# covered_by_dictionary, so its labels do not belong in `dicionario` either.
DIRECTORY_RESOLVED = {("institution", "state_fips")}


def dicionario_from_data(table, code_column, label_column):
    """Distinct code -> label pairs read from a cleaned table."""
    pairs = {}
    for path in sorted((OUTPUT_DIR / table).glob("year=*/data.parquet")):
        tbl = pq.read_table(path, columns=[code_column, label_column])
        for code, label in zip(
            tbl.column(code_column).to_pylist(),
            tbl.column(label_column).to_pylist(),
            strict=True,
        ):
            if code is not None and label:
                pairs.setdefault(str(code), str(label))
    return [
        {
            "id_tabela": table,
            "nome_coluna": code_column,
            "chave": code,
            "cobertura_temporal": "",
            "valor": label,
        }
        for code, label in sorted(
            pairs.items(), key=lambda kv: (len(kv[0]), kv[0])
        )
    ]


def build_dicionario():
    """Value -> label pairs, from the published data dictionary workbook."""
    # pyrefly: ignore [untyped-import]
    import openpyxl

    wb = openpyxl.load_workbook(DICT_XLSX, read_only=True)
    raw_to_bd_inst = {raw: bd for bd, raw in spec.INSTITUTION_COLUMNS if raw}
    rows = []
    sheets = {
        "Institution_Data_Dictionary": ("institution", raw_to_bd_inst),
        "FieldOfStudy_Data_Dictionary": ("field_of_study", None),
    }
    for sheet, (table, mapping) in sheets.items():
        ws = wb[sheet]
        it = ws.iter_rows(values_only=True)
        hdr = next(it)
        iv, ival, ilab = (
            hdr.index(x) for x in ("VARIABLE NAME", "VALUE", "LABEL")
        )
        current = None
        for r in it:
            if r[iv]:
                current = str(r[iv]).strip().upper()
            if not current or r[ival] in (None, ""):
                continue
            if mapping is not None:
                col = mapping.get(current)
                if col is None:
                    continue  # variable lives in a long table; see `variable`
            else:
                col = spec.bd_name_field_of_study(current)
            if (
                table,
                col,
            ) in LABEL_VALUED | DATA_DERIVED.keys() | DIRECTORY_RESOLVED:
                continue
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": col,
                    "chave": str(r[ival]).strip(),
                    "cobertura_temporal": "",
                    "valor": str(r[ilab]).strip() if r[ilab] else "",
                }
            )
    for (table, code_column), label_column in DATA_DERIVED.items():
        rows += dicionario_from_data(table, code_column, label_column)
    frame = pd.DataFrame(rows).drop_duplicates()
    # Written next to the code as well, so build_architecture can set
    # covered_by_dictionary from what the dictionary actually contains
    # rather than from a hand-maintained list that can drift out of step.
    frame.to_csv(CODE_DIR / "dicionario_labels.csv", index=False)
    return frame


# --------------------------------------------------------------------- main


def main():
    by_source, file_years = load_dictionary()
    stats = {
        "rows": {},
        "suppressed_wide": 0,
        "suppressed_long": 0,
        "suppressed_fos": 0,
        "banded": 0,
        "missing_in_year": [],
    }

    merged = sorted(RAW_DIR.glob("MERGED*_PP.csv"))
    fos = sorted(RAW_DIR.glob("FieldOfStudyData*_PP.csv"))

    for path in merged:
        clean_institution(path, int(file_years[path.name]), by_source, stats)
    for path in fos:
        clean_field_of_study(path, int(file_years[path.name]), stats)

    inst_header = _header(merged[-1])
    fos_header = _header(fos[-1])
    stats["rows"]["variable"] = write_parquet(
        build_variable_table(by_source, inst_header, fos_header), "variable"
    )
    stats["rows"]["dicionario"] = write_parquet(
        build_dicionario(), "dicionario"
    )

    log.info(
        "row counts: %s", json.dumps(stats["rows"], indent=2, sort_keys=True)
    )
    (CODE_DIR / "clean_stats.json").write_text(
        json.dumps(stats, indent=2, sort_keys=True, default=str)
    )
    return stats


if __name__ == "__main__":
    main()
