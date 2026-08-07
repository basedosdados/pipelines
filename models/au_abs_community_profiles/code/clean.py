"""Melt ABS Census DataPacks (short-header) into the long au_abs_community_profiles
store. One long frame per geography level, plus an auxiliary_info cell catalogue.

Long fact schema (per geo table):
    census_year INT64 (partition) | id_<level> STRING | profile STRING (GCP/TSP)
    | table_code STRING | cell_code STRING | value FLOAT64

Key rules (see ONBOARDING_PLAN.md):
- Geo code is always CSV column 0 (2016/21 = <GEO>_CODE_<year>; 2011 = region_id).
- Geo level + census year (for GCP) come from the FILENAME, not the header.
- profile: G/B tables -> "GCP" (2011 BCP is the GCP-equivalent), T tables -> "TSP".
- table_code = filename table-part with a trailing split letter removed (G04A -> G04).
- TSP: census_year comes from the year token inside each cell code (_C11/_C2011 ...);
  the token is stripped so cell_code is year-invariant.
- value: ".." (not-applicable, whole columns) -> dropped; genuine 0 kept; FLOAT64.

Run:  python clean.py <input_root> [<output_dir>]
      python clean.py input/investigation output/investigation   # validate on STE samples
"""

import glob
import os
import re
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# DataPack geo token (last filename token) -> (bd table, id column | None for national)
GEO = {
    "AUS": ("national", None),
    "AUST": ("national", None),
    "STE": ("state", "id_state"),
    "SA1": ("sa1", "id_sa1"),
    "SA2": ("sa2", "id_sa2"),
    "SA3": ("sa3", "id_sa3"),
    "SA4": ("sa4", "id_sa4"),
    "GCCSA": ("gccsa", "id_gccsa"),
    "LGA": ("lga", "id_lga"),
    "SAL": ("suburb", "id_suburb"),
    "POA": ("postal_area", "id_postal_area"),
    "CED": (
        "commonwealth_electoral_division",
        "id_commonwealth_electoral_division",
    ),
    "SED": ("state_electoral_division", "id_state_electoral_division"),
}

# filename: <year>Census_<tablepart>_<region>_<geo>[_short].csv
FN_RE = re.compile(
    r"^(\d{4})Census_([A-Za-z]+\d+[A-Za-z]?)_(\w+?)_([A-Za-z0-9]+)(_short)?\.csv$"
)

# TSP encodes the census year in the cell code in three forms (all confirmed to
# cover 100% of TSP cells): C-prefixed 2-digit (Tot_persons_C11_M, C11_OO_DS_...),
# C-prefixed 4-digit (Med_age_persns_C2011), and <year>_Ce (IP_..._2011_Ce_M).
_TOK = r"(2011|2016|2021|11|16|21)"
TSP_CE_RE = re.compile(r"(?:^|_)(2011|2016|2021)_Ce(?=_|$)")
TSP_START_RE = re.compile(rf"^C{_TOK}_")
TSP_MIDEND_RE = re.compile(rf"_C{_TOK}(?=_|$)")
# any residual year token (used to assert a clean strip)
TSP_ANY_RE = re.compile(
    r"(?:(?:^|_)(?:2011|2016|2021)_Ce|(?:^|_)C(?:2011|2016|2021|11|16|21))(?=_|$)"
)
TSP_YEAR = {
    "11": 2011,
    "16": 2016,
    "21": 2021,
    "2011": 2011,
    "2016": 2016,
    "2021": 2021,
}

MEDIAN_AVG_TABLES = {"G02", "B02", "T02"}  # "Selected Medians and Averages"


def parse_filename(fn):
    m = FN_RE.match(fn)
    if not m:
        return None
    year, tablepart, _region, geo = (
        m.group(1),
        m.group(2),
        m.group(3),
        m.group(4),
    )
    return int(year), tablepart.upper(), geo.upper()


def table_code_of(code):
    """Logical table code, normalized identically for the fact (from filenames)
    and the dictionary (whose 'Profile table' has messy variants like 'G13a' and
    'G17 '): trim, then drop one trailing letter of any case.
    G04A -> G04, G13a -> G13, 'G17 ' -> G17, G01 -> G01, T03E -> T03.
    """
    return re.sub(r"[A-Za-z]$", "", str(code).strip())


def profile_of(table_code):
    return "TSP" if table_code.startswith("T") else "GCP"


def strip_tsp_year(cell_code):
    """(census_year, year_invariant_code) for a TSP cell, else (None, cell_code).

    Removes the year token plus exactly one bounding underscore, whatever its
    position: ^C11_rest -> rest; a_C11_b -> a_b; a_C2011 -> a.
    """
    m = TSP_CE_RE.search(cell_code)
    if m:
        yr, s, e = TSP_YEAR[m.group(1)], m.start(), m.end()
        base = (
            cell_code[e:].lstrip("_")
            if s == 0
            else cell_code[:s] + cell_code[e:]
        )
        return yr, base
    m = TSP_START_RE.match(cell_code)
    if m:
        return TSP_YEAR[m.group(1)], cell_code[m.end() :]
    m = TSP_MIDEND_RE.search(cell_code)
    if m:
        return TSP_YEAR[m.group(1)], cell_code[: m.start()] + cell_code[
            m.end() :
        ]
    return None, cell_code


def melt_csv(path):
    """One data CSV -> long rows dict-of-columns (before typing/renaming)."""
    fn = os.path.basename(path)
    parsed = parse_filename(fn)
    if parsed is None:
        raise ValueError(f"unparseable filename: {fn}")
    year, tablepart, geo = parsed
    if geo not in GEO:
        raise ValueError(f"unknown geo token {geo!r} in {fn}")
    tcode = table_code_of(tablepart)
    profile = profile_of(tcode)

    df = pd.read_csv(path, dtype=str)
    geocol = df.columns[0]
    # DataPacks prefix non-ABS geo codes with the level token (LGA10050, CED101,
    # SED10001, SAL..., POA...); the directory stores bare codes. Strip a leading
    # alpha prefix so the FK matches. Main-structure (SA*, numeric) and GCCSA
    # (digit-first, e.g. 1GSYD) codes have no leading alpha and are untouched.
    df[geocol] = df[geocol].str.replace(r"^[A-Za-z]+", "", regex=True)
    long = df.melt(id_vars=[geocol], var_name="cell_code", value_name="value")
    long = long.rename(columns={geocol: "geo_code"})

    # ".." (not applicable) and blanks -> NaN -> drop
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"]).reset_index(drop=True)

    long["table_code"] = tcode
    long["profile"] = profile
    if profile == "TSP":
        yr, base = zip(*long["cell_code"].map(strip_tsp_year), strict=False)
        long["census_year"] = list(yr)
        long["cell_code"] = list(base)
        if long["census_year"].isna().any():
            bad = long.loc[long["census_year"].isna(), "cell_code"].unique()[
                :5
            ]
            raise ValueError(f"TSP cells with no year token in {fn}: {bad}")
        resid = long["cell_code"].map(lambda s: bool(TSP_ANY_RE.search(s)))
        if resid.any():
            raise ValueError(
                f"residual year token after strip in {fn}: "
                f"{long.loc[resid, 'cell_code'].unique()[:5]}"
            )
    else:
        long["census_year"] = year
    long["geo_level"] = GEO[geo][0]
    long["id_col"] = GEO[geo][1] or ""
    return long


def find_data_csvs(root):
    return [
        p
        for p in glob.glob(os.path.join(root, "**", "*.csv"), recursive=True)
        if FN_RE.match(os.path.basename(p))
    ]


# ---- cell dictionary (auxiliary_info) --------------------------------------


def _find_header_row(raw, needle):
    for i in range(min(20, len(raw))):
        cells = [str(v).replace(" ", "").lower() for v in raw.iloc[i].tolist()]
        if any(needle in c for c in cells):
            return i
    return None


def _norm(c):
    return str(c).replace(" ", "").lower()


def load_dictionary(pack_dir, profile, pack_year):
    """Return (cell_df, table_df) for one pack's Metadata workbook."""
    xlsx = glob.glob(
        os.path.join(pack_dir, "**", "Metadata_*.xlsx"), recursive=True
    )
    if not xlsx:
        return None, None
    mf = xlsx[0]
    xl = pd.ExcelFile(mf)
    cell_sheet = next(
        s for s in xl.sheet_names if "celldescriptor" in _norm(s)
    )
    tbl_sheet = next(s for s in xl.sheet_names if "population" in _norm(s))

    raw = pd.read_excel(mf, sheet_name=cell_sheet, header=None, dtype=str)
    hr = _find_header_row(raw, "short")
    cell = pd.read_excel(
        mf, sheet_name=cell_sheet, header=hr, dtype=str
    ).dropna(how="all")
    cell.columns = [_norm(c) for c in cell.columns]
    cell = cell.rename(
        columns={
            "short": "cell_code",
            "long": "long_description",
            "datapackfile": "datapack_part",
            "profiletable": "table_code",
            "columnheadingdescriptioninprofile": "heading",
        }
    )
    cell = cell[cell["cell_code"].notna()].copy()
    cell["table_code"] = cell["table_code"].map(table_code_of)

    rawt = pd.read_excel(mf, sheet_name=tbl_sheet, header=None, dtype=str)
    htr = _find_header_row(rawt, "tablenumber")
    tbl = pd.read_excel(
        mf, sheet_name=tbl_sheet, header=htr, dtype=str
    ).dropna(how="all")
    tbl.columns = [_norm(c) for c in tbl.columns]
    tbl = tbl.rename(
        columns={
            "tablenumber": "table_code",
            "tablename": "table_name",
            "tablepopulation": "table_population",
        }
    )
    tbl = tbl[tbl["table_code"].notna()][
        ["table_code", "table_name", "table_population"]
    ]
    tbl["table_code"] = tbl["table_code"].map(table_code_of)
    tbl = tbl.drop_duplicates(subset="table_code", keep="first")
    return cell, tbl


def build_auxiliary_info(packs):
    """packs: list of (pack_dir, profile, pack_year)."""
    rows = []
    for pack_dir, profile, pack_year in packs:
        cell, tbl = load_dictionary(pack_dir, profile, pack_year)
        if cell is None:
            continue
        c = cell.copy()
        c["profile"] = profile
        if profile == "TSP":
            yr, base = zip(*c["cell_code"].map(strip_tsp_year), strict=False)
            c["census_year"] = list(yr)
            c["cell_code"] = list(base)
        else:
            c["census_year"] = pack_year
        tmap_name = dict(
            # pyrefly: ignore [unsupported-operation]
            zip(tbl["table_code"], tbl["table_name"], strict=False)
        )
        tmap_pop = dict(
            # pyrefly: ignore [unsupported-operation]
            zip(tbl["table_code"], tbl["table_population"], strict=False)
        )
        c["table_name"] = c["table_code"].map(tmap_name)
        c["table_population"] = c["table_code"].map(tmap_pop)
        rows.append(c)
    aux = pd.concat(rows, ignore_index=True)

    def statistic(r):
        if r["table_code"] not in MEDIAN_AVG_TABLES:
            return "count"
        s = f"{r['cell_code']} {r['long_description']}".lower()
        if s.startswith("median") or "median" in r["cell_code"].lower()[:7]:
            return "median"
        if s.startswith("average") or r["cell_code"].lower().startswith(
            ("avg", "average")
        ):
            return "average"
        return "median" if "med" in r["cell_code"].lower() else "average"

    aux["statistic_type"] = aux.apply(statistic, axis=1)

    def unit(r):
        if r["statistic_type"] == "count":
            pop = r.get("table_population")
            pop = "" if pd.isna(pop) else str(pop)
            return pop.split("(")[0].strip().rstrip(".").lower() or "persons"
        h = r.get("heading")
        h = "" if pd.isna(h) else str(h)
        m = re.search(r"\(([^)]+)\)", h)
        return m.group(1).strip() if m else ""

    aux["measurement_unit"] = aux.apply(unit, axis=1)
    cols = [
        "profile",
        "census_year",
        "table_code",
        "table_name",
        "table_population",
        "cell_code",
        "long_description",
        "heading",
        "datapack_part",
        "statistic_type",
        "measurement_unit",
    ]
    aux = aux[[c for c in cols if c in aux.columns]].drop_duplicates()
    aux["census_year"] = aux["census_year"].astype("Int64")
    return aux.reset_index(drop=True)


# ---- write (streaming: one ParquetWriter per level, bounded memory) --------

LEVEL_ID = {tbl: idc for tbl, idc in GEO.values()}  # bd table -> id col | None


def level_schema(id_col):
    fields = [pa.field("census_year", pa.int64())]
    if id_col:
        fields.append(pa.field(id_col, pa.string()))
    fields += [
        pa.field("profile", pa.string()),
        pa.field("table_code", pa.string()),
        pa.field("cell_code", pa.string()),
        pa.field("value", pa.float64()),
    ]
    return pa.schema(fields)


def frame_to_arrow(frame, id_col, schema):
    data = {
        "census_year": frame["census_year"].astype("int64").to_numpy(),
        "profile": frame["profile"].astype(str),
        "table_code": frame["table_code"].astype(str),
        "cell_code": frame["cell_code"].astype(str),
        "value": frame["value"].astype("float64").to_numpy(),
    }
    if id_col:
        data[id_col] = frame["geo_code"].astype(str)
    df = pd.DataFrame(data)[[f.name for f in schema]]
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "input/packs"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "output/packs"
    here = os.path.dirname(os.path.abspath(__file__))
    ds = os.path.dirname(here)
    root = os.path.join(ds, root)
    out_dir = os.path.join(ds, out_dir)
    os.makedirs(out_dir, exist_ok=True)

    csvs = sorted(find_data_csvs(root))
    print(f"found {len(csvs)} data CSVs under {os.path.relpath(root, ds)}")

    # stream each CSV straight to its level's ParquetWriter (bounded memory)
    writers, schemas, counts, byprofyr = {}, {}, {}, {}
    for p in csvs:
        long = melt_csv(p)
        lvl = long["geo_level"].iloc[0]
        if lvl not in writers:
            schemas[lvl] = level_schema(LEVEL_ID[lvl])
            writers[lvl] = pq.ParquetWriter(
                os.path.join(out_dir, f"{lvl}.parquet"),
                schemas[lvl],
                compression="snappy",
            )
            counts[lvl], byprofyr[lvl] = 0, {}
        tbl = frame_to_arrow(long, LEVEL_ID[lvl], schemas[lvl])
        writers[lvl].write_table(tbl)
        counts[lvl] += tbl.num_rows
        # pyrefly: ignore [not-iterable]
        for (pr, yr), n in (
            long.groupby(["profile", "census_year"]).size().items()
        ):
            byprofyr[lvl][(pr, int(yr))] = byprofyr[lvl].get(
                (pr, int(yr)), 0
            ) + int(n)  # pyrefly: ignore [unnecessary-type-conversion]
    for w in writers.values():
        w.close()

    print("\n=== per-geo-level long tables ===")
    for lvl in sorted(counts):
        print(f"  {lvl:12} rows={counts[lvl]:>11}  {byprofyr[lvl]}")

    # auxiliary_info: one pack dir per (profile, year); dictionaries only (small)
    packs = {}
    for p in csvs:
        # pyrefly: ignore [not-iterable]
        year, tablepart, _geo = parse_filename(os.path.basename(p))
        prof = profile_of(table_code_of(tablepart))
        d = os.path.dirname(p)
        while d != root and not glob.glob(os.path.join(d, "Metadata")):
            d = os.path.dirname(d)
        packs[(prof, year, d)] = (d, prof, year)
    aux = build_auxiliary_info(list(packs.values()))
    aux.to_parquet(
        os.path.join(out_dir, "auxiliary_info.parquet"), index=False
    )
    print(f"\n=== auxiliary_info: {len(aux)} rows ===")
    print(aux["statistic_type"].value_counts().to_dict())
    print(aux.groupby(["profile", "census_year"]).size().to_dict())


if __name__ == "__main__":
    main()
