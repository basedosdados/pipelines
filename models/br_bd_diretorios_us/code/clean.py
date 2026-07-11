import datetime
import re
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

BASE = Path(__file__).resolve().parents[1] / "data"
INPUT = BASE / "input"
OUTPUT = BASE / "output"

REGION_NAMES = {"1": "Northeast", "2": "Midwest", "3": "South", "4": "West"}
DIVISION_NAMES = {
    "1": "New England",
    "2": "Middle Atlantic",
    "3": "East North Central",
    "4": "West North Central",
    "5": "South Atlantic",
    "6": "East South Central",
    "7": "West South Central",
    "8": "Mountain",
    "9": "Pacific",
}
STATE_DIVISION = {
    "CT": "1",
    "ME": "1",
    "MA": "1",
    "NH": "1",
    "RI": "1",
    "VT": "1",
    "NJ": "2",
    "NY": "2",
    "PA": "2",
    "IL": "3",
    "IN": "3",
    "MI": "3",
    "OH": "3",
    "WI": "3",
    "IA": "4",
    "KS": "4",
    "MN": "4",
    "MO": "4",
    "NE": "4",
    "ND": "4",
    "SD": "4",
    "DE": "5",
    "DC": "5",
    "FL": "5",
    "GA": "5",
    "MD": "5",
    "NC": "5",
    "SC": "5",
    "VA": "5",
    "WV": "5",
    "AL": "6",
    "KY": "6",
    "MS": "6",
    "TN": "6",
    "AR": "7",
    "LA": "7",
    "OK": "7",
    "TX": "7",
    "AZ": "8",
    "CO": "8",
    "ID": "8",
    "MT": "8",
    "NV": "8",
    "NM": "8",
    "UT": "8",
    "WY": "8",
    "AK": "9",
    "CA": "9",
    "HI": "9",
    "OR": "9",
    "WA": "9",
}
DIVISION_REGION = {
    "1": "1",
    "2": "1",
    "3": "2",
    "4": "2",
    "5": "3",
    "6": "3",
    "7": "3",
    "8": "4",
    "9": "4",
}
TERRITORY_FIPS = {"60", "66", "69", "72", "74", "78"}
FREELY_ASSOCIATED_STATES = [
    {
        "id_state": "64",
        "abbreviation": "FM",
        "name": "Federated States of Micronesia",
    },
    {"id_state": "68", "abbreviation": "MH", "name": "Marshall Islands"},
    {"id_state": "70", "abbreviation": "PW", "name": "Palau"},
]
ISLAND_AREA_FIPS = {"60", "66", "69", "74", "78"}


def nullify(df):
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(
                lambda x: x.strip() if isinstance(x, str) else x
            )
            df[col] = df[col].map(
                lambda x: (
                    None
                    if x is None or pd.isna(x) or x in ("", "nan", "None")
                    else x
                )
            )
    return df


def write_table(df, slug, fields):
    df = nullify(df.copy())
    names = [f[0] for f in fields]
    df = df[names]
    for name, typ in fields:
        if typ == pa.float64():
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif typ == pa.int64():
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
    schema = pa.schema(fields)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    out = OUTPUT / slug
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / "data.parquet", compression="snappy")
    return df


def read_gazetteer(filename):
    df = pd.read_csv(
        INPUT / "gazetteer" / filename,
        sep="|",
        dtype=str,
        encoding="utf-8-sig",
    )
    df.columns = [c.strip() for c in df.columns]
    for col in df.columns:
        df[col] = df[col].str.strip()
    return df


def area_km2(series):
    return pd.to_numeric(series, errors="coerce") / 1e6


def centroid_wkt(lat, lon):
    lat = lat.str.strip().str.lstrip("+")
    lon = lon.str.strip().str.lstrip("+")
    return "POINT(" + lon + " " + lat + ")"


# U.S. Census Bureau state code and ICPSR state code, keyed by postal
# abbreviation (50 states + DC). Territories and freely associated states have
# no assigned code and are left null.
CENSUS_CODE = {
    "AK": "94",
    "AL": "63",
    "AR": "71",
    "AZ": "86",
    "CA": "93",
    "CO": "84",
    "CT": "16",
    "DC": "53",
    "DE": "51",
    "FL": "59",
    "GA": "58",
    "HI": "95",
    "IA": "42",
    "ID": "82",
    "IL": "33",
    "IN": "32",
    "KS": "47",
    "KY": "61",
    "LA": "72",
    "MA": "14",
    "MD": "52",
    "ME": "11",
    "MI": "34",
    "MN": "41",
    "MO": "43",
    "MS": "64",
    "MT": "81",
    "NC": "56",
    "ND": "44",
    "NE": "46",
    "NH": "12",
    "NJ": "22",
    "NM": "85",
    "NV": "88",
    "NY": "21",
    "OH": "31",
    "OK": "73",
    "OR": "92",
    "PA": "23",
    "RI": "15",
    "SC": "57",
    "SD": "45",
    "TN": "62",
    "TX": "74",
    "UT": "87",
    "VA": "54",
    "VT": "13",
    "WA": "91",
    "WI": "35",
    "WV": "55",
    "WY": "83",
}
ICPSR_CODE = {
    "AK": "81",
    "AL": "41",
    "AR": "42",
    "AZ": "61",
    "CA": "71",
    "CO": "62",
    "CT": "1",
    "DC": "55",
    "DE": "11",
    "FL": "43",
    "GA": "44",
    "HI": "82",
    "IA": "31",
    "ID": "63",
    "IL": "21",
    "IN": "22",
    "KS": "32",
    "KY": "51",
    "LA": "45",
    "MA": "3",
    "MD": "52",
    "ME": "2",
    "MI": "23",
    "MN": "33",
    "MO": "34",
    "MS": "46",
    "MT": "64",
    "NC": "47",
    "ND": "36",
    "NE": "35",
    "NH": "4",
    "NJ": "12",
    "NM": "66",
    "NV": "65",
    "NY": "13",
    "OH": "24",
    "OK": "53",
    "OR": "72",
    "PA": "14",
    "RI": "5",
    "SC": "48",
    "SD": "37",
    "TN": "54",
    "TX": "49",
    "UT": "67",
    "VA": "40",
    "VT": "6",
    "WA": "73",
    "WI": "25",
    "WV": "56",
    "WY": "68",
}


def clean_state():
    df = pd.read_csv(
        INPUT / "state" / "state.txt", sep="|", dtype=str, encoding="utf-8-sig"
    )
    df = df.rename(
        columns={
            "STATE": "id_state",
            "STUSAB": "abbreviation",
            "STATE_NAME": "name",
            "STATENS": "id_gnis",
        }
    )
    df["id_census"] = df["abbreviation"].map(CENSUS_CODE)
    df["id_icpsr"] = df["abbreviation"].map(ICPSR_CODE)
    df["type"] = df["id_state"].map(
        lambda x: (
            "district"
            if x == "11"
            else "territory"
            if x in TERRITORY_FIPS
            else "state"
        )
    )
    df["id_division"] = df["abbreviation"].map(STATE_DIVISION)
    df["id_region"] = df["id_division"].map(DIVISION_REGION)
    df["name_region"] = df["id_region"].map(REGION_NAMES)
    df["name_division"] = df["id_division"].map(DIVISION_NAMES)
    fas = pd.DataFrame(FREELY_ASSOCIATED_STATES)
    fas["type"] = "freely associated state"
    df = pd.concat([df, fas], ignore_index=True)
    fields = [
        ("id_state", pa.string()),
        ("abbreviation", pa.string()),
        ("name", pa.string()),
        ("id_gnis", pa.string()),
        ("id_census", pa.string()),
        ("id_icpsr", pa.string()),
        ("id_region", pa.string()),
        ("name_region", pa.string()),
        ("id_division", pa.string()),
        ("name_division", pa.string()),
        ("type", pa.string()),
    ]
    return write_table(df, "state", fields)


def county_type(name):
    if name == "District of Columbia":
        return "district"
    if name.endswith(" Parish"):
        return "parish"
    if name.endswith("Borough"):
        return "borough"
    if name.endswith("Census Area"):
        return "census area"
    if name.endswith("Municipality"):
        return "municipality"
    if name.endswith("Municipio"):
        return "municipio"
    if name.endswith("Planning Region"):
        return "planning region"
    if name.endswith(" city") or name == "Carson City":
        return "city"
    if name.endswith(" District"):
        return "district"
    if name.endswith(" Island") or name.endswith(" Islands"):
        return "island"
    return "county"


def island_area_county_type(name):
    last = name.split()[-1].lower()
    return {
        "district": "district",
        "island": "island",
        "islands": "island",
        "municipality": "municipality",
        "atoll": "atoll",
        "guam": "island",
    }.get(last, last)


def island_area_counties(existing_ids, state_names):
    raw = pd.read_csv(
        INPUT / "county" / "national_county2020.txt",
        sep="|",
        dtype=str,
        encoding="utf-8-sig",
    )
    raw = raw[raw["STATEFP"].isin(ISLAND_AREA_FIPS)].copy()
    raw["id_county"] = raw["STATEFP"] + raw["COUNTYFP"]
    raw = raw[~raw["id_county"].isin(existing_ids)]
    df = pd.DataFrame()
    df["id_county"] = raw["id_county"]
    df["name"] = raw["COUNTYNAME"]
    df["type"] = raw["COUNTYNAME"].map(island_area_county_type)
    df["id_state"] = raw["STATEFP"]
    df["abbreviation_state"] = raw["STATE"]
    df["name_state"] = df["id_state"].map(state_names)
    df["id_gnis"] = raw["COUNTYNS"]
    return df


def read_cbsa_delineation():
    df = pd.read_excel(INPUT / "cbsa" / "list1_2023.xlsx", header=2, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df = df[df["CBSA Code"].notna()].copy()
    for col in df.columns:
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
        df[col] = df[col].str.replace(r"\.0$", "", regex=True)
    return df


def clean_county(state_df):
    gaz = read_gazetteer("2025_Gaz_counties_national.txt")
    df = pd.DataFrame()
    df["id_county"] = gaz["GEOID"]
    df["name"] = gaz["NAME"]
    df["type"] = gaz["NAME"].map(county_type)
    df["id_state"] = gaz["GEOID"].str[:2]
    df["abbreviation_state"] = gaz["USPS"]
    df["id_gnis"] = gaz["ANSICODE"]
    state_names = state_df.set_index("id_state")["name"]
    df["name_state"] = df["id_state"].map(state_names)
    omb = read_cbsa_delineation()
    omb["id_county"] = omb["FIPS State Code"].str.zfill(2) + omb[
        "FIPS County Code"
    ].str.zfill(3)
    omb = omb.drop_duplicates(subset="id_county")
    omb["central_outlying"] = omb["Central/Outlying County"].str.lower()
    omb = omb.rename(
        columns={
            "CBSA Code": "id_cbsa",
            "CBSA Title": "name_cbsa",
            "CSA Code": "id_csa",
            "CSA Title": "name_csa",
        }
    )
    df = df.merge(
        omb[
            [
                "id_county",
                "id_cbsa",
                "name_cbsa",
                "id_csa",
                "name_csa",
                "central_outlying",
            ]
        ],
        on="id_county",
        how="left",
    )
    df["area_land_km2"] = area_km2(gaz["ALAND"]).to_numpy()
    df["area_water_km2"] = area_km2(gaz["AWATER"]).to_numpy()
    df["centroid"] = centroid_wkt(gaz["INTPTLAT"], gaz["INTPTLONG"]).to_numpy()
    island = island_area_counties(set(df["id_county"]), state_names)
    print(f"county: appending {len(island)} island-area county equivalents")
    df = pd.concat([df, island], ignore_index=True)
    fields = [
        ("id_county", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("name_state", pa.string()),
        ("id_gnis", pa.string()),
        ("id_cbsa", pa.string()),
        ("name_cbsa", pa.string()),
        ("id_csa", pa.string()),
        ("name_csa", pa.string()),
        ("central_outlying", pa.string()),
        ("area_land_km2", pa.float64()),
        ("area_water_km2", pa.float64()),
        ("centroid", pa.string()),
    ]
    return write_table(df, "county", fields)


def clean_place():
    gaz = read_gazetteer("2025_Gaz_place_national.txt")
    df = pd.DataFrame()
    df["id_place"] = gaz["GEOID"]
    df["name"] = gaz["NAME"]
    df["class_code"] = gaz["LSAD"]
    df["type"] = gaz["LSAD"].map(
        lambda x: (
            "census designated place" if x == "57" else "incorporated place"
        )
    )
    df["id_gnis"] = gaz["ANSICODE"]
    df["id_state"] = gaz["GEOID"].str[:2]
    df["abbreviation_state"] = gaz["USPS"]
    df["area_land_km2"] = area_km2(gaz["ALAND"])
    df["area_water_km2"] = area_km2(gaz["AWATER"])
    df["centroid"] = centroid_wkt(gaz["INTPTLAT"], gaz["INTPTLONG"])
    fields = [
        ("id_place", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
        ("class_code", pa.string()),
        ("id_gnis", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("area_land_km2", pa.float64()),
        ("area_water_km2", pa.float64()),
        ("centroid", pa.string()),
    ]
    return write_table(df, "place", fields)


def clean_census_tract_2020():
    gaz = read_gazetteer("2025_Gaz_tracts_national.txt")
    df = pd.DataFrame()
    df["id_census_tract"] = gaz["GEOID"]
    df["id_county"] = gaz["GEOID"].str[:5]
    df["id_state"] = gaz["GEOID"].str[:2]
    df["area_land_km2"] = area_km2(gaz["ALAND"])
    df["area_water_km2"] = area_km2(gaz["AWATER"])
    df["centroid"] = centroid_wkt(gaz["INTPTLAT"], gaz["INTPTLONG"])
    fields = [
        ("id_census_tract", pa.string()),
        ("id_county", pa.string()),
        ("id_state", pa.string()),
        ("area_land_km2", pa.float64()),
        ("area_water_km2", pa.float64()),
        ("centroid", pa.string()),
    ]
    return write_table(df, "census_tract_2020", fields)


def clean_zcta_2020():
    gaz = read_gazetteer("2025_Gaz_zcta_national.txt")
    df = pd.DataFrame()
    df["id_zcta"] = gaz["GEOID"]
    df["area_land_km2"] = area_km2(gaz["ALAND"])
    df["area_water_km2"] = area_km2(gaz["AWATER"])
    df["centroid"] = centroid_wkt(gaz["INTPTLAT"], gaz["INTPTLONG"])
    fields = [
        ("id_zcta", pa.string()),
        ("area_land_km2", pa.float64()),
        ("area_water_km2", pa.float64()),
        ("centroid", pa.string()),
    ]
    return write_table(df, "zcta_2020", fields)


def clean_cbsa_2023():
    omb = read_cbsa_delineation()
    df = omb.drop_duplicates(subset="CBSA Code").copy()
    out = pd.DataFrame()
    out["id_cbsa"] = df["CBSA Code"]
    out["name"] = df["CBSA Title"]
    out["type"] = df["Metropolitan/Micropolitan Statistical Area"].map(
        lambda x: (
            "metropolitan"
            if isinstance(x, str) and x.startswith("Metro")
            else "micropolitan"
        )
    )
    out["id_csa"] = df["CSA Code"]
    out["name_csa"] = df["CSA Title"]
    fields = [
        ("id_cbsa", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
        ("id_csa", pa.string()),
        ("name_csa", pa.string()),
    ]
    return write_table(out, "cbsa_2023", fields)


def cd_type(id_state):
    if id_state == "72":
        return "resident_commissioner"
    if id_state in ("11", "60", "66", "69", "78"):
        return "delegate"
    return "voting"


def clean_congressional_district_119():
    gaz = read_gazetteer("2025_Gaz_119CDs_national.txt")
    df = pd.DataFrame()
    df["id_congressional_district"] = gaz["GEOID"]
    df["id_state"] = gaz["GEOID"].str[:2]
    df["abbreviation_state"] = gaz["USPS"]
    df["district"] = gaz["GEOID"].str[2:]
    df["type"] = df["id_state"].map(cd_type)
    df.loc[df["district"] == "ZZ", "type"] = "not defined"
    fields = [
        ("id_congressional_district", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("district", pa.string()),
        ("type", pa.string()),
    ]
    return write_table(df, "congressional_district_119", fields)


def clean_puma_2020(state_df):
    import tempfile
    import zipfile

    from dbfread import DBF

    puma_dir = INPUT / "tiger_puma"
    for attempt in range(4):
        sources = (
            sorted(puma_dir.glob("tl_*_puma20.dbf"))
            if puma_dir.exists()
            else []
        )
        zips = (
            sorted(puma_dir.glob("tl_*_puma20.zip"))
            if puma_dir.exists()
            else []
        )
        if len(sources) >= 50 or len(zips) >= 50:
            break
        if attempt < 3:
            print(
                f"puma_2020: found {len(sources)} dbf / {len(zips)} zip files, waiting 120s (attempt {attempt + 1}/3)"
            )
            time.sleep(120)
    else:
        raise FileNotFoundError(
            f"tiger_puma incomplete: {len(sources)} dbf / {len(zips)} zip files found, expected >= 50"
        )
    tmpdir = tempfile.mkdtemp(prefix="tiger_puma_")
    if len(sources) < 50:
        sources = []
        for zpath in zips:
            with zipfile.ZipFile(zpath) as zf:
                dbf_names = [n for n in zf.namelist() if n.endswith(".dbf")]
                for n in dbf_names:
                    sources.append(Path(zf.extract(n, tmpdir)))
        sources = sorted(sources)
    records = []
    for path in sources:
        for rec in DBF(path, encoding="utf-8"):
            records.append(
                {
                    "id_puma": str(rec["GEOID20"]).strip(),
                    "name": str(rec["NAMELSAD20"]).strip(),
                    "id_state": str(rec["STATEFP20"]).strip(),
                    "aland": rec["ALAND20"],
                    "awater": rec["AWATER20"],
                    "lat": str(rec["INTPTLAT20"]).strip().lstrip("+"),
                    "lon": str(rec["INTPTLON20"]).strip().lstrip("+"),
                }
            )
    df = pd.DataFrame(records)
    abbrev = state_df.set_index("id_state")["abbreviation"]
    df["abbreviation_state"] = df["id_state"].map(abbrev)
    df["area_land_km2"] = pd.to_numeric(df["aland"], errors="coerce") / 1e6
    df["area_water_km2"] = pd.to_numeric(df["awater"], errors="coerce") / 1e6
    df["centroid"] = "POINT(" + df["lon"] + " " + df["lat"] + ")"
    fields = [
        ("id_puma", pa.string()),
        ("name", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("area_land_km2", pa.float64()),
        ("area_water_km2", pa.float64()),
        ("centroid", pa.string()),
    ]
    return write_table(df, "puma_2020", fields)


def null_invalid_fipst(id_state, state_df, table):
    valid = set(state_df["id_state"])
    invalid = ~id_state.isin(valid)
    if invalid.any():
        dist = id_state[invalid].value_counts().to_dict()
        print(f"{table}: nulled id_state for FIPST not in state table: {dist}")
    return id_state.where(~invalid, None)


def clean_school_district(state_df):
    raw = pd.read_csv(
        INPUT / "ccd" / "ccd_lea_029_2324_w_1a_073124.csv",
        dtype=str,
        encoding="utf-8-sig",
        low_memory=False,
    )
    df = pd.DataFrame()
    df["id_school_district"] = raw["LEAID"].str.strip().str.zfill(7)
    df["name"] = raw["LEA_NAME"]
    df["type"] = raw["LEA_TYPE_TEXT"]
    df["level"] = raw["LEVEL"] if "LEVEL" in raw.columns else None
    df["id_state"] = null_invalid_fipst(
        raw["FIPST"].str.strip().str.zfill(2), state_df, "school_district"
    )
    df["abbreviation_state"] = raw["ST"]
    df["id_county"] = (
        raw["CNTY"].str.strip().str.zfill(5) if "CNTY" in raw.columns else None
    )
    df["operational_status"] = raw["SY_STATUS_TEXT"]
    fields = [
        ("id_school_district", pa.string()),
        ("name", pa.string()),
        ("type", pa.string()),
        ("level", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("id_county", pa.string()),
        ("operational_status", pa.string()),
    ]
    return write_table(df, "school_district", fields)


def clean_school(state_df):
    raw = pd.read_csv(
        INPUT / "ccd" / "ccd_sch_029_2324_w_1a_073124.csv",
        dtype=str,
        encoding="utf-8-sig",
        low_memory=False,
    )
    df = pd.DataFrame()
    df["id_school"] = raw["NCESSCH"].str.strip().str.zfill(12)
    df["name"] = raw["SCH_NAME"]
    df["id_school_district"] = raw["LEAID"].str.strip().str.zfill(7)
    df["id_state"] = null_invalid_fipst(
        raw["FIPST"].str.strip().str.zfill(2), state_df, "school"
    )
    df["abbreviation_state"] = raw["ST"]
    df["id_county"] = (
        raw["CNTY"].str.strip().str.zfill(5) if "CNTY" in raw.columns else None
    )
    df["type"] = raw["SCH_TYPE_TEXT"]
    df["level"] = raw["LEVEL"]
    df["charter"] = raw["CHARTER_TEXT"]
    df["operational_status"] = raw["SY_STATUS_TEXT"]
    fields = [
        ("id_school", pa.string()),
        ("name", pa.string()),
        ("id_school_district", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("id_county", pa.string()),
        ("type", pa.string()),
        ("level", pa.string()),
        ("charter", pa.string()),
        ("operational_status", pa.string()),
    ]
    return write_table(df, "school", fields)


SECTOR_MAP = {
    0: "administrative unit",
    1: "public 4-year or above",
    2: "private not-for-profit 4-year or above",
    3: "private for-profit 4-year or above",
    4: "public 2-year",
    5: "private not-for-profit 2-year",
    6: "private for-profit 2-year",
    7: "public less-than-2-year",
    8: "private not-for-profit less-than-2-year",
    9: "private for-profit less-than-2-year",
    99: None,
}
CONTROL_MAP = {
    1: "public",
    2: "private not-for-profit",
    3: "private for-profit",
    -3: None,
}
ICLEVEL_MAP = {
    1: "4-year or above",
    2: "2-year",
    3: "less-than-2-year",
    -3: None,
}
CYACTIVE_MAP = {1: "active", 2: "inactive", 3: "closed"}


def map_code(series, mapping):
    codes = pd.to_numeric(series, errors="coerce")
    return codes.map(lambda x: mapping.get(int(x)) if pd.notna(x) else None)


def clean_higher_education_institution():
    raw = pd.read_csv(
        INPUT / "ipeds" / "HD2024.csv",
        dtype=str,
        encoding="latin-1",
        low_memory=False,
    )
    raw.columns = [c.lstrip("﻿\xef\xbb\xbf") for c in raw.columns]
    print(
        "higher_education_institution CYACTIVE values:",
        sorted(raw["CYACTIVE"].str.strip().unique()),
    )
    df = pd.DataFrame()
    df["id_institution"] = raw["UNITID"].str.strip()
    opeid = raw["OPEID"].str.strip()
    df["id_opeid"] = opeid.map(lambda x: None if x in ("-2", "") else x)
    df["name"] = raw["INSTNM"]
    df["id_state"] = raw["FIPS"].str.strip().str.zfill(2)
    df["abbreviation_state"] = raw["STABBR"]
    county = pd.to_numeric(raw["COUNTYCD"], errors="coerce")
    df["id_county"] = county.map(
        lambda x: str(int(x)).zfill(5) if pd.notna(x) and x > 0 else None
    )
    df["city"] = raw["CITY"]
    df["zip"] = raw["ZIP"].str.strip().str[:5]
    df["sector"] = map_code(raw["SECTOR"], SECTOR_MAP)
    df["control"] = map_code(raw["CONTROL"], CONTROL_MAP)
    df["level"] = map_code(raw["ICLEVEL"], ICLEVEL_MAP)
    df["operational_status"] = map_code(raw["CYACTIVE"], CYACTIVE_MAP)
    fields = [
        ("id_institution", pa.string()),
        ("id_opeid", pa.string()),
        ("name", pa.string()),
        ("id_state", pa.string()),
        ("abbreviation_state", pa.string()),
        ("id_county", pa.string()),
        ("city", pa.string()),
        ("zip", pa.string()),
        ("sector", pa.string()),
        ("control", pa.string()),
        ("level", pa.string()),
        ("operational_status", pa.string()),
    ]
    return write_table(df, "higher_education_institution", fields)


def legislator_record(person):
    ids = person.get("id", {})
    name = person.get("name", {})
    bio = person.get("bio", {})

    def s(value):
        return None if value is None else str(value)

    fec = ids.get("fec")
    birthday = bio.get("birthday")
    return {
        "id_bioguide": s(ids.get("bioguide")),
        "name_first": s(name.get("first")),
        "name_middle": s(name.get("middle")),
        "name_last": s(name.get("last")),
        "name_suffix": s(name.get("suffix")),
        "name_official_full": s(name.get("official_full")),
        "birthday": datetime.date.fromisoformat(birthday)
        if birthday
        else None,
        "gender": s(bio.get("gender")),
        "id_govtrack": s(ids.get("govtrack")),
        "id_icpsr": s(ids.get("icpsr")),
        "id_lis": s(ids.get("lis")),
        "id_thomas": s(ids.get("thomas")),
        "id_opensecrets": s(ids.get("opensecrets")),
        "id_votesmart": s(ids.get("votesmart")),
        "id_cspan": s(ids.get("cspan")),
        "id_fec": ",".join(fec) if fec else None,
        "id_wikidata": s(ids.get("wikidata")),
        "wikipedia": s(ids.get("wikipedia")),
        "ballotpedia": s(ids.get("ballotpedia")),
    }


def clean_congress_member():
    with open(
        INPUT / "congress_legislators" / "legislators-current.yaml"
    ) as f:
        current = yaml.safe_load(f)
    with open(
        INPUT / "congress_legislators" / "legislators-historical.yaml"
    ) as f:
        historical = yaml.safe_load(f)
    df = pd.DataFrame([legislator_record(p) for p in current + historical])
    df = df.drop_duplicates(subset="id_bioguide", keep="first")
    fields = [
        ("id_bioguide", pa.string()),
        ("name_first", pa.string()),
        ("name_middle", pa.string()),
        ("name_last", pa.string()),
        ("name_suffix", pa.string()),
        ("name_official_full", pa.string()),
        ("birthday", pa.date32()),
        ("gender", pa.string()),
        ("id_govtrack", pa.string()),
        ("id_icpsr", pa.string()),
        ("id_lis", pa.string()),
        ("id_thomas", pa.string()),
        ("id_opensecrets", pa.string()),
        ("id_votesmart", pa.string()),
        ("id_cspan", pa.string()),
        ("id_fec", pa.string()),
        ("id_wikidata", pa.string()),
        ("wikipedia", pa.string()),
        ("ballotpedia", pa.string()),
    ]
    return write_table(df, "congress_member", fields)


def clean_naics_2022():
    raw = pd.read_excel(
        INPUT / "naics" / "2-6_digit_2022_Codes.xlsx", sheet_name=0, dtype=str
    )
    codes = raw.iloc[:, 1].astype(str).str.strip()
    titles = raw.iloc[:, 2].astype(str).str.strip().str.rstrip("T").str.strip()
    df = pd.DataFrame({"id_naics": codes, "name": titles})
    df = df[
        df["id_naics"].notna()
        & (df["id_naics"] != "nan")
        & (df["id_naics"] != "")
    ]
    df["level"] = df["id_naics"].map(lambda c: 2 if "-" in c else len(c))
    sector_map = {}
    for _, row in df[df["level"] == 2].iterrows():
        code = row["id_naics"]
        if "-" in code:
            lo, hi = code.split("-")
            for two in range(int(lo), int(hi) + 1):
                sector_map[str(two)] = (code, row["name"])
        else:
            sector_map[code] = (code, row["name"])
    name_by_code = df.set_index("id_naics")["name"].to_dict()

    def parents(row):
        code, level = row["id_naics"], row["level"]
        out = {
            "id_sector": None,
            "name_sector": None,
            "id_subsector": None,
            "name_subsector": None,
            "id_industry_group": None,
            "name_industry_group": None,
            "id_naics_industry": None,
            "name_naics_industry": None,
        }
        if level > 2:
            sector = sector_map.get(code[:2])
            if sector:
                out["id_sector"], out["name_sector"] = sector
        if level > 3:
            out["id_subsector"] = code[:3]
            out["name_subsector"] = name_by_code.get(code[:3])
        if level > 4:
            out["id_industry_group"] = code[:4]
            out["name_industry_group"] = name_by_code.get(code[:4])
        if level > 5:
            out["id_naics_industry"] = code[:5]
            out["name_naics_industry"] = name_by_code.get(code[:5])
        return pd.Series(out)

    df = pd.concat(
        [
            df.reset_index(drop=True),
            df.apply(parents, axis=1).reset_index(drop=True),
        ],
        axis=1,
    )
    fields = [
        ("id_naics", pa.string()),
        ("name", pa.string()),
        ("level", pa.int64()),
        ("id_sector", pa.string()),
        ("name_sector", pa.string()),
        ("id_subsector", pa.string()),
        ("name_subsector", pa.string()),
        ("id_industry_group", pa.string()),
        ("name_industry_group", pa.string()),
        ("id_naics_industry", pa.string()),
        ("name_naics_industry", pa.string()),
    ]
    return write_table(df, "naics_2022", fields)


def clean_soc_2018():
    raw = pd.read_excel(
        INPUT / "soc" / "soc_structure_2018.xlsx",
        sheet_name=0,
        header=None,
        dtype=str,
    )
    header_idx = raw.index[
        raw.iloc[:, 0].astype(str).str.strip() == "Major Group"
    ][0]
    data = raw.iloc[header_idx + 1 :].copy()
    level_cols = {0: "major", 1: "minor", 2: "broad", 3: "detailed"}
    rows = []
    for _, r in data.iterrows():
        title = str(r.iloc[4]).strip() if pd.notna(r.iloc[4]) else None
        for col_idx, level in level_cols.items():
            val = r.iloc[col_idx]
            if pd.notna(val) and str(val).strip():
                rows.append(
                    {"id_soc": str(val).strip(), "name": title, "level": level}
                )
                break
    names = {r["id_soc"]: r["name"] for r in rows}
    current = {"major": None, "minor": None, "broad": None}
    for row in rows:
        level, code = row["level"], row["id_soc"]
        if level == "major":
            current = {"major": code, "minor": None, "broad": None}
        elif level == "minor":
            current["minor"], current["broad"] = code, None
        elif level == "broad":
            current["broad"] = code
        row["id_major_group"] = current["major"] if level != "major" else None
        row["id_minor_group"] = (
            current["minor"] if level in ("broad", "detailed") else None
        )
        row["id_broad_occupation"] = (
            current["broad"] if level == "detailed" else None
        )
        for parent, col in [
            ("id_major_group", "name_major_group"),
            ("id_minor_group", "name_minor_group"),
            ("id_broad_occupation", "name_broad_occupation"),
        ]:
            row[col] = names.get(row[parent]) if row[parent] else None
        if row["id_major_group"]:
            assert code[:2] == row["id_major_group"][:2], (
                f"soc hierarchy mismatch: {code}"
            )
        if row["id_broad_occupation"]:
            assert code[:5] == row["id_broad_occupation"][:5], (
                f"soc hierarchy mismatch: {code}"
            )
    df = pd.DataFrame(rows)
    fields = [
        ("id_soc", pa.string()),
        ("name", pa.string()),
        ("level", pa.string()),
        ("id_major_group", pa.string()),
        ("name_major_group", pa.string()),
        ("id_minor_group", pa.string()),
        ("name_minor_group", pa.string()),
        ("id_broad_occupation", pa.string()),
        ("name_broad_occupation", pa.string()),
    ]
    return write_table(df, "soc_2018", fields)


# Census industry/occupation code lists, one directory table per vintage.
# (slug, source file, sheet, kind). Vintage-in-name follows naics_2022/soc_2018.
CENSUS_IO = [
    ("census_industry_2002", "2002-acs-ind-codes.xls", 0, "industry"),
    (
        "census_industry_2007",
        "2007-acs-ind-codes.xls",
        " Ind Codes ",
        "industry",
    ),
    ("census_industry_2012", "census-2012-final-code-list.xls", 0, "industry"),
    (
        "census_industry_2017",
        "2017-industry-code-list-with-crosswalk.xlsx",
        "2017 Census Industry Code List",
        "industry",
    ),
    (
        "census_occupation_2002",
        "2002-census-occupation-codes.xls",
        0,
        "occupation",
    ),
    (
        "census_occupation_2010",
        "2010-occ-codes-with-crosswalk-from-2002-2011.xls",
        "2010OccCodeList",
        "occupation",
    ),
    (
        "census_occupation_2018",
        "2018-occupation-code-list-and-crosswalk.xlsx",
        "2018 Census Occ Code List",
        "occupation",
    ),
]


def _io_cell(v):
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def clean_census_io(slug, filename, sheet, kind):
    """Extract the leaf (4-digit) Census industry/occupation codes.

    Group/section rows carry code ranges (e.g. 0170-0490) and are skipped;
    only detailed 4-digit codes — the values ATUS stores — are kept. The
    crosswalk column (NAICS for industry, SOC for occupation) is carried as
    published (may hold ranges or multiple codes for a single census code).
    """
    df = pd.read_excel(
        INPUT / "census_io" / filename,
        sheet_name=sheet,
        header=None,
        dtype=str,
    )
    xwalk_kw = "naics code" if kind == "industry" else "soc code"
    id_col = (
        "id_census_industry" if kind == "industry" else "id_census_occupation"
    )
    xwalk_out = "id_naics" if kind == "industry" else "id_soc"
    code_col = xwalk_col = header_idx = None
    for i in range(min(40, len(df))):
        row = [_io_cell(x) for x in df.iloc[i].tolist()]
        for j, c in enumerate(cell.lower() for cell in row):
            if (
                code_col is None
                and c.endswith("code")
                and (
                    "census code" in c
                    or ("industry code" in c and "naics" not in c)
                    or ("occupation code" in c and "soc" not in c)
                )
            ):
                code_col, header_idx = j, i
            if xwalk_col is None and c.endswith("code") and xwalk_kw in c:
                xwalk_col = j
        if code_col is not None:
            break
    if code_col is None:
        raise ValueError(f"census_io: no code column found in {filename}")
    rows = []
    for i in range(header_idx + 1, len(df)):
        raw = [_io_cell(x) for x in df.iloc[i].tolist()]
        code = raw[code_col] if code_col < len(raw) else ""
        if not re.fullmatch(r"\d{3,4}", code):
            continue
        name = next((raw[j] for j in range(code_col) if raw[j]), "")
        xw = (
            raw[xwalk_col]
            if (xwalk_col is not None and xwalk_col < len(raw))
            else ""
        )
        if xw.lower() in ("none", "n/a", "na"):
            xw = ""
        rows.append({id_col: code.zfill(4), "name": name, xwalk_out: xw})
    out = (
        pd.DataFrame(rows)
        .query("name != ''")
        .drop_duplicates(id_col, keep="first")
        .reset_index(drop=True)
    )
    fields = [
        (id_col, pa.string()),
        ("name", pa.string()),
        (xwalk_out, pa.string()),
    ]
    return write_table(out, slug, fields)


PRIMARY_KEYS = {
    "state": "id_state",
    "county": "id_county",
    "place": "id_place",
    "census_tract_2020": "id_census_tract",
    "zcta_2020": "id_zcta",
    "cbsa_2023": "id_cbsa",
    "congressional_district_119": "id_congressional_district",
    "puma_2020": "id_puma",
    "school_district": "id_school_district",
    "school": "id_school",
    "higher_education_institution": "id_institution",
    "congress_member": "id_bioguide",
    "naics_2022": "id_naics",
    "soc_2018": "id_soc",
    **{
        slug: (
            "id_census_industry"
            if kind == "industry"
            else "id_census_occupation"
        )
        for slug, _f, _s, kind in CENSUS_IO
    },
}


def main():
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_colwidth", 28)
    results = {}
    runners = [
        ("state", clean_state),
        ("county", lambda: clean_county(results["state"])),
        ("place", clean_place),
        ("census_tract_2020", clean_census_tract_2020),
        ("zcta_2020", clean_zcta_2020),
        ("cbsa_2023", clean_cbsa_2023),
        ("congressional_district_119", clean_congressional_district_119),
        ("school_district", lambda: clean_school_district(results["state"])),
        ("school", lambda: clean_school(results["state"])),
        ("higher_education_institution", clean_higher_education_institution),
        ("congress_member", clean_congress_member),
        ("naics_2022", clean_naics_2022),
        ("soc_2018", clean_soc_2018),
        ("puma_2020", lambda: clean_puma_2020(results["state"])),
        *[
            (slug, lambda f=f, s=s, k=k, sl=slug: clean_census_io(sl, f, s, k))
            for slug, f, s, k in CENSUS_IO
        ],
    ]
    selected = set(sys.argv[1:]) or {slug for slug, _ in runners}
    if selected & {"county", "school_district", "school", "puma_2020"}:
        selected.add("state")
    failures = {}
    for slug, fn in runners:
        if slug not in selected:
            continue
        try:
            results[slug] = fn()
            print(f"done: {slug} ({len(results[slug])} rows)")
        except Exception as exc:
            failures[slug] = f"{type(exc).__name__}: {exc}"
            print(f"FAILED: {slug} — {failures[slug]}")

    print("\n===== SUMMARY =====")
    for slug in PRIMARY_KEYS:
        if slug not in selected:
            continue
        if slug in failures:
            print(f"\n--- {slug}: FAILED — {failures[slug]}")
            continue
        df = results[slug]
        pk = PRIMARY_KEYS[slug]
        unique = df[pk].is_unique
        nulls = int(df[pk].isna().sum())
        print(
            f"\n--- {slug}: rows={len(df)} cols={len(df.columns)} pk={pk} unique={'OK' if unique else 'FAIL'} pk_nulls={nulls}"
        )
        print(df.head(2).to_string(index=False))
        assert unique, f"{slug}: duplicate {pk}"
        assert nulls == 0, f"{slug}: null {pk}"


if __name__ == "__main__":
    main()
