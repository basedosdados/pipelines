"""Clean AustralianPoliticians (Rohan Alexander) into Data Basis tables.

Source: https://github.com/RohanAlexander/australian_politicians (data/, MIT).
Produces one typed Parquet per table under output/, matching the architecture
CSVs in architecture/. One-shot onboarding => typed Parquet with explicit schema.

Six tables: politician, party_affiliation, house_member, senator, ministry,
dicionario.
"""

import csv
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

HERE = Path(__file__).resolve().parent
INPUT = HERE / "input"
OUTPUT = HERE / "output"
ARCH = HERE / "architecture"
INPUT.mkdir(exist_ok=True)
OUTPUT.mkdir(exist_ok=True)

# Pinned source commit (master as of onboarding)
REF = "0662c0c78bc9e0f61cffd2970934fd6554ed6cd4"
BASE = f"https://raw.githubusercontent.com/RohanAlexander/australian_politicians/{REF}/data"
SRC_FILES = [
    "australian_politicians-all.csv",
    "australian_politicians-all-by_party.csv",
    "australian_politicians-mps-by_division.csv",
    "australian_politicians-senators-by_state.csv",
    "australian_politicians-ministries.csv",
    "australian_politicians-uniqueID_to_aphID.csv",
]

# Source uses a bare surname in a few satellite rows where the master uses a
# disambiguated id (surname + birth year). Only verified same-person fixes here.
# Connelly = Vince Connelly (Connelly1978), Stirling WA, entered 2019-05-18.
ID_REMAP = {"Connelly": "Connelly1978"}

# Abbreviation -> ASGS state code (br_bd_diretorios_au.state.id_state)
STATE_MAP = {
    "NSW": "1",
    "VIC": "2",
    "QLD": "3",
    "SA": "4",
    "WA": "5",
    "TAS": "6",
    "NT": "7",
    "ACT": "8",
    "OT": "9",
}

# Map pyarrow types from architecture bigquery_type
PA_TYPE = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}


def download():
    for f in SRC_FILES:
        dst = INPUT / f
        if dst.exists():
            continue
        print(f"downloading {f}")
        r = requests.get(f"{BASE}/{f}", timeout=60)
        r.raise_for_status()
        dst.write_bytes(r.content)


def read_src(name):
    """Read a source CSV, treating 'NA' and '' as missing."""
    return pd.read_csv(
        INPUT / name, dtype=str, keep_default_na=False, na_values=["NA", ""]
    )


def remap_ids(s):
    """Normalise bare-surname politician ids to the master's disambiguated id."""
    return s.map(lambda v: ID_REMAP.get(v, v) if pd.notna(v) else v)


def arch_cols(table):
    """Ordered (name, bigquery_type) from the architecture CSV."""
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return [(r["name"], r["bigquery_type"]) for r in csv.DictReader(fh)]


def to_date(s):
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").dt.date


def fill_flag(s):
    """1-or-missing flag -> strict '0'/'1' string."""
    return s.where(s.notna(), "0").map(
        lambda v: "1" if str(v).strip() in ("1", "1.0") else "0"
    )


def keep_flag(s):
    """0/1 flag, missing preserved as NULL."""

    def m(v):
        if pd.isna(v):
            return None
        return "1" if str(v).strip() in ("1", "1.0") else "0"

    return s.map(m)


def write_table(table, df):
    """Reorder to architecture, cast, write typed Parquet."""
    cols = arch_cols(table)
    names = [c for c, _ in cols]
    missing = set(names) - set(df.columns)
    assert not missing, f"{table}: missing built columns {missing}"
    df = df[names]
    fields = []
    arrays = []
    for name, typ in cols:
        pat = PA_TYPE[typ]
        col = df[name]
        if typ == "INT64":
            col = pd.to_numeric(col, errors="coerce")
            vals = [None if pd.isna(v) else int(v) for v in col]
        elif typ == "FLOAT64":
            vals = [None if pd.isna(v) else float(v) for v in col]
        elif typ == "DATE":
            vals = [None if (v is None or pd.isna(v)) else v for v in col]
        else:  # STRING
            vals = [None if pd.isna(v) else str(v) for v in col]
        arr = pa.array(vals, type=pat)
        fields.append(pa.field(name, pat))
        arrays.append(arr)
    tbl = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    pq.write_table(tbl, OUTPUT / f"{table}.parquet", compression="snappy")
    print(
        f"  wrote output/{table}.parquet  rows={tbl.num_rows} cols={tbl.num_columns}"
    )
    return tbl.num_rows


def build_politician():
    df = read_src("australian_politicians-all.csv")
    aph = read_src("australian_politicians-uniqueID_to_aphID.csv")
    aph_map = dict(zip(remap_ids(aph["uniqueID"]), aph["aphID"], strict=True))
    out = pd.DataFrame(
        {
            "id_politician": remap_ids(df["uniqueID"]),
            "id_wikidata": df["wikidataID"],
            "id_aph": df["uniqueID"].map(aph_map),
            "surname": df["surname"],
            "all_other_names": df["allOtherNames"],
            "first_name": df["firstName"],
            "common_name": df["commonName"],
            "display_name": df["displayName"],
            "earlier_or_later_names": df["earlierOrLaterNames"],
            "title": df["title"],
            "gender": df["gender"],
            "birth_date": to_date(df["birthDate"]),
            "birth_year": df["birthYear"],
            "birth_place": df["birthPlace"],
            "death_date": to_date(df["deathDate"]),
            "indicator_member": keep_flag(df["member"]),
            "indicator_senator": keep_flag(df["senator"]),
            "indicator_prime_minister": fill_flag(df["wasPrimeMinister"]),
            "url_wikipedia": df["wikipedia"],
            "url_adb": df["adb"],
            "comments": df["comments"],
        }
    )
    return out


def build_party():
    df = read_src("australian_politicians-all-by_party.csv")
    out = pd.DataFrame(
        {
            "id_politician": remap_ids(df["uniqueID"]),
            "party_abbreviation": df["partyAbbrev"],
            "party_name": df["partyName"],
            "party_simplified_name": df["partySimplifiedName"],
            "date_start": to_date(df["partyFrom"]),
            "date_end": to_date(df["partyTo"]),
            "indicator_party_changed_name": fill_flag(df["partyChangedName"]),
            "indicator_specific_date_inputted": fill_flag(
                df["partySpecificDateInputted"]
            ),
            "comments": df["partyComments"],
        }
    )
    return out


def build_house():
    df = read_src("australian_politicians-mps-by_division.csv")
    ebe = df["enteredAtByElection"].map(
        lambda v: (
            "1"
            if str(v).strip() in ("1", "Yes")
            else ("0" if str(v).strip() == "No" else None)
        )
    )
    out = pd.DataFrame(
        {
            "id_politician": remap_ids(df["uniqueID"]),
            "id_state": df["stateOfDivision"].map(STATE_MAP),
            "abbreviation_state": df["stateOfDivision"],
            "division": df["division"],
            "date_start": to_date(df["mpFrom"]),
            "date_end": to_date(df["mpTo"]),
            "end_reason": df["mpEndReason"],
            "indicator_entered_at_by_election": ebe,
            "indicator_changed_seat": fill_flag(df["mpChangedSeat"]),
            "comments": df["mpComments"],
        }
    )
    return out


def build_senator():
    df = read_src("australian_politicians-senators-by_state.csv")
    out = pd.DataFrame(
        {
            "id_politician": remap_ids(df["uniqueID"]),
            "id_state": df["senatorsState"].map(STATE_MAP),
            "abbreviation_state": df["senatorsState"],
            "date_start": to_date(df["senatorFrom"]),
            "date_end": to_date(df["senatorTo"]),
            "end_reason": df["senatorEndReason"],
            "indicator_section_15_selection": keep_flag(df["sec15Sel"]),
            "comments": df["senatorComments"],
        }
    )
    return out


def build_ministry():
    df = read_src("australian_politicians-ministries.csv")
    out = pd.DataFrame(
        {
            "id_politician": remap_ids(df["uniqueID"]),
            "ministry": df["ministry"],
            "ministry_number": df["ministry_number"],
            "ministry_party": df["ministry_party"],
            "ministry_title": df["ministry_title"],
            "display_name": df["ministry_name"],
            "date_start": to_date(df["ministry_from"]),
            "date_end": to_date(df["ministry_to"]),
            "indicator_assistant_or_secretary": fill_flag(
                df["ministry_assistant_minister_or_parliamentary_secretary"]
            ),
            "comments": df["ministry_comment"],
        }
    )
    return out


def build_dicionario():
    """value->label maps for covered_by_dictionary columns (all boolean flags)."""
    flag_cols = {
        "politician": [
            "indicator_member",
            "indicator_senator",
            "indicator_prime_minister",
        ],
        "party_affiliation": [
            "indicator_party_changed_name",
            "indicator_specific_date_inputted",
        ],
        "house_member": [
            "indicator_entered_at_by_election",
            "indicator_changed_seat",
        ],
        "senator": ["indicator_section_15_selection"],
        "ministry": ["indicator_assistant_or_secretary"],
    }
    rows = []
    for table, cols in flag_cols.items():
        for c in cols:
            for key, val in (("0", "No"), ("1", "Yes")):
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": c,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": val,
                    }
                )
    return pd.DataFrame(rows)


BUILDERS = {
    "politician": build_politician,
    "party_affiliation": build_party,
    "house_member": build_house,
    "senator": build_senator,
    "ministry": build_ministry,
    "dicionario": build_dicionario,
}


def main():
    download()
    counts = {}
    for table, fn in BUILDERS.items():
        print(table)
        counts[table] = write_table(table, fn())
    print("\nROW COUNTS:", counts)


if __name__ == "__main__":
    main()
