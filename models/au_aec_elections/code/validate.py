"""Validate the cleaned au_aec_elections parquet against the raw AEC sources.

Checks, in order:
  1. Row counts — every cleaned table matches the total data rows of its sources.
  2. Key columns — no NULLs where the grain requires a value.
  3. Staging typing — every parquet column is STRING, and no NULL was stringified.
  4. Substance — spot checks against facts independently known about the elections.

Run:  uv run python models/au_aec_elections/code/validate.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pipelines.datasets.au_aec_elections import utils
from pipelines.datasets.au_aec_elections.constants import constants, data_root

ROOT = data_root()
OUTPUT = ROOT / "output"

failures: list[str] = []
notes: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"  FAIL {msg}")


def ok(msg: str) -> None:
    print(f"  ok   {msg}")


def read_table(table: str) -> pd.DataFrame:
    path = OUTPUT / table
    if not path.exists():
        fail(f"{table}: no output directory")
        return pd.DataFrame()
    frames = []
    for f in sorted(path.rglob("*.parquet")):
        df = pd.read_parquet(f)
        if f.parent.name.startswith("year="):
            df["year"] = f.parent.name.split("=", 1)[1]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def source_rows(pattern_files: list[Path], preamble: bool = True) -> int:
    total = 0
    for f in pattern_files:
        df = (
            utils.read_aec_csv(f)
            if preamble
            else utils.read_transparency_csv(f)
        )
        total += len(df)
    return total


def event_dirs() -> list[tuple[int, Path]]:
    out = []
    for ev, (year, _seg) in constants.FEDERAL_ELECTIONS.value.items():
        out.append((ev, ROOT / "input" / "results" / str(year)))
    for ev in constants.EXTRA_EVENTS.value:
        out.append((ev, ROOT / "input" / "results_extra" / str(ev)))
    return out


def files_for(stem: str, by_state: bool = False) -> list[Path]:
    found = []
    for ev, d in event_dirs():
        if by_state:
            found.extend(sorted(d.glob(f"{stem}-{ev}-*.csv")))
        else:
            p = d / f"{stem}-{ev}.csv"
            if p.exists():
                found.append(p)
    return found


# ======================================================================================

print("\n=== 1. Row counts against source ===")

RESULT_SOURCES = {
    "polling_place": ("GeneralPollingPlacesDownload", False),
    "party": ("GeneralPartyDetailsDownload", False),
    "house_candidate": ("HouseCandidatesDownload", False),
    "house_first_preference_division": (
        "HouseFirstPrefsByCandidateByVoteTypeDownload",
        False,
    ),
    "house_first_preference_polling_place": (
        "HouseStateFirstPrefsByPollingPlaceDownload",
        True,
    ),
    "house_two_candidate_preferred_polling_place": (
        "HouseTcpByCandidateByPollingPlaceDownload",
        False,
    ),
    "house_two_party_preferred_division": (
        "HouseTppByDivisionDownload",
        False,
    ),
    "house_two_party_preferred_polling_place": (
        "HouseTppByPollingPlaceDownload",
        False,
    ),
    "senate_candidate": ("SenateCandidatesDownload", False),
    "senate_first_preference_division": (
        "SenateFirstPrefsByDivisionByVoteTypeDownload",
        False,
    ),
    "referendum_polling_place": (
        "ReferendumPollingPlaceResultsByStateDownload",
        True,
    ),
}

tables: dict[str, pd.DataFrame] = {}
for table in constants.TABLES.value:
    tables[table] = read_table(table)

for table, (stem, by_state) in RESULT_SOURCES.items():
    expected = source_rows(files_for(stem, by_state))
    actual = len(tables[table])
    if expected == actual:
        ok(f"{table}: {actual:,} rows == source")
    else:
        fail(f"{table}: {actual:,} rows != source {expected:,}")

TRANSPARENCY_SOURCES = {
    "disclosure_receipt": [("AllAnnualData", "Detailed Receipts.csv")],
    "disclosure_election_return": [
        ("AllElectionsData", "Senate Groups and Candidate Return Summary.csv")
    ],
    "disclosure_donation": [(b, f) for b, f, *_ in utils._DONATION_SOURCES],
    "disclosure_return_annual": [
        ("AllAnnualData", f) for f, *_ in utils._ANNUAL_RETURN_SOURCES
    ],
}
for table, srcs in TRANSPARENCY_SOURCES.items():
    expected = source_rows(
        [ROOT / "input" / "transparency" / b / f for b, f in srcs],
        preamble=False,
    )
    actual = len(tables[table])
    if expected == actual:
        ok(f"{table}: {actual:,} rows == source")
    else:
        fail(f"{table}: {actual:,} rows != source {expected:,}")

# division_summary is a merge, so its expected size is one row per division per chamber.
expected_summary = 0
for ev, d in event_dirs():
    for stem in (
        "HouseTurnoutByDivisionDownload",
        "SenateTurnoutByDivisionDownload",
        "ReferendumTurnoutByDivisionDownload",
    ):
        p = d / f"{stem}-{ev}.csv"
        if p.exists():
            expected_summary += len(utils.read_aec_csv(p))
actual = len(tables["division_summary"])
if expected_summary == actual:
    ok(f"division_summary: {actual:,} rows == source turnout files")
else:
    fail(f"division_summary: {actual:,} rows != source {expected_summary:,}")


print("\n=== 2. Key columns are populated ===")

KEY_COLUMNS = {
    "election": ["year", "election_id", "election_name", "election_type"],
    "polling_place": [
        "year",
        "election_id",
        "division_id",
        "polling_place_id",
    ],
    "party": ["year", "election_id", "party_abbreviation"],
    "house_candidate": ["year", "election_id", "division_id", "candidate_id"],
    "house_first_preference_division": [
        "year",
        "election_id",
        "division_id",
        "candidate_id",
    ],
    "house_first_preference_polling_place": [
        "year",
        "election_id",
        "division_id",
        "polling_place_id",
        "candidate_id",
    ],
    "house_two_candidate_preferred_polling_place": [
        "year",
        "election_id",
        "division_id",
        "polling_place_id",
        "candidate_id",
    ],
    "house_two_party_preferred_division": [
        "year",
        "election_id",
        "division_id",
    ],
    "house_two_party_preferred_polling_place": [
        "year",
        "election_id",
        "division_id",
        "polling_place_id",
    ],
    "senate_candidate": [
        "year",
        "election_id",
        "state_abbreviation",
        "candidate_id",
    ],
    "senate_first_preference_division": [
        "year",
        "election_id",
        "division_id",
        "candidate_id",
    ],
    "division_summary": ["year", "election_id", "chamber", "division_id"],
    "referendum_polling_place": [
        "year",
        "election_id",
        "division_id",
        "polling_place_id",
        "question_number",
    ],
    "disclosure_donation": ["year", "disclosure_type", "direction", "value"],
    "disclosure_receipt": [
        "year",
        "financial_year",
        "recipient_name",
        "value",
    ],
    "disclosure_return_annual": [
        "year",
        "financial_year",
        "return_type",
        "name",
    ],
    "disclosure_election_return": [
        "year",
        "election_name",
        "return_type",
        "name",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave", "valor"],
}
for table, cols in KEY_COLUMNS.items():
    df = tables[table]
    if df.empty:
        fail(f"{table}: empty")
        continue
    for col in cols:
        n_null = df[col].isna().sum()
        if n_null:
            fail(f"{table}.{col}: {n_null:,} NULL of {len(df):,}")
    else:
        ok(f"{table}: key columns populated")


print("\n=== 3. Staging parquet is all-STRING and NULL-safe ===")

for table in constants.TABLES.value:
    path = OUTPUT / table
    files = sorted(path.rglob("*.parquet"))
    if not files:
        fail(f"{table}: no parquet written")
        continue
    arrow_schema = pq.read_schema(files[0])
    non_string = [f.name for f in arrow_schema if str(f.type) != "string"]
    if non_string:
        fail(f"{table}: non-STRING columns in staging parquet {non_string}")
        continue
    df = tables[table]
    polluted = [
        c for c in df.columns if (df[c].astype("string") == "nan").any()
    ]
    if polluted:
        fail(f"{table}: literal 'nan' written in {polluted}")
        continue
    ok(f"{table}: all-STRING, no 'nan' literals")


print("\n=== 4. Substance ===")


def check(label: str, actual, expected) -> None:
    if actual == expected:
        ok(f"{label}: {actual}")
    else:
        fail(f"{label}: got {actual}, expected {expected}")


tpp = tables["house_two_party_preferred_division"]
check(
    "2025 House divisions with a TPP result",
    len(tpp[tpp["year"] == "2025"]),
    150,
)
check(
    "2022 House divisions with a TPP result",
    len(tpp[tpp["year"] == "2022"]),
    151,
)

members = tables["house_candidate"]
elected_2025 = members[
    (members["year"] == "2025") & (members["elected"] == "Y")
]
check("2025 House members elected", len(elected_2025), 150)

sen = tables["senate_candidate"]
elected_sen_2025 = sen[(sen["year"] == "2025") & (sen["elected"] == "Y")]
check("2025 senators elected", len(elected_sen_2025), 40)

elections = tables["election"]
check("events catalogued", len(elections), 34)
check(
    "federal elections catalogued",
    int((elections["election_type"] == "federal_election").sum()),
    8,
)

# The 2023 referendum failed nationally and in every state.
ref = tables["referendum_polling_place"]
yes = pd.to_numeric(ref["yes_votes"], errors="coerce").sum()
no = pd.to_numeric(ref["no_votes"], errors="coerce").sum()
if no > yes:
    ok(
        f"2023 referendum rejected: yes={yes:,.0f} no={no:,.0f} "
        f"({100 * yes / (yes + no):.2f}% yes)"
    )
else:
    fail(f"2023 referendum: yes={yes:,.0f} no={no:,.0f} — expected No to lead")

# The two-party-preferred split must sum to the reported total, division by division.
tpp_num = tpp.assign(
    lab=pd.to_numeric(tpp["labor_votes"], errors="coerce"),
    coa=pd.to_numeric(tpp["coalition_votes"], errors="coerce"),
    tot=pd.to_numeric(tpp["total_votes"], errors="coerce"),
)
mismatch = tpp_num[
    (tpp_num["lab"] + tpp_num["coa"] - tpp_num["tot"]).abs() > 0
]
if mismatch.empty:
    ok(f"TPP labor + coalition == total in all {len(tpp_num):,} division rows")
else:
    fail(f"TPP votes do not sum to total in {len(mismatch):,} rows")

# Party labels must not have been swapped by the column-order flip across vintages.
alp_2025 = tpp_num[(tpp_num["year"] == "2025")]["lab"].sum()
lnc_2025 = tpp_num[(tpp_num["year"] == "2025")]["coa"].sum()
if alp_2025 > lnc_2025:
    ok(
        f"2025 national TPP: ALP {100 * alp_2025 / (alp_2025 + lnc_2025):.2f}% "
        f"(Labor won 2025)"
    )
else:
    fail("2025 national TPP has the Coalition ahead — columns may be swapped")

alp_2013 = tpp_num[(tpp_num["year"] == "2013")]["lab"].sum()
lnc_2013 = tpp_num[(tpp_num["year"] == "2013")]["coa"].sum()
if lnc_2013 > alp_2013:
    ok(
        f"2013 national TPP: Coalition {100 * lnc_2013 / (alp_2013 + lnc_2013):.2f}% "
        f"(Coalition won 2013)"
    )
else:
    fail("2013 national TPP has Labor ahead — columns may be swapped")

# Donations must not be double counted: the two directions are distinct reports.
don = tables["disclosure_donation"]
print(
    f"  info donations by direction: "
    f"{don['direction'].value_counts().to_dict()}"
)
print(
    f"  info donations by disclosure_type: "
    f"{don['disclosure_type'].value_counts().to_dict()}"
)

# Coverage
for table in (
    "house_first_preference_polling_place",
    "disclosure_receipt",
    "disclosure_donation",
):
    yrs = pd.to_numeric(tables[table]["year"], errors="coerce").dropna()
    print(f"  info {table}: {int(yrs.min())}-{int(yrs.max())}")


print("\n" + "=" * 70)
if failures:
    print(f"{len(failures)} CHECK(S) FAILED")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("ALL CHECKS PASSED")
