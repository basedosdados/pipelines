"""One-shot bootstrap: clean every FBI CDE source into partitioned parquet.

This is the onboarding run. It imports the transform from
``pipelines/datasets/us_fbi_cde/utils.py`` rather than re-implementing it, so
the recurring Prefect flow and this script cannot drift.

Order of work:

1. every NIBRS state-year bundle -> the eight incident-level tables, and the
   agency-year participation rows the agency table needs;
2. every Return A year -> ``ucr_summary``, and the agency-year header rows;
3. the law enforcement employee extract, joined to 1 and 2 -> ``agency``;
4. the hate crime extract -> ``hate_crime``.

Run with ``--limit`` to clean a sample first; the full run writes roughly
1.3 billion rows.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.constants import constants
from pipelines.datasets.us_fbi_cde.utils import (
    clean_nibrs_bundle,
    parse_reta_file,
    read_csv_all_strings,
    write_partition,
)

DATA_ROOT = constants.DATA_ROOT.value
INPUT = DATA_ROOT / "input"
OUTPUT = DATA_ROOT / "output"
SIDECAR = DATA_ROOT / "sidecar"

NIBRS_TABLES = [
    "incident",
    "offense",
    "offender",
    "victim",
    "victim_offense",
    "victim_offender_relationship",
    "arrestee",
    "property",
]


# --------------------------------------------------------------------------
# Pass 1 - NIBRS bundles
# --------------------------------------------------------------------------


def clean_one_bundle(args):
    """Clean one state-year bundle and write its partition of every table."""
    zip_path, state_abbr, year = args
    tables = clean_nibrs_bundle(zip_path, state_abbr, year)
    counts = {}
    for table in NIBRS_TABLES:
        frame = tables[table]
        counts[table] = len(frame)
        write_partition(
            frame,
            table,
            OUTPUT,
            {"year": str(year), "state_abbr": state_abbr},
        )
    sidecar = SIDECAR / "nibrs"
    sidecar.mkdir(parents=True, exist_ok=True)
    tag = f"{state_abbr}-{year}"
    tables["_participation"].to_parquet(
        sidecar / f"participation_{tag}.parquet", index=False
    )
    tables["_agency_attributes"].to_parquet(
        sidecar / f"attributes_{tag}.parquet", index=False
    )
    return tag, counts


def pass_nibrs(limit=None, workers=4):
    bundles = sorted((INPUT / "nibrs").glob("*.zip"))
    if limit:
        bundles = bundles[:limit]
    jobs = []
    for path in bundles:
        state_abbr, year = path.stem.split("-")
        jobs.append((str(path), state_abbr, int(year)))
    totals = Counter()
    done = 0
    started = time.time()
    with ProcessPoolExecutor(workers) as pool:
        futures = {pool.submit(clean_one_bundle, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                _tag, counts = future.result()
            except Exception as error:
                print(f"FAILED {job[1]}-{job[2]}: {error}", flush=True)
                continue
            totals.update(counts)
            done += 1
            if done % 25 == 0:
                elapsed = time.time() - started
                print(
                    f"  {done}/{len(jobs)} bundles  {elapsed / 60:.1f} min  "
                    f"rows so far {sum(totals.values()):,}",
                    flush=True,
                )
    print("NIBRS row counts:")
    for table in NIBRS_TABLES:
        print(f"  {table:32s} {totals[table]:>14,}")
    return totals


# --------------------------------------------------------------------------
# Pass 2 - Return A
# --------------------------------------------------------------------------


def clean_one_reta(args):
    """Parse one Return A year and write its ucr_summary partition."""
    zip_path, year = args
    summary, agency = parse_reta_file(zip_path, year)
    if summary.empty:
        return year, 0
    for column in [
        "actual_count",
        "unfounded_count",
        "cleared_count",
        "juvenile_cleared_count",
    ]:
        summary[column] = summary[column].map(
            lambda v: None if pd.isna(v) else str(int(v))
        )
    sidecar = SIDECAR / "reta"
    sidecar.mkdir(parents=True, exist_ok=True)
    agency.to_parquet(sidecar / f"agency_{year}.parquet", index=False)
    # ori is filled in later from the legacy crosswalk; write it now so the
    # partition has every declared column.
    summary["ori"] = pd.NA
    write_partition(summary, "ucr_summary", OUTPUT, {"year": str(year)})
    return year, len(summary)


def pass_reta(limit=None, workers=4):
    files = sorted((INPUT / "reta").glob("reta-[0-9]*.zip"))
    if limit:
        files = files[:limit]
    jobs = [(str(path), int(path.stem.split("-")[1])) for path in files]
    total = 0
    with ProcessPoolExecutor(workers) as pool:
        for year, rows in pool.map(clean_one_reta, jobs):
            total += rows
            print(f"  reta {year}: {rows:,} rows", flush=True)
    print(f"ucr_summary rows: {total:,}")
    return total


# --------------------------------------------------------------------------
# Pass 3 - agency
# --------------------------------------------------------------------------


def county_crosswalk():
    """Map (state abbreviation, upper-case county name) -> five-digit FIPS."""
    path = INPUT / "national_county2020.txt"
    if not path.exists():
        urllib.request.urlretrieve(constants.CENSUS_COUNTY_ANSI.value, path)
    frame = read_csv_all_strings(path, sep="|")
    frame.columns = [c.strip().upper() for c in frame.columns]
    state_col = "STATE" if "STATE" in frame.columns else "STATE_ABBR"
    name_col = "COUNTYNAME" if "COUNTYNAME" in frame.columns else "COUNTY_NAME"
    frame["fips"] = frame["STATEFP"].str.zfill(2) + frame[
        "COUNTYFP"
    ].str.zfill(3)
    # Strip the "County"/"Parish"/"Borough" suffix: the CDE publishes bare names.
    bare = (
        frame[name_col]
        .str.upper()
        .str.replace(
            r"\s+(COUNTY|PARISH|BOROUGH|CENSUS AREA|CITY AND BOROUGH|MUNICIPALITY|"
            r"CITY|PLANNING REGION)$",
            "",
            regex=True,
        )
        .str.strip()
    )
    lookup = {}
    ambiguous = set()
    for state, name, fips in zip(
        frame[state_col], bare, frame["fips"], strict=False
    ):
        key = (state, name)
        if key in lookup and lookup[key] != fips:
            ambiguous.add(key)
        lookup[key] = fips
    for key in ambiguous:
        lookup.pop(key, None)
    states = dict(
        zip(frame[state_col], frame["STATEFP"].str.zfill(2), strict=False)
    )
    return lookup, states


def legacy_ori_crosswalk():
    """Map the seven-character legacy ORI to the nine-character modern one."""
    frames = []
    for path in sorted((SIDECAR / "nibrs").glob("attributes_*.parquet")):
        frames.append(pd.read_parquet(path, columns=["ori", "legacy_ori"]))
    if not frames:
        return {}, 0
    combined = pd.concat(frames, ignore_index=True).dropna()
    combined = combined[combined["legacy_ori"].str.len() == 7]
    combined = combined.drop_duplicates(subset=["legacy_ori"])
    crosswalk = dict(
        zip(combined["legacy_ori"], combined["ori"], strict=False)
    )
    matches_rule = sum(1 for k, v in crosswalk.items() if v == k + "00")
    return crosswalk, matches_rule


def pass_agency():
    lee = read_csv_all_strings(INPUT / "lee_1960_2025.csv")
    lee.columns = [c.strip().lower() for c in lee.columns]

    participation = (
        pd.concat(
            [
                pd.read_parquet(p)
                for p in sorted(
                    (SIDECAR / "nibrs").glob("participation_*.parquet")
                )
            ],
            ignore_index=True,
        )
        if list((SIDECAR / "nibrs").glob("participation_*.parquet"))
        else pd.DataFrame(columns=["year", "ori", "nibrs_months_reported"])
    )
    participation = participation.drop_duplicates(subset=["year", "ori"])

    attributes = (
        pd.concat(
            [
                pd.read_parquet(p)
                for p in sorted(
                    (SIDECAR / "nibrs").glob("attributes_*.parquet")
                )
            ],
            ignore_index=True,
        )
        if list((SIDECAR / "nibrs").glob("attributes_*.parquet"))
        else pd.DataFrame(
            columns=[
                "year",
                "ori",
                "legacy_ori",
                "nibrs_start_date",
                "nibrs_participated",
                "state_abbr",
            ]
        )
    )
    attributes = attributes.drop_duplicates(subset=["year", "ori"])

    reta_agency = (
        pd.concat(
            [
                pd.read_parquet(p)
                for p in sorted((SIDECAR / "reta").glob("agency_*.parquet"))
            ],
            ignore_index=True,
        )
        if list((SIDECAR / "reta").glob("agency_*.parquet"))
        else pd.DataFrame(
            columns=[
                "year",
                "state_abbr",
                "legacy_ori",
                "core_city_flag",
                "covered_by_ori",
                "summary_months_reported",
                "officer_killed_felonious_count",
                "officer_killed_accidental_count",
                "officer_assaulted_count",
            ]
        )
    )

    crosswalk, rule_matches = legacy_ori_crosswalk()
    print(
        f"legacy ORI crosswalk: {len(crosswalk):,} agencies, "
        f"{rule_matches:,} ({rule_matches / max(len(crosswalk), 1):.1%}) follow the "
        f"legacy+'00' rule"
    )
    if not reta_agency.empty:
        reta_agency["ori"] = reta_agency["legacy_ori"].map(
            lambda v: (
                crosswalk.get(v, f"{v}00") if isinstance(v, str) else None
            )
        )
        reta_agency = reta_agency.drop_duplicates(subset=["year", "ori"])

    base = lee.rename(
        columns={
            "data_year": "year",
            "pub_agency_name": "agency_name",
            "pub_agency_unit": "agency_unit",
            "agency_type_name": "agency_type",
            "population_group_desc": "population_group_description",
            "officer_ct": "officer_count",
            "civilian_ct": "civilian_count",
            "total_pe_ct": "employee_count",
            "male_officer_ct": "male_officer_count",
            "male_cilvilian_ct": "male_civilian_count",
            "female_officer_ct": "female_officer_count",
            "female_cilvilian_ct": "female_civilian_count",
            "pe_ct_per_1000": "employee_per_1000_inhabitants",
        }
    )
    base = base.drop(
        columns=["male_total_ct", "female_total_ct"], errors="ignore"
    )

    # Agencies that filed a Return A or reported to NIBRS but never filed an
    # employee return are absent from the employee extract; add them so the
    # coverage fields exist for every reporting agency.
    extra_keys = pd.concat(
        [
            reta_agency[["year", "ori", "state_abbr"]]
            if not reta_agency.empty
            else None,
            attributes[["year", "ori", "state_abbr"]]
            if not attributes.empty
            else None,
        ],
        ignore_index=True,
    ).dropna(subset=["ori"])
    known = set(zip(base["year"], base["ori"], strict=False))
    extra = extra_keys[
        ~pd.Series(
            list(zip(extra_keys["year"], extra_keys["ori"], strict=False)),
            index=extra_keys.index,
        ).isin(known)
    ].drop_duplicates(subset=["year", "ori"])
    print(f"agencies present only in Return A / NIBRS: {len(extra):,}")
    agency = pd.concat([base, extra], ignore_index=True)

    agency = agency.merge(participation, on=["year", "ori"], how="left")
    agency = agency.merge(
        attributes[
            [
                "year",
                "ori",
                "legacy_ori",
                "nibrs_start_date",
                "nibrs_participated",
            ]
        ],
        on=["year", "ori"],
        how="left",
    )
    if not reta_agency.empty:
        agency = agency.merge(
            reta_agency.drop(columns=["state_abbr", "legacy_ori"]),
            on=["year", "ori"],
            how="left",
        )

    lookup, state_fips = county_crosswalk()
    agency["state_id"] = agency["state_abbr"].map(state_fips)
    names = agency["county_name"].fillna("").str.upper().str.strip()
    agency["county_id"] = [
        lookup.get((state, name))
        for state, name in zip(agency["state_abbr"], names, strict=False)
    ]
    matched = agency["county_id"].notna().sum()
    with_name = (names != "").sum()
    print(
        f"county FIPS matched for {matched:,} of {with_name:,} agency-years with a "
        f"county name ({matched / max(with_name, 1):.1%})"
    )

    agency["nibrs_participated"] = agency["nibrs_participated"].fillna(
        agency["nibrs_months_reported"].notna().map({True: "Y", False: pd.NA})
    )

    for year, group in agency.groupby("year"):
        if not str(year).isdigit():
            continue
        write_partition(group, "agency", OUTPUT, {"year": str(year)})
    print(f"agency rows: {len(agency):,}")
    return len(agency)


# --------------------------------------------------------------------------
# Pass 4 - hate crime
# --------------------------------------------------------------------------


def pass_hate_crime():
    path = INPUT / "extracted_hate_crime" / "hate_crime.csv"
    frame = read_csv_all_strings(path)
    frame.columns = [c.strip().lower() for c in frame.columns]
    frame = frame.replace("NULL", pd.NA)
    frame = frame.rename(
        columns={
            "data_year": "year",
            "pug_agency_name": "agency_name",
            "pub_agency_unit": "agency_unit",
            "agency_type_name": "agency_type",
            "population_group_description": "population_group_desc",
            "bias_desc": "bias_description",
            "total_individual_victims": "individual_victim_count",
            "total_offender_count": "offender_count",
            "multiple_offense": "multiple_offense_flag",
            "multiple_bias": "multiple_bias_flag",
        }
    )
    for year, group in frame.groupby("year"):
        write_partition(group, "hate_crime", OUTPUT, {"year": str(year)})
    print(f"hate_crime rows: {len(frame):,}")
    return len(frame)


# --------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--steps", default="nibrs,reta,agency,hate_crime")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    OUTPUT.mkdir(parents=True, exist_ok=True)
    SIDECAR.mkdir(parents=True, exist_ok=True)
    steps = args.steps.split(",")
    report = {}
    if "nibrs" in steps:
        print("== NIBRS bundles")
        report["nibrs"] = dict(pass_nibrs(args.limit, args.workers))
    if "reta" in steps:
        print("== Return A")
        report["ucr_summary"] = pass_reta(args.limit, args.workers)
    if "agency" in steps:
        print("== agency")
        report["agency"] = pass_agency()
    if "hate_crime" in steps:
        print("== hate crime")
        report["hate_crime"] = pass_hate_crime()
    (DATA_ROOT / "clean_report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
