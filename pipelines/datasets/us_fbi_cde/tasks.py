"""Prefect tasks for us_fbi_cde.

Thin wrappers over the pure functions in :mod:`utils`. The transform itself
lives there so the one-shot onboarding scripts and this flow share it.
"""

from __future__ import annotations

import json
import shutil
import urllib.request
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.us_fbi_cde.constants import constants
from pipelines.datasets.us_fbi_cde.utils import (
    clean_nibrs_bundle,
    download_key,
    parse_reta_file,
    read_csv_all_strings,
    signed_url,
    write_partition,
)

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


@task(name="us_fbi_cde: latest data year")
def discover_latest_year() -> int:
    """Read the data year of the most recent NIBRS release from the CDE catalogue.

    The catalogue the download page itself consumes is the only machine-readable
    statement of what has been published; probing object keys would confuse "not
    yet released" with "temporarily unavailable".
    """
    with urllib.request.urlopen(
        constants.MASTER_FILE_CATALOGUE.value, timeout=60
    ) as r:
        catalogue = json.load(r)
    for entry in catalogue:
        if entry.get("id") == "nibrs":
            return int(entry["maxYear"])
    raise ValueError("the CDE master-file catalogue has no NIBRS entry")


@task(name="us_fbi_cde: download refresh window")
def download_window(
    work_dir: str, latest_year: int, years_back: int = 2
) -> dict:
    """Download the state-year bundles, Return A years and full-history extracts.

    The FBI revises the two data years preceding a release, so the window covers
    them as well as the new year. The employee, hate crime and participation
    extracts ship the full history in one file each and are always re-fetched.
    """
    work = Path(work_dir)
    nibrs_dir = work / "input" / "nibrs"
    reta_dir = work / "input" / "reta"
    flat_dir = work / "input"
    years = list(range(latest_year - years_back, latest_year + 1))

    bundles = []
    for year in years:
        for state in constants.STATES.value:
            key = constants.NIBRS_INCIDENT_KEY.value.format(
                year=year, state=state
            )
            if not signed_url(key):
                continue
            path = download_key(key, nibrs_dir)
            if path:
                bundles.append((str(path), state, year))
    print(f"downloaded {len(bundles)} NIBRS bundles for {years}")

    reta = []
    for year in years:
        if year < constants.RETA_FIRST_YEAR.value:
            continue
        key = constants.RETA_KEY.value.format(year=year)
        if not signed_url(key):
            continue
        path = download_key(key, reta_dir)
        if path:
            reta.append((str(path), year))
    print(f"downloaded {len(reta)} Return A files")

    flat = {}
    for name, key in [
        ("lee", constants.LEE_KEY.value),
        ("hate_crime", constants.HATE_CRIME_KEY.value),
    ]:
        path = download_key(key, flat_dir)
        if path is None:
            raise RuntimeError(f"could not download {key}")
        flat[name] = str(path)

    return {"bundles": bundles, "reta": reta, "flat": flat, "years": years}


@task(name="us_fbi_cde: clean refresh window")
def clean_window(work_dir: str, downloads: dict) -> dict:
    """Clean the window into partitioned parquet and return one path per table."""
    work = Path(work_dir)
    output = work / "output"
    years = downloads["years"]
    participation, attributes, reta_agency = [], [], []

    for zip_path, state, year in downloads["bundles"]:
        tables = clean_nibrs_bundle(zip_path, state, year)
        for table in NIBRS_TABLES:
            write_partition(
                tables[table],
                table,
                output,
                {"year": str(year), "state_abbr": state},
            )
        participation.append(tables["_participation"])
        attributes.append(tables["_agency_attributes"])

    for zip_path, year in downloads["reta"]:
        summary, agency = parse_reta_file(zip_path, year)
        if summary.empty:
            continue
        for column in [
            "actual_count",
            "unfounded_count",
            "cleared_count",
            "juvenile_cleared_count",
        ]:
            summary[column] = summary[column].map(
                lambda v: None if pd.isna(v) else str(int(v))
            )
        # A modern ORI is the seven-character NCIC ORI plus a two-digit
        # sub-unit suffix, and a Return A record is filed by the parent agency.
        # The NIBRS bundles' own legacy_ori column is a nine-character alternate
        # ORI, not the short form, so it is no use as a crosswalk.
        summary["ori"] = summary["legacy_ori"].map(
            lambda v: f"{v}00" if isinstance(v, str) and v else None
        )
        write_partition(summary, "ucr_summary", output, {"year": str(year)})
        reta_agency.append(agency)

    _write_agency(
        output,
        downloads["flat"]["lee"],
        years,
        participation,
        attributes,
        reta_agency,
    )
    _write_hate_crime(output, downloads["flat"]["hate_crime"], years)
    _copy_dicionario(output)

    paths = {}
    for table in [
        *NIBRS_TABLES,
        "ucr_summary",
        "agency",
        "hate_crime",
        "dicionario",
    ]:
        directory = output / table
        if directory.exists():
            paths[table] = str(directory)
    return paths


def _write_agency(
    output, lee_path, years, participation, attributes, reta_agency
):
    """Rebuild only the agency-year partitions inside the refresh window."""
    from pipelines.datasets.us_fbi_cde.spec import column_names

    lee = read_csv_all_strings(lee_path)
    lee.columns = [c.strip().lower() for c in lee.columns]
    lee = lee.rename(columns={"data_year": "year"})
    lee = lee[lee["year"].isin({str(y) for y in years})]

    frames = [f for f in participation if f is not None and not f.empty]
    part = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=["year", "ori", "nibrs_months_reported"])
    )
    frames = [f for f in attributes if f is not None and not f.empty]
    attrs = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(
            columns=[
                "year",
                "ori",
                "legacy_ori",
                "nibrs_start_date",
                "nibrs_participated",
            ]
        )
    )
    frames = [f for f in reta_agency if f is not None and not f.empty]
    reta = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    agency = lee.rename(
        columns={
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
    agency = agency.merge(
        part.drop_duplicates(["year", "ori"]), on=["year", "ori"], how="left"
    )
    agency = agency.merge(
        attrs.drop_duplicates(["year", "ori"])[
            ["year", "ori", "nibrs_start_date", "nibrs_participated"]
        ],
        on=["year", "ori"],
        how="left",
    )
    if not reta.empty:
        reta["ori"] = reta["legacy_ori"].map(
            lambda v: f"{v}00" if isinstance(v, str) else None
        )
        agency = agency.merge(
            reta.drop(columns=["state_abbr", "legacy_ori"]).drop_duplicates(
                ["year", "ori"]
            ),
            on=["year", "ori"],
            how="left",
        )
    for name in column_names("agency"):
        if name not in agency.columns:
            agency[name] = pd.NA
    for year, group in agency.groupby("year"):
        write_partition(group, "agency", output, {"year": str(year)})


def _write_hate_crime(output, path, years):
    import io
    import zipfile

    if str(path).endswith(".zip"):
        with zipfile.ZipFile(path) as zf:
            member = next(
                name for name in zf.namelist() if name.lower().endswith(".csv")
            )
            with zf.open(member) as handle:
                frame = read_csv_all_strings(
                    io.TextIOWrapper(handle, encoding="latin-1", newline="")
                )
    else:
        frame = read_csv_all_strings(path)
    frame.columns = [c.strip().lower() for c in frame.columns]
    frame = frame.rename(
        columns={
            "data_year": "year",
            "pug_agency_name": "agency_name",
            "pub_agency_unit": "agency_unit",
            "agency_type_name": "agency_type",
            "bias_desc": "bias_description",
            "total_individual_victims": "individual_victim_count",
            "total_offender_count": "offender_count",
            "multiple_offense": "multiple_offense_flag",
            "multiple_bias": "multiple_bias_flag",
        }
    )
    wanted = {str(y) for y in years}
    for year, group in frame[frame["year"].isin(wanted)].groupby("year"):
        write_partition(group, "hate_crime", output, {"year": str(year)})


def _copy_dicionario(output):
    """Copy the committed dictionary CSV into the output tree as parquet."""
    source = Path(constants.ARCHITECTURE_DIR.value).parent / "dicionario.csv"
    frame = read_csv_all_strings(source)
    directory = Path(output) / "dicionario"
    directory.mkdir(parents=True, exist_ok=True)
    write_partition(frame, "dicionario", output, {})
    return str(directory)


@task(name="us_fbi_cde: clean work directory")
def cleanup(work_dir: str) -> None:
    shutil.rmtree(work_dir, ignore_errors=True)
