"""Pure download and cleaning functions for us_fbi_cde.

No Prefect imports live here. The one-shot onboarding scripts under
``models/us_fbi_cde/code/`` and the recurring flow in ``flows.py`` both import
these functions, so the transform has exactly one home.

Two source shapes are handled:

* **NIBRS relational bundles** — one zip per state per year holding ~43
  normalised CSVs. Their filenames are lowercase and their columns differ before
  and after the 2019/2020 redesign (``cde_agencies.csv`` vs ``agencies.csv``,
  ``offense_type_id`` vs ``offense_code``, no ``data_year`` at all in the early
  years). :func:`clean_nibrs_bundle` normalises both shapes to one schema.
* **Return A summary master files** — one fixed-width flat file per year,
  LRECL 7385, whose layout comes from the FBI's own record description: a
  305-character header followed by twelve 590-character month blocks.
"""

from __future__ import annotations

import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SIGNED_URL_ENDPOINT = "https://cde.ucr.cjis.gov/LATEST/s3/signedurl"

# The NIBRS bundles are named with postal codes, but the FBI's own employee and
# hate crime extracts, its ORIs and the Return A state table all use the UCR
# code. Nebraska is the only one that differs, and left alone it would be "NE"
# in the incident tables and "NB" everywhere else in the same dataset.
UCR_STATE_ABBR = {"NE": "NB"}


def canonical_state(state_abbr):
    """Map a bundle's filename state to the code the rest of the dataset uses.

    Call this once, at the point the bundle is scheduled, so the same value
    reaches both the row contents and the partition path. Mapping it only inside
    the cleaner leaves the caller writing `state_abbr=NE/` directories whose rows
    say `NB` — the partition key wins, and the state ends up mislabelled.
    """
    return UCR_STATE_ABBR.get(state_abbr, state_abbr)


# The FBI is inconsistent about missing values: the NIBRS bundles leave the
# field empty, while the law enforcement employee extract writes the literal
# string "NULL" — in 750,787 of its 785,127 pub_agency_unit values. Read as
# text, that string would ship as a real value.
NA_VALUES = ["", "NULL"]

# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def signed_url(key, attempts=5):
    """Ask the CDE for a presigned S3 URL for ``key``.

    Returns ``None`` when the object does not exist — the endpoint simply omits
    the key from its response, which makes this double as an existence check.
    The URL it hands back expires after 900 seconds, so it is requested per
    object rather than cached.
    """
    url = f"{SIGNED_URL_ENDPOINT}?key={urllib.parse.quote(key)}"
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=90) as response:
                return json.load(response).get(key)
        except Exception:
            time.sleep(2 * (attempt + 1))
    return None


def download_key(key, dest_dir, expected_size=None, attempts=4):
    """Download one CDE object into ``dest_dir``, resuming if already complete."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / os.path.basename(key)
    if out.exists() and (
        expected_size is None or out.stat().st_size == expected_size
    ):
        return out
    for attempt in range(attempts):
        url = signed_url(key)
        if not url:
            return None
        part = out.with_suffix(out.suffix + ".part")
        try:
            urllib.request.urlretrieve(url, part)
            if (
                expected_size is not None
                and part.stat().st_size != expected_size
            ):
                part.unlink()
                continue
            part.replace(out)
            return out
        except Exception:
            time.sleep(3 * (attempt + 1))
    return None


def available_nibrs_bundles(states, years):
    """Return ``{key: None}`` for every state-year bundle the CDE actually has."""
    keys = [
        f"nibrs/incident/{year}/{state}-{year}.zip"
        for year in years
        for state in states
    ]
    return [key for key in keys if signed_url(key)]


# --------------------------------------------------------------------------
# NIBRS relational bundles
# --------------------------------------------------------------------------

# Columns the FBI uses for its own ingest plumbing. They carry no analytical
# content, differ between eras, and one of them (incident_number) is the
# agency's own case number.
DROP_COLUMNS = {
    "ff_line_number",
    "did",
    "ddocname",
    "data_home",
    "orig_format",
    "incident_number",
    "arrest_num",
    "month_pub_status",
    "update_flag",
    "prepared_date",
    "outside_agency_id",
}

# Lookup tables shipped inside every bundle, mapping the bundle-local surrogate
# id to the FBI's published code. ``{logical file: (id column, code column)}``.
CODE_LOOKUPS = {
    "nibrs_age": ("age_id", "age_code"),
    "nibrs_arrest_type": ("arrest_type_id", "arrest_type_code"),
    "nibrs_assignment_type": ("assignment_type_id", "assignment_type_code"),
    "nibrs_activity_type": ("activity_type_id", "activity_type_code"),
    "nibrs_bias_list": ("bias_id", "bias_code"),
    "nibrs_cleared_except": ("cleared_except_id", "cleared_except_code"),
    "nibrs_drug_measure_type": ("drug_measure_type_id", "drug_measure_code"),
    "nibrs_ethnicity": ("ethnicity_id", "ethnicity_code"),
    "nibrs_injury": ("injury_id", "injury_code"),
    "nibrs_location_type": ("location_id", "location_code"),
    "nibrs_prop_desc_type": ("prop_desc_id", "prop_desc_code"),
    # The property loss lookup ships no separate code column because its
    # surrogate id already is the published NIBRS code (values 1 to 8), so this
    # one resolves to itself.
    "nibrs_prop_loss_type": ("prop_loss_id", "prop_loss_id"),
    "nibrs_relationship": ("relationship_id", "relationship_code"),
    "nibrs_suspected_drug_type": (
        "suspected_drug_type_id",
        "suspected_drug_code",
    ),
    "nibrs_victim_type": ("victim_type_id", "victim_type_code"),
    "nibrs_weapon_type": ("weapon_id", "weapon_code"),
    "ref_race": ("race_id", "race_code"),
}


def _members(zf):
    """Map lowercase basename without extension -> member path."""
    out = {}
    for name in zf.namelist():
        base = os.path.basename(name).lower()
        if base.endswith(".csv"):
            out[base[:-4]] = name
    return out


def _read(zf, members, logical, usecols=None):
    """Read one CSV out of the bundle as all-string columns, or ``None``."""
    member = members.get(logical)
    if member is None:
        return None
    with zf.open(member) as handle:
        text = io.TextIOWrapper(handle, encoding="latin-1", newline="")
        frame = pd.read_csv(
            text,
            dtype=str,
            keep_default_na=False,
            na_values=NA_VALUES,
            low_memory=False,
        )
    frame.columns = [c.strip().lower() for c in frame.columns]
    frame = frame.loc[:, ~frame.columns.duplicated()]
    frame = frame.drop(
        columns=[c for c in frame.columns if c in DROP_COLUMNS],
        errors="ignore",
    )
    if usecols is not None:
        for col in usecols:
            if col not in frame.columns:
                frame[col] = pd.NA
        frame = frame[list(usecols)]
    return frame


def _first_column(frame, *candidates):
    """Return the first candidate column present, else a same-length NA Series.

    Column availability varies between the pre-2020 and post-2020 bundle
    layouts, so every read of an optional column goes through here rather than
    branching on the era.
    """
    if frame is None:
        return pd.Series(dtype="object")
    for name in candidates:
        if name in frame.columns:
            return frame[name]
    return pd.Series(pd.NA, index=frame.index, dtype="object")


MONTHS = {
    "JAN": "01",
    "FEB": "02",
    "MAR": "03",
    "APR": "04",
    "MAY": "05",
    "JUN": "06",
    "JUL": "07",
    "AUG": "08",
    "SEP": "09",
    "OCT": "10",
    "NOV": "11",
    "DEC": "12",
}
_ORACLE_DATE = re.compile(r"^(\d{2})-([A-Z]{3})-(\d{2})$")


def _as_date(series, data_year=None):
    """Normalise a source date to ``YYYY-MM-DD``.

    Three shapes appear across the bundles and all three must be handled:

    * ``2023-01-01`` and ``2023-05-31 13:49:51.367`` — ISO, with or without a
      time part;
    * ``09-AUG-15`` — Oracle's ``DD-MON-YY``, used by 212 of the 1,066 bundles
      across 48 states. The century is resolved against the bundle's own data
      year rather than a fixed pivot, so an incident dated in an adjacent year
      still lands in the right century.

    BigQuery's ``SAFE_CAST(... AS DATE)`` accepts only the bare ISO form and
    returns NULL for everything else, so an unhandled shape would empty the
    column silently rather than fail.
    """
    if series is None or len(series) == 0:
        return series
    text = series.astype("object").str.strip()
    iso = text.str.slice(0, 10)
    iso = iso.where(iso.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False))

    oracle = text.where(text.str.match(r"^\d{2}-[A-Z]{3}-\d{2}$", na=False))
    if oracle.notna().any():
        century = 1900 if data_year is None else (data_year // 100) * 100

        def convert(value):
            if not isinstance(value, str):
                return pd.NA
            match = _ORACLE_DATE.match(value)
            if not match:
                return pd.NA
            day, month, short_year = match.groups()
            month = MONTHS.get(month)
            if month is None:
                return pd.NA
            year = century + int(short_year)
            if data_year is not None and abs(year - data_year) > 50:
                year += 100 if year < data_year else -100
            return f"{year:04d}-{month}-{day}"

        iso = iso.fillna(oracle.map(convert))
    return iso


def _as_number(series, integer=True):
    """Null out values that BigQuery's SAFE_CAST would silently drop.

    ``age_num`` is the reason this exists: it carries the non-numeric age codes
    ``NS``, ``BB``, ``NB`` and ``NN`` in 12-14% of rows. Those codes are already
    preserved in ``age_code``, so nulling the numeric column loses nothing and
    stops a typed column from quietly emptying in the dbt model.
    """
    if series is None or len(series) == 0:
        return series
    text = series.astype("object").str.strip()
    pattern = r"^-?\d+$" if integer else r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"
    return text.where(text.str.match(pattern, na=False))


def _code_maps(zf, members):
    """Build ``{lookup name: {surrogate id: published code}}`` for this bundle."""
    maps = {}
    for logical, (id_col, code_col) in CODE_LOOKUPS.items():
        frame = _read(zf, members, logical)
        if (
            frame is None
            or id_col not in frame.columns
            or code_col not in frame.columns
        ):
            maps[logical] = {}
            continue
        maps[logical] = dict(zip(frame[id_col], frame[code_col], strict=False))
    # Older bundles key offences on a surrogate; newer ones use the code itself.
    offense_type = _read(zf, members, "nibrs_offense_type")
    if offense_type is not None and "offense_type_id" in offense_type.columns:
        maps["offense_type"] = dict(
            zip(
                offense_type["offense_type_id"],
                offense_type["offense_code"],
                strict=False,
            )
        )
    else:
        maps["offense_type"] = {}
    return maps


def _resolve(series, mapping):
    """Map surrogate ids to published codes, leaving already-coded values alone."""
    if not mapping:
        return series
    return series.map(lambda v: mapping.get(v, v) if pd.notna(v) else pd.NA)


def _fold(child, parent_key, code_col, mapping, count_name):
    """Collapse an up-to-N link table onto its parent.

    Returns a frame of ``parent_key``, the lowest published code, and how many
    rows the child held, so a user can see when the fold dropped detail.
    """
    if child is None or child.empty or parent_key not in child.columns:
        return pd.DataFrame(columns=[parent_key, "code", count_name])
    resolved = _resolve(child[code_col], mapping)
    frame = pd.DataFrame({parent_key: child[parent_key], "code": resolved})
    grouped = frame.groupby(parent_key, dropna=True)
    out = grouped["code"].min().to_frame("code")
    # Counts must serialise as "3", never "3.0": the staging parquet is
    # all-string and safe_cast would turn "3.0" into NULL on an INT64 column.
    out[count_name] = grouped.size().astype("int64").astype(str)
    return out.reset_index()


def clean_nibrs_bundle(zip_path, state_abbr, year):
    """Clean one state-year NIBRS bundle into the project's table schema.

    Returns ``{table name: DataFrame}`` with every value already a string, in
    the column order declared by :mod:`spec`. Tables absent from the bundle come
    back as empty frames with the right columns.
    """
    from pipelines.datasets.us_fbi_cde.spec import column_names

    state_abbr = canonical_state(state_abbr)

    with zipfile.ZipFile(zip_path) as zf:
        members = _members(zf)
        maps = _code_maps(zf, members)

        month = _read(zf, members, "nibrs_month")
        agencies = _read(zf, members, "agencies")
        if agencies is None:
            agencies = _read(zf, members, "cde_agencies")
        incident = _read(zf, members, "nibrs_incident")
        offense = _read(zf, members, "nibrs_offense")
        offender = _read(zf, members, "nibrs_offender")
        victim = _read(zf, members, "nibrs_victim")
        victim_offense = _read(zf, members, "nibrs_victim_offense")
        victim_rel = _read(zf, members, "nibrs_victim_offender_rel")
        victim_injury = _read(zf, members, "nibrs_victim_injury")
        arrestee = _read(zf, members, "nibrs_arrestee")
        arrestee_weapon = _read(zf, members, "nibrs_arrestee_weapon")
        groupb = _read(zf, members, "nibrs_arrestee_groupb")
        groupb_weapon = _read(zf, members, "nibrs_arrestee_groupb_weapon")
        weapon = _read(zf, members, "nibrs_weapon")
        bias = _read(zf, members, "nibrs_bias_motivation")
        prop = _read(zf, members, "nibrs_property")
        prop_desc = _read(zf, members, "nibrs_property_desc")
        drug = _read(zf, members, "nibrs_suspected_drug")

    out = {}

    # --- incident: attach the ORI through the monthly table -------------
    ori_by_agency = {}
    if agencies is not None and "ori" in agencies.columns:
        key = "agency_id" if "agency_id" in agencies.columns else None
        if key:
            ori_by_agency = dict(
                zip(agencies[key], agencies["ori"], strict=False)
            )

    if incident is not None and not incident.empty:
        agency_id = (
            incident["agency_id"] if "agency_id" in incident.columns else None
        )
        if (
            agency_id is None
            and month is not None
            and "nibrs_month_id" in incident.columns
        ):
            month_to_agency = dict(
                zip(month["nibrs_month_id"], month["agency_id"], strict=False)
            )
            agency_id = incident["nibrs_month_id"].map(month_to_agency)
        frame = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "ori": agency_id.map(ori_by_agency)
                if agency_id is not None
                else pd.NA,
                "incident_id": incident["incident_id"],
                "incident_date": _as_date(
                    _first_column(incident, "incident_date"), year
                ),
                "incident_hour": _first_column(incident, "incident_hour"),
                "report_date_flag": _first_column(
                    incident, "report_date_flag"
                ),
                "cargo_theft_flag": _first_column(
                    incident, "cargo_theft_flag"
                ),
                "cleared_except_code": _resolve(
                    _first_column(incident, "cleared_except_id"),
                    maps["nibrs_cleared_except"],
                ),
                "cleared_except_date": _as_date(
                    _first_column(incident, "cleared_except_date"), year
                ),
                "incident_status": _first_column(incident, "incident_status"),
                "submission_date": _as_date(
                    _first_column(incident, "submission_date"), year
                ),
            }
        )
        out["incident"] = frame
        incident_to_ori = dict(
            zip(frame["incident_id"], frame["ori"], strict=False)
        )
    else:
        out["incident"] = pd.DataFrame(columns=column_names("incident"))
        incident_to_ori = {}

    # --- offense, with weapon and bias motivation folded on --------------
    if offense is not None and not offense.empty:
        offense_code = _first_column(offense, "offense_code")
        if offense_code.isna().all():
            offense_code = _resolve(
                _first_column(offense, "offense_type_id"), maps["offense_type"]
            )
        folded_weapon = _fold(
            weapon,
            "offense_id",
            "weapon_id",
            maps["nibrs_weapon_type"],
            "weapon_count",
        ).rename(columns={"code": "weapon_code"})
        folded_bias = _fold(
            bias,
            "offense_id",
            "bias_id",
            maps["nibrs_bias_list"],
            "bias_motivation_count",
        ).rename(columns={"code": "bias_motivation_code"})
        frame = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "offense_id": offense["offense_id"],
                "incident_id": offense["incident_id"],
                "offense_code": offense_code,
                "attempt_complete_flag": _first_column(
                    offense, "attempt_complete_flag"
                ),
                "location_code": _resolve(
                    _first_column(offense, "location_id"),
                    maps["nibrs_location_type"],
                ),
                "premises_entered_count": _first_column(
                    offense, "num_premises_entered"
                ),
                "method_entry_code": _first_column(
                    offense, "method_entry_code"
                ),
            }
        )
        frame = frame.merge(folded_weapon, on="offense_id", how="left")
        frame = frame.merge(folded_bias, on="offense_id", how="left")
        out["offense"] = frame
    else:
        out["offense"] = pd.DataFrame(columns=column_names("offense"))

    # --- offender --------------------------------------------------------
    if offender is not None and not offender.empty:
        out["offender"] = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "offender_id": offender["offender_id"],
                "incident_id": offender["incident_id"],
                "offender_sequence_number": _first_column(
                    offender, "offender_seq_num"
                ),
                "age_code": _resolve(
                    _first_column(offender, "age_id"), maps["nibrs_age"]
                ),
                "age": _as_number(_first_column(offender, "age_num")),
                "age_range_low": _as_number(
                    _first_column(offender, "age_range_low_num")
                ),
                "age_range_high": _as_number(
                    _first_column(
                        offender, "age_range_high_num", "age_code_range_high"
                    )
                ),
                "sex_code": _first_column(offender, "sex_code"),
                "race_code": _resolve(
                    _first_column(offender, "race_id"), maps["ref_race"]
                ),
                "ethnicity_code": _resolve(
                    _first_column(offender, "ethnicity_id"),
                    maps["nibrs_ethnicity"],
                ),
            }
        )
    else:
        out["offender"] = pd.DataFrame(columns=column_names("offender"))

    # --- victim, with injury folded on -----------------------------------
    if victim is not None and not victim.empty:
        folded_injury = _fold(
            victim_injury,
            "victim_id",
            "injury_id",
            maps["nibrs_injury"],
            "injury_count",
        ).rename(columns={"code": "injury_code"})
        frame = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "victim_id": victim["victim_id"],
                "incident_id": victim["incident_id"],
                "victim_sequence_number": _first_column(
                    victim, "victim_seq_num"
                ),
                "victim_type_code": _resolve(
                    _first_column(victim, "victim_type_id"),
                    maps["nibrs_victim_type"],
                ),
                "age_code": _resolve(
                    _first_column(victim, "age_id"), maps["nibrs_age"]
                ),
                "age": _as_number(_first_column(victim, "age_num")),
                "age_range_low": _as_number(
                    _first_column(victim, "age_range_low_num")
                ),
                "age_range_high": _as_number(
                    _first_column(
                        victim, "age_range_high_num", "age_code_range_high"
                    )
                ),
                "sex_code": _first_column(victim, "sex_code"),
                "race_code": _resolve(
                    _first_column(victim, "race_id"), maps["ref_race"]
                ),
                "ethnicity_code": _resolve(
                    _first_column(victim, "ethnicity_id"),
                    maps["nibrs_ethnicity"],
                ),
                "resident_status_code": _first_column(
                    victim, "resident_status_code"
                ),
                "assignment_type_code": _resolve(
                    _first_column(victim, "assignment_type_id"),
                    maps["nibrs_assignment_type"],
                ),
                "activity_type_code": _resolve(
                    _first_column(victim, "activity_type_id"),
                    maps["nibrs_activity_type"],
                ),
            }
        )
        frame = frame.merge(folded_injury, on="victim_id", how="left")
        out["victim"] = frame
    else:
        out["victim"] = pd.DataFrame(columns=column_names("victim"))

    # --- victim link tables ----------------------------------------------
    if victim_offense is not None and not victim_offense.empty:
        out["victim_offense"] = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "victim_id": victim_offense["victim_id"],
                "offense_id": victim_offense["offense_id"],
            }
        )
    else:
        out["victim_offense"] = pd.DataFrame(
            columns=column_names("victim_offense")
        )

    if victim_rel is not None and not victim_rel.empty:
        out["victim_offender_relationship"] = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "victim_id": victim_rel["victim_id"],
                "offender_id": victim_rel["offender_id"],
                "relationship_code": _resolve(
                    _first_column(victim_rel, "relationship_id"),
                    maps["nibrs_relationship"],
                ),
            }
        )
    else:
        out["victim_offender_relationship"] = pd.DataFrame(
            columns=column_names("victim_offender_relationship")
        )

    # --- arrestee: Group A from incidents, Group B standalone -------------
    arrestee_frames = []
    if arrestee is not None and not arrestee.empty:
        offense_code = _first_column(arrestee, "offense_code")
        if offense_code.isna().all():
            offense_code = _resolve(
                _first_column(arrestee, "offense_type_id"),
                maps["offense_type"],
            )
        folded = _fold(
            arrestee_weapon,
            "arrestee_id",
            "weapon_id",
            maps["nibrs_weapon_type"],
            "_weapon_count",
        ).rename(columns={"code": "weapon_code"})
        frame = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "ori": arrestee["incident_id"].map(incident_to_ori),
                "arrestee_id": arrestee["arrestee_id"],
                "incident_id": arrestee["incident_id"],
                "arrest_group": "A",
                "arrestee_sequence_number": _first_column(
                    arrestee, "arrestee_seq_num"
                ),
                "arrest_date": _as_date(
                    _first_column(arrestee, "arrest_date"), year
                ),
                "arrest_type_code": _resolve(
                    _first_column(arrestee, "arrest_type_id"),
                    maps["nibrs_arrest_type"],
                ),
                "multiple_arrestee_indicator": _first_column(
                    arrestee, "multiple_indicator"
                ),
                "offense_code": offense_code,
                "age_code": _resolve(
                    _first_column(arrestee, "age_id"), maps["nibrs_age"]
                ),
                "age": _as_number(_first_column(arrestee, "age_num")),
                "age_range_low": _as_number(
                    _first_column(arrestee, "age_range_low_num")
                ),
                "age_range_high": _first_column(
                    arrestee, "age_range_high_num"
                ),
                "sex_code": _first_column(arrestee, "sex_code"),
                "race_code": _resolve(
                    _first_column(arrestee, "race_id"), maps["ref_race"]
                ),
                "ethnicity_code": _resolve(
                    _first_column(arrestee, "ethnicity_id"),
                    maps["nibrs_ethnicity"],
                ),
                "resident_status_code": _first_column(
                    arrestee, "resident_code"
                ),
                "under_18_disposition_code": _first_column(
                    arrestee, "under_18_disposition_code"
                ),
            }
        )
        frame = frame.merge(folded, on="arrestee_id", how="left")
        frame = frame.drop(columns=["_weapon_count"], errors="ignore")
        arrestee_frames.append(frame)

    if groupb is not None and not groupb.empty:
        folded = _fold(
            groupb_weapon,
            "groupb_arrestee_id",
            "weapon_id",
            maps["nibrs_weapon_type"],
            "_weapon_count",
        ).rename(columns={"code": "weapon_code"})
        frame = pd.DataFrame(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "ori": pd.NA,
                "arrestee_id": groupb["groupb_arrestee_id"],
                "incident_id": pd.NA,
                "arrest_group": "B",
                "arrestee_sequence_number": _first_column(
                    groupb, "arrestee_seq_num"
                ),
                "arrest_date": _as_date(
                    _first_column(groupb, "arrest_date"), year
                ),
                "arrest_type_code": _resolve(
                    _first_column(groupb, "arrest_type_id"),
                    maps["nibrs_arrest_type"],
                ),
                "multiple_arrestee_indicator": pd.NA,
                "offense_code": _first_column(groupb, "offense_code"),
                "age_code": _resolve(
                    _first_column(groupb, "age_id"), maps["nibrs_age"]
                ),
                "age": _as_number(_first_column(groupb, "age_num")),
                "age_range_low": _as_number(
                    _first_column(groupb, "age_range_low_num")
                ),
                "age_range_high": _as_number(
                    _first_column(groupb, "age_range_high_num")
                ),
                "sex_code": _first_column(groupb, "sex_code"),
                "race_code": _resolve(
                    _first_column(groupb, "race_id"), maps["ref_race"]
                ),
                "ethnicity_code": _resolve(
                    _first_column(groupb, "ethnicity_id"),
                    maps["nibrs_ethnicity"],
                ),
                "resident_status_code": _first_column(groupb, "resident_code"),
                "under_18_disposition_code": _first_column(
                    groupb, "under_18_disposition_code"
                ),
            }
        )
        frame = frame.merge(
            folded.rename(columns={"groupb_arrestee_id": "arrestee_id"}),
            on="arrestee_id",
            how="left",
        )
        frame = frame.drop(columns=["_weapon_count"], errors="ignore")
        arrestee_frames.append(frame)

    out["arrestee"] = (
        pd.concat(arrestee_frames, ignore_index=True)
        if arrestee_frames
        else pd.DataFrame(columns=column_names("arrestee"))
    )

    # --- property: one row per description, keeping description-less rows --
    if prop is not None and not prop.empty:
        base = pd.DataFrame(
            {
                "property_id": prop["property_id"],
                "incident_id": prop["incident_id"],
                "property_loss_code": _resolve(
                    _first_column(prop, "prop_loss_id"),
                    maps["nibrs_prop_loss_type"],
                ),
                "stolen_count": _as_number(
                    _first_column(prop, "stolen_count")
                ),
                "recovered_count": _as_number(
                    _first_column(prop, "recovered_count")
                ),
            }
        )
        if prop_desc is not None and not prop_desc.empty:
            desc = pd.DataFrame(
                {
                    "property_id": prop_desc["property_id"],
                    "property_description_id": _first_column(
                        prop_desc, "nibrs_prop_desc_id"
                    ),
                    "property_description_code": _resolve(
                        _first_column(prop_desc, "prop_desc_id"),
                        maps["nibrs_prop_desc_type"],
                    ),
                    "property_value": _first_column(
                        prop_desc, "property_value"
                    ),
                    "date_recovered": _as_date(
                        _first_column(prop_desc, "date_recovered"), year
                    ),
                }
            )
        else:
            desc = pd.DataFrame(
                columns=[
                    "property_id",
                    "property_description_id",
                    "property_description_code",
                    "property_value",
                    "date_recovered",
                ]
            )
        frame = base.merge(desc, on="property_id", how="left")
        # The published nibrs_prop_desc_id is frequently blank, and one property
        # record can carry several descriptions, so falling back to the property
        # id alone collides. The natural key is the property plus the description
        # type; where even that is absent the record has no description at all
        # and there is exactly one row for it.
        fallback = "p" + frame["property_id"].astype(str)
        with_code = frame["property_description_code"].notna()
        fallback = fallback.where(
            ~with_code,
            fallback + "-" + frame["property_description_code"].astype(str),
        )
        frame["property_description_id"] = frame[
            "property_description_id"
        ].fillna(fallback)
        folded_drug = _fold(
            drug,
            "property_id",
            "suspected_drug_type_id",
            maps["nibrs_suspected_drug_type"],
            "_drug_count",
        ).rename(columns={"code": "suspected_drug_code"})
        frame = frame.merge(folded_drug, on="property_id", how="left")
        if (
            drug is not None
            and not drug.empty
            and "property_id" in drug.columns
        ):
            first_drug = (
                drug.sort_values("suspected_drug_type_id")
                .groupby("property_id", as_index=False)
                .first()
            )
            quantities = pd.DataFrame(
                {
                    "property_id": first_drug["property_id"].to_numpy(),
                    "drug_quantity": _first_column(
                        first_drug, "est_drug_qty"
                    ).to_numpy(),
                    "drug_measure_code": _resolve(
                        _first_column(first_drug, "drug_measure_type_id"),
                        maps["nibrs_drug_measure_type"],
                    ).to_numpy(),
                }
            )
            frame = frame.merge(quantities, on="property_id", how="left")
        frame["year"] = str(year)
        frame["state_abbr"] = state_abbr
        frame = frame.drop(columns=["_drug_count"], errors="ignore")
        out["property"] = frame
    else:
        out["property"] = pd.DataFrame(columns=column_names("property"))

    # --- agency-year NIBRS participation, folded into the agency table -----
    # Counting the monthly table works identically in both bundle eras, so it is
    # preferred over the pre-2020 agency_participation.csv, which exists only in
    # the early bundles.
    participation = pd.DataFrame(
        columns=["year", "ori", "nibrs_months_reported"]
    )
    if month is not None and not month.empty and ori_by_agency:
        reported = month[month["reported_status"].isin(["I", "Z"])]
        counts = reported.groupby("agency_id")["month_num"].nunique()
        participation = pd.DataFrame(
            {
                "year": str(year),
                "ori": [ori_by_agency.get(a) for a in counts.index],
                "nibrs_months_reported": counts.astype("int64")
                .astype(str)
                .to_numpy(),
            }
        )
        participation = participation[participation["ori"].notna()]
    out["_participation"] = participation

    # --- the agency attribute rows this bundle contributes -----------------
    # The post-2020 agencies.csv is one row per agency-year. The pre-2020
    # cde_agencies.csv is a current-state list covering every agency the state
    # ever had, so it is restricted to the ORIs that actually reported in this
    # year before its attributes are carried forward.
    if (
        agencies is not None
        and not agencies.empty
        and "ori" in agencies.columns
    ):
        attributes = pd.DataFrame(
            {
                "year": str(year),
                "ori": agencies["ori"],
                "legacy_ori": _first_column(agencies, "legacy_ori"),
                # Only the post-2020 bundles carry a real start date; the
                # older ones give a bare year, which is left null rather than
                # fabricated into a January date.
                "nibrs_start_date": _as_date(
                    _first_column(agencies, "nibrs_start_date"), year
                ),
                "nibrs_participated": _first_column(
                    agencies, "nibrs_participated"
                ),
                "state_abbr": _first_column(agencies, "state_abbr"),
            }
        ).drop_duplicates(subset=["year", "ori"])
        if "data_year" not in agencies.columns and not participation.empty:
            attributes = attributes[
                attributes["ori"].isin(set(participation["ori"]))
            ]
        out["_agency_attributes"] = attributes
    else:
        out["_agency_attributes"] = pd.DataFrame(
            columns=[
                "year",
                "ori",
                "legacy_ori",
                "nibrs_start_date",
                "nibrs_participated",
                "state_abbr",
            ]
        )

    # Enforce declared column order and string dtype on the published tables.
    for table in list(out):
        if table.startswith("_"):
            continue
        names = column_names(table)
        frame = out[table]
        for name in names:
            if name not in frame.columns:
                frame[name] = pd.NA
        out[table] = frame[names]

    # Row-count parity against the source CSVs. A fold or a merge that silently
    # duplicated or dropped rows is invisible in the output, so it is asserted
    # here rather than left for a later aggregate check.
    expected = {
        "incident": len(incident) if incident is not None else 0,
        "offense": len(offense) if offense is not None else 0,
        "offender": len(offender) if offender is not None else 0,
        "victim": len(victim) if victim is not None else 0,
        "victim_offense": len(victim_offense)
        if victim_offense is not None
        else 0,
        "victim_offender_relationship": len(victim_rel)
        if victim_rel is not None
        else 0,
        "arrestee": (len(arrestee) if arrestee is not None else 0)
        + (len(groupb) if groupb is not None else 0),
    }
    for table, count in expected.items():
        if len(out[table]) != count:
            raise ValueError(
                f"{zip_path}: {table} produced {len(out[table])} rows, "
                f"source has {count}"
            )
    # property is a left join of the property records onto their descriptions,
    # so it has one row per description plus one for each record without any.
    if prop is not None and prop_desc is not None:
        described = (
            prop_desc["property_id"].nunique() if not prop_desc.empty else 0
        )
        undescribed = prop["property_id"].nunique() - described
        target = len(prop_desc) + max(undescribed, 0)
        if len(out["property"]) != target:
            raise ValueError(
                f"{zip_path}: property produced {len(out['property'])} rows, "
                f"expected {target}"
            )
    return out


# --------------------------------------------------------------------------
# Return A summary master files
# --------------------------------------------------------------------------

RETA_LRECL = 7385
RETA_HEADER_LEN = 305
RETA_MONTH_LEN = 590

# The 28 offence line items of the Return A form, in file order. Item 28 is
# unused and item 27 carries data only for years before 1974, so neither is
# published.
RETA_OFFENSES = [
    "murder",
    "manslaughter",
    "rape_total",
    "rape_by_force",
    "attempted_rape",
    "robbery_total",
    "robbery_firearm",
    "robbery_knife",
    "robbery_other_weapon",
    "robbery_strong_arm",
    "assault_total",
    "assault_firearm",
    "assault_knife",
    "assault_other_weapon",
    "assault_hands_feet",
    "assault_simple",
    "burglary_total",
    "burglary_forcible_entry",
    "burglary_unlawful_entry",
    "burglary_attempted",
    "larceny_total",
    "motor_vehicle_theft_total",
    "motor_vehicle_theft_auto",
    "motor_vehicle_theft_truck_bus",
    "motor_vehicle_theft_other",
    "grand_total",
    "larceny_under_50",
    "unused",
]
RETA_PUBLISHED_OFFENSES = RETA_OFFENSES[:26]

# Offsets of each card within a month block, 0-based from the block start.
RETA_CARDS = {
    "unfounded_count": 17,
    "actual_count": 157,
    "cleared_count": 297,
    "juvenile_cleared_count": 437,
}

# Zoned-decimal overpunch: the final character carries the sign and last digit.
RETA_OVERPUNCH = {c: i for i, c in enumerate("}JKLMNOPQR")}

# Numeric state code -> postal abbreviation, from the record description.
RETA_STATE_CODES = {
    "50": "AK",
    "01": "AL",
    "03": "AR",
    "54": "AS",
    "02": "AZ",
    "04": "CA",
    "05": "CO",
    "06": "CT",
    "52": "CZ",
    "08": "DC",
    "07": "DE",
    "09": "FL",
    "10": "GA",
    "55": "GM",
    "51": "HI",
    "14": "IA",
    "11": "ID",
    "12": "IL",
    "13": "IN",
    "15": "KS",
    "16": "KY",
    "17": "LA",
    "20": "MA",
    "19": "MD",
    "18": "ME",
    "21": "MI",
    "22": "MN",
    "24": "MO",
    "23": "MS",
    "25": "MT",
    "26": "NB",
    "32": "NC",
    "33": "ND",
    "28": "NH",
    "29": "NJ",
    "30": "NM",
    "27": "NV",
    "31": "NY",
    "34": "OH",
    "35": "OK",
    "36": "OR",
    "37": "PA",
    "53": "PR",
    "38": "RI",
    "39": "SC",
    "40": "SD",
    "41": "TN",
    "42": "TX",
    "43": "UT",
    "62": "VI",
    "45": "VA",
    "44": "VT",
    "46": "WA",
    "48": "WI",
    "47": "WV",
    "49": "WY",
    # Absent from the 1990 record description but present in the recent
    # files: 98 is the federal agencies (ATF, FBI field offices), which the
    # FBI's own employee extract also codes FS; 69 is the Northern Mariana
    # Islands, whose ORIs carry the MK prefix.
    "98": "FS",
    "69": "MK",
}


def parse_reta_count(raw):
    """Parse one 5-character Return A count field.

    Blank means not reported. A trailing overpunch character marks a negative
    adjustment, which the FBI uses to correct a previously filed month.
    """
    raw = raw.strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    body, last = raw[:-1], raw[-1]
    if last in RETA_OVERPUNCH and (body.isdigit() or body == ""):
        return -(int(body or 0) * 10 + RETA_OVERPUNCH[last])
    return None


def iter_reta_lines(zip_path):
    """Yield the decoded lines of the single flat file inside a Return A zip.

    The member name changes almost every year (``KCRETA85.DAT``, ``KEN95``,
    ``RETA-COMB.txt``, ``reta-2023.txt``), so it is taken positionally. The
    1990-1996 archives use PKWARE Implode, which Python's ``zipfile`` cannot
    decompress, so those fall back to the ``unzip`` binary. Line endings are
    CRLF in the older files and LF in the newer ones.
    """
    import subprocess

    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise ValueError(
                f"{zip_path}: expected one member, found {len(members)}"
            )
        member = members[0]
        if member.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            with zf.open(member) as handle:
                for raw in handle:
                    yield raw.decode("latin-1").rstrip("\r\n")
            return

    process = subprocess.Popen(
        ["unzip", "-p", str(zip_path)], stdout=subprocess.PIPE
    )
    stream = process.stdout
    if stream is None:
        raise RuntimeError(f"unzip produced no output for {zip_path}")
    try:
        for raw in stream:
            yield raw.decode("latin-1").rstrip("\r\n")
    finally:
        stream.close()
        if process.wait() != 0:
            raise RuntimeError(f"unzip failed for {zip_path}")


def parse_reta_file(zip_path, year):
    """Parse one Return A flat file.

    Returns ``(summary, agency_year)``: the long agency x month x offence counts
    and the per-agency-year header fields that belong on the agency table.
    """
    summary_rows = []
    agency_rows = []
    # The recent files carry more than one physical record for some agencies —
    # 272 ORIs in 2022, one of them 16 times — each with its own counts. They
    # are kept and numbered rather than collapsed, since nothing in the record
    # says whether they replace or supplement one another.
    record_seen = Counter()
    for line in iter_reta_lines(zip_path):
        if len(line) != RETA_LRECL:
            continue
        state_abbr = RETA_STATE_CODES.get(line[1:3])
        legacy_ori = line[3:10].strip()
        if not legacy_ori:
            continue
        record_seen[legacy_ori] += 1
        record_number = str(record_seen[legacy_ori])
        months_reported = line[41:43].strip()
        killed_felonious = killed_accidental = assaulted = 0
        for month_index in range(12):
            start = RETA_HEADER_LEN + month_index * RETA_MONTH_LEN
            block = line[start : start + RETA_MONTH_LEN]
            # Month block layout, 0-based from the block start: 0-1 month
            # included in, 2-7 date of last update, 8-12 card 0-4 type,
            # 13-16 card 0-3 P/T, then the four 140-character card bodies.
            record_type = block[9]  # card 1 type: the actual-offense card
            breakdown = block[14]  # card 1 P/T
            killed_felonious += parse_reta_count(block[577:580]) or 0
            killed_accidental += parse_reta_count(block[580:583]) or 0
            assaulted += parse_reta_count(block[583:590]) or 0
            if record_type in ("", " ", "0"):
                continue  # month not updated: nothing was filed
            for index, offense in enumerate(RETA_PUBLISHED_OFFENSES):
                counts = {}
                any_value = False
                for measure, offset in RETA_CARDS.items():
                    value = parse_reta_count(
                        block[offset + index * 5 : offset + (index + 1) * 5]
                    )
                    counts[measure] = value
                    if value:
                        any_value = True
                if not any_value:
                    continue
                summary_rows.append(
                    {
                        "year": str(year),
                        "state_abbr": state_abbr,
                        "legacy_ori": legacy_ori,
                        "record_number": record_number,
                        "month": str(month_index + 1),
                        "offense_code": offense,
                        "actual_count": counts["actual_count"],
                        "unfounded_count": counts["unfounded_count"],
                        "cleared_count": counts["cleared_count"],
                        "juvenile_cleared_count": counts[
                            "juvenile_cleared_count"
                        ],
                        "record_type_code": record_type,
                        "breakdown_reported_flag": breakdown.strip(),
                    }
                )
        agency_rows.append(
            {
                "year": str(year),
                "state_abbr": state_abbr,
                "legacy_ori": legacy_ori,
                "record_number": record_number,
                "core_city_flag": line[22].strip(),
                "covered_by_ori": line[23:30].strip(),
                "summary_months_reported": months_reported,
                "officer_killed_felonious_count": str(killed_felonious),
                "officer_killed_accidental_count": str(killed_accidental),
                "officer_assaulted_count": str(assaulted),
            }
        )
    return pd.DataFrame(summary_rows), pd.DataFrame(agency_rows)


# --------------------------------------------------------------------------
# Parquet output
# --------------------------------------------------------------------------


def write_partition(frame, table, output_root, partition_values):
    """Write one hive partition as all-STRING snappy parquet.

    Staging is all-string by house convention: ``upload_to_gcs`` builds the
    external table from a stringified one-row header, so typed parquet is
    rejected on read, and the dbt model ``safe_cast``s every column anyway.
    The cast goes through arrow rather than ``astype(str)``, which would render
    NULL as the literal string "nan".
    """
    from pipelines.datasets.us_fbi_cde.spec import column_names

    names = [n for n in column_names(table) if n not in partition_values]
    if frame.empty:
        return None
    frame = frame.loc[:, [n for n in names if n in frame.columns]]
    for name in names:
        if name not in frame.columns:
            frame[name] = pd.NA
    frame = frame[names]

    schema = pa.schema([pa.field(name, pa.string()) for name in names])
    table_arrow = pa.Table.from_pandas(frame, preserve_index=False)
    table_arrow = table_arrow.cast(schema)

    parts = "/".join(f"{k}={v}" for k, v in partition_values.items())
    directory = Path(output_root) / table / parts
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "data.parquet"
    pq.write_table(table_arrow, destination, compression="snappy")
    return destination


def read_csv_all_strings(path, **kwargs):
    """Read a CSV with every column as a nullable string."""
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_values=NA_VALUES,
        low_memory=False,
        **kwargs,
    )
