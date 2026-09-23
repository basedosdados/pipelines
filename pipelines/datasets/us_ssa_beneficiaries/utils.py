"""Pure download and cleaning functions for us_ssa_beneficiaries.

No Prefect imports here: the one-shot onboarding bootstrap under
``models/us_ssa_beneficiaries/code/`` imports these same functions, so the
cleaning transform lives in exactly one place.

Source
------
SSA publishes "flattened time series" JSON concatenating every annual edition
of *OASDI Beneficiaries by State and County* (1999-) and *SSI Recipients by
State and County* (1998-).  Each file carries its own schema metadata
(dimensions, measures, units, footnotes), so the transform is driven by the
published metadata rather than by hardcoded column positions.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download_all(output_dir: Path | str) -> list[Path]:
    """Fetch the eight flattened time-series JSON files.

    www.ssa.gov sits behind Akamai, which returns HTTP 403 to requests, urllib
    and plain curl regardless of headers.  curl_cffi's Chrome TLS fingerprint
    is admitted.  (The page also advertises .csv links, but those are generated
    in the browser from the JSON and 404 server-side -- JSON is the only real
    artefact.)
    """
    from curl_cffi import requests as cr

    from pipelines.datasets.us_ssa_beneficiaries.constants import constants

    bases = {
        "oasdi": constants.OASDI_BASE.value,
        "ssi": constants.SSI_BASE.value,
    }
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for base_key, stem in constants.SOURCE_FILES.value:
        url = bases[base_key] + stem + ".json"
        response = cr.get(
            url, impersonate=constants.IMPERSONATE.value, timeout=600
        )
        response.raise_for_status()
        # Fail loudly on a WAF interstitial served with HTTP 200.
        if not response.content.lstrip().startswith(b"{"):
            raise ValueError(
                f"{url} did not return JSON (got {response.content[:120]!r})"
            )
        dest = output_dir / f"{stem}.json"
        dest.write_bytes(response.content)
        log.info("downloaded %s (%d bytes)", dest.name, len(response.content))
        written.append(dest)
    return written


# ---------------------------------------------------------------------------
# Geography
# ---------------------------------------------------------------------------

# Editions differ in how they label the same area.  Normalise to one spelling.
AREA_ALIASES = {
    "United States": "All areas",
    "Virgin Islands": "U.S. Virgin Islands",
}

# Two-digit ANSI/FIPS state codes.  Non-geographic residual rows
# ("All areas", "Other", "Foreign countries", "Unknown") map to None.
STATE_FIPS = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi": "28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
    "American Samoa": "60",
    "Guam": "66",
    "Northern Mariana Islands": "69",
    "Puerto Rico": "72",
    "U.S. Virgin Islands": "78",
}

STATE_TOTAL_LABEL = "State total"
NATIONAL_LABEL = "All areas"

# Maryland, Missouri, Nevada and Virginia contain independent cities that are
# not part of any county.  In ANSI these are numbered from 510 upward within
# the state; ordinary counties never reach 510 (Texas tops out at 48507).
CITY_STATES = {"Maryland", "Missouri", "Nevada", "Virginia"}

# Spelling variants used in the pre-2008 editions that survive neither exact
# nor whitespace-insensitive matching against the coded years.  Left-hand side
# is the old spelling as published, lowercased.
COUNTY_NAME_ALIASES = {
    # Straight misspellings in the older editions.
    ("Virginia", "albermarle"): "albemarle",
    ("Nebraska", "hitchock"): "hitchcock",
    ("North Dakota", "mountrial"): "mountrail",
    ("New Mexico", "san jaun"): "san juan",
    ("New York", "schnectady"): "schenectady",
    ("New Hampshire", "hillsboro"): "hillsborough",
    ("Michigan", "oickinson"): "dickinson",
    # Renamed between editions.  Dade County became Miami-Dade in 1997 but the
    # 1999 edition still used the old name.
    ("Florida", "dade"): "miami-dade",
    # Truncated in the pre-2007 editions.  Prince of Wales-Outer Ketchikan
    # (02201) was itself dissolved in 2008 into Prince of Wales-Hyder (02198)
    # and Wrangell (02275); it must map to its own code, not its successor's.
    ("Alaska", "prince of wales-outer"): "prince of wales-outer ketchikan",
}

# Editions 1999-2002 list Manassas city as "Manassas" and Manassas Park city as
# "Manassas City"; 2003 lists them as "Manassas City" and "Manassas Park City";
# 2004 onward as "Manassas" and "Manassas Park".  The middle spelling therefore
# means different cities in different years, so it is resolved by what else is
# present in the same state-year rather than by the label alone.
_MANASSAS_PARK = ("Virginia", "manassaspark")


def normalise_area(name: str) -> str:
    """Collapse cross-edition spellings of a state or area name."""
    return AREA_ALIASES.get(name.strip(), name.strip())


def _squash(name: str) -> str:
    """Lowercase, drop punctuation and whitespace: 'De Kalb' -> 'dekalb'."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _is_independent_city(state: str, ansi: str | None) -> bool:
    return bool(
        state in CITY_STATES
        and ansi
        and len(ansi) == 5
        and int(ansi[2:]) >= 510
    )


def build_ansi_crosswalk(rows: list[dict]) -> tuple[dict, dict]:
    """Build (county-block, city-block) name -> ANSI maps from the coded years.

    SSA introduced ANSI codes in the 2008 (OASDI) / 2009 (SSI) editions and
    back-filled ``null`` for earlier years.  The codes are recoverable because
    the same file carries both the coded and uncoded years: every county name
    is looked up against SSA's *own* later spelling, not an external gazetteer.
    """
    county_xw: dict[tuple[str, str], str] = {}
    city_xw: dict[tuple[str, str], str] = {}
    for r in rows:
        ansi = r.get("ansi")
        name = str(r.get("county_or_city", "")).strip()
        state = normalise_area(str(r.get("state_or_area", "")))
        if not ansi or len(ansi) != 5 or name == STATE_TOTAL_LABEL:
            continue
        target = city_xw if _is_independent_city(state, ansi) else county_xw
        target[(state, _squash(name))] = ansi
    return county_xw, city_xw


def _lookup(
    state: str, name: str, county_xw: dict, city_xw: dict, prefer_city: bool
) -> str | None:
    """Look a single county/city name up in SSA's own later-year spelling."""
    raw = name.strip()
    key = _squash(COUNTY_NAME_ALIASES.get((state, raw.lower()), raw))
    # Some editions spell out "Charles City County"; the coded years do not.
    if key.endswith("county") and len(key) > len("county"):
        key = key[: -len("county")]
    order = (city_xw, county_xw) if prefer_city else (county_xw, city_xw)

    # Before the 2005 edition, independent cities carry a " City" suffix and
    # are interleaved alphabetically with the counties; from 2005 the suffix is
    # dropped and the cities move to a trailing block.  So the old spelling
    # disambiguates what the new spelling cannot: "Bedford City" is the city,
    # plain "Bedford" the county.
    m = re.match(r"^(.*)city$", key)
    if m and state in CITY_STATES:
        hit = city_xw.get((state, m.group(1)))
        if hit:
            return hit
    for xw in order:
        hit = xw.get((state, key))
        if hit:
            return hit
    if m:
        # "Charles City" and "James City" are Virginia *counties* whose names
        # genuinely end in "City"; likewise "Carson City", Nevada.
        for xw in order:
            hit = xw.get((state, m.group(1)))
            if hit:
                return hit
    return None


def resolve_county_ids(
    rows: list[dict], county_xw: dict, city_xw: dict
) -> list[str | None]:
    """Reconstruct the county ANSI code for every row, in source order.

    Returns ``None`` rather than guessing.  Rows that stay ``None`` are
    genuinely unmappable: the "Unknown" county rows, and Alaska census areas
    and Virginia independent cities abolished or renamed before SSA began
    publishing codes.

    The 2005-2007 editions are the hard case: SSA had already dropped the
    " City" suffix but had not yet added ANSI codes, so seven names -- Baltimore,
    St. Louis, Bedford, Fairfax, Franklin, Richmond, Roanoke -- appear twice in
    the same state with nothing to tell them apart.  They are resolved by
    occurrence: SSA lists the counties alphabetically and then the independent
    cities alphabetically, so within a state-year the first is the county and
    the second the city.
    """
    # How many times each name occurs within its (year, state) block, and
    # which occurrence this row is.
    seen: dict[tuple, int] = {}
    counts: dict[tuple, int] = {}
    for r in rows:
        k = (
            str(r["month"])[:4],
            normalise_area(str(r["state_or_area"])),
            str(r.get("county_or_city", "")).strip(),
        )
        counts[k] = counts.get(k, 0) + 1

    out: list[str | None] = []
    for r in rows:
        ansi = r.get("ansi")
        state = normalise_area(str(r["state_or_area"]))
        name = str(r.get("county_or_city", "")).strip()
        if ansi and len(ansi) == 5:
            out.append(ansi)
            continue
        if not name or name == STATE_TOTAL_LABEL:
            out.append(None)
            continue
        k = (str(r["month"])[:4], state, name)
        occurrence = seen.get(k, 0)
        seen[k] = occurrence + 1
        if (state, _squash(name)) == ("Virginia", "manassascity") and (
            (k[0], state, "Manassas") in counts
        ):
            # Both spellings present: "Manassas" is the city, so this is the Park.
            out.append(city_xw.get(_MANASSAS_PARK))
            continue
        out.append(
            _lookup(
                state,
                name,
                county_xw,
                city_xw,
                prefer_city=counts[k] > 1 and occurrence > 0,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Value parsing
# ---------------------------------------------------------------------------

# SSA encodes non-numeric cells in-band.  Never coerce any of these to zero:
# a suppressed county is not a county with no beneficiaries.
# SSA encodes non-numeric cells in-band. Never coerce any of these to zero:
# a suppressed county is not a county with no beneficiaries.
#
# The `a` marker means different things on the two kinds of measure. On an
# amount it is SSA's "Less than $500" footnote. On a *count* it is an orphan
# marker with no footnote behind it -- it appears in exactly one row, Bedford
# city, Virginia in 2014, the year after the city was abolished and merged back
# into Bedford County -- so reading it as "less than $500 beneficiaries" would
# be nonsense. Hence a note map per measure kind.
AMOUNT_NOTES = {
    "(x)": "suppressed_disclosure",
    "a": "less_than_500_dollars",
}
COUNT_NOTES = {
    "(x)": "suppressed_disclosure",
    "a": "not_available",
}


def parse_value(
    raw: Any, notes: dict[str, str] = AMOUNT_NOTES
) -> tuple[float | None, str | None]:
    """Split a published cell into (numeric value, note code).

    ``(X)`` marks disclosure suppression and ``a`` marks a footnote whose
    meaning depends on the measure. Both carry real information and both are
    returned as a ``None`` value with the reason preserved, never as ``0``.

    Args:
        raw: The cell as published.
        notes: Marker-to-note-code map; pass :data:`COUNT_NOTES` for a count.
    """
    if raw is None:
        return None, "not_available"
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw), None
    text = str(raw).strip()
    if not text:
        return None, "not_available"
    note = notes.get(text.lower())
    if note:
        return None, note
    cleaned = text.replace(",", "").replace("$", "")
    try:
        return float(cleaned), None
    except ValueError:
        log.warning("unparseable cell %r -> not_available", raw)
        return None, "not_available"


# ---------------------------------------------------------------------------
# Measure -> category maps
# ---------------------------------------------------------------------------

# The eleven OASDI measures are two overlapping cuts of one universe: nine
# benefit types that sum to the total, plus a sex split of the 65-or-older
# subset.  Decomposing them into three orthogonal dimensions keeps both cuts
# addressable and makes the double-count hazard explicit -- summing across
# rows is only meaningful within one consistent slice.
OASDI_CATEGORIES = {
    "oasdi": ("total", "total", "total"),
    "ret_workers": ("retired_worker", "total", "total"),
    "ret_spouses": ("retired_spouse", "total", "total"),
    "ret_children": ("retired_child", "total", "total"),
    "surv_widows_parents": ("survivor_widow_parent", "total", "total"),
    "surv_children": ("survivor_child", "total", "total"),
    "di_workers": ("disabled_worker", "total", "total"),
    "di_spouses": ("disabled_spouse", "total", "total"),
    "di_children": ("disabled_child", "total", "total"),
    "oasdi_65_older_men": ("total", "65_or_older", "men"),
    "oasdi_65_older_women": ("total", "65_or_older", "women"),
}

# (eligibility_category, age_group, oasdi_concurrent)
SSI_CATEGORIES = {
    "ssi": ("total", "total", "total"),
    "ssi_aged": ("aged", "total", "total"),
    "ssi_blind_disabled": ("blind_or_disabled", "total", "total"),
    "ssi_under_18": ("total", "under_18", "total"),
    "ssi_18_64": ("total", "18_64", "total"),
    "ssi_65_older": ("total", "65_or_older", "total"),
    "concurrent": ("total", "total", "yes"),
    # SSI payment measures (Table 2) reuse the same suffixes without a prefix.
    "total": ("total", "total", "total"),
    "aged": ("aged", "total", "total"),
    "blind_disabled": ("blind_or_disabled", "total", "total"),
    "under_18": ("total", "under_18", "total"),
    "18_64": ("total", "18_64", "total"),
    "65_older": ("total", "65_or_older", "total"),
}


def _suffix(measure: str, prefixes: Iterable[str]) -> str:
    for p in prefixes:
        if measure.startswith(p):
            return measure[len(p) :]
    return measure


def load_table(path: Path | str) -> tuple[dict, list[dict]]:
    """Read one flattened time-series file, returning (metadata, rows)."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload["metadata"], payload["data"]


def _melt(
    rows: list[dict],
    measures: Iterable[str],
    prefixes: Iterable[str],
    categories: dict,
    dims: tuple[str, str, str],
    value_col: str,
    county_ids: list[str | None] | None = None,
    notes: dict[str, str] = AMOUNT_NOTES,
) -> pd.DataFrame:
    """Melt wide measures into one row per (key, category) with a note column."""
    out = []
    for idx, r in enumerate(rows):
        base = {
            "year": int(str(r["month"])[:4]),
            "state_or_area": normalise_area(str(r["state_or_area"])),
            "county_or_city": r.get("county_or_city"),
            "county_id": county_ids[idx] if county_ids is not None else None,
        }
        # Join on the resolved code, not the printed name: the count and amount
        # tables sometimes spell the same county differently in the same year
        # ("Dickinson" in Table 4, "Oickinson" in Table 5), which would split
        # one county into two half-populated rows.  Entities with no code fall
        # back to the name, which is unique among them.
        base["join_key"] = (
            base["county_id"] or f"name:{base['county_or_city']}"
        )
        for m in measures:
            suffix = _suffix(m, prefixes)
            cat = categories.get(suffix)
            if cat is None:
                raise KeyError(
                    f"measure {m!r} (suffix {suffix!r}) has no category mapping"
                )
            value, note = parse_value(r.get(m), notes)
            rec = dict(base)
            rec[dims[0]], rec[dims[1]], rec[dims[2]] = cat
            rec[value_col] = value
            rec[f"{value_col}_note"] = note
            out.append(rec)
    return pd.DataFrame(out)


def _merge_count_and_amount(
    counts: pd.DataFrame, amounts: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    carried = [
        c
        for c in ("county_or_city", "county_id")
        if c in counts.columns and c in amounts.columns and c not in keys
    ]
    merged = counts.merge(
        amounts.drop(columns=carried),
        on=keys,
        how="outer",
        validate="one_to_one",
    )
    return merged


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

OASDI_DIMS = ("benefit_type", "age_group", "sex")
SSI_DIMS = ("eligibility_category", "age_group", "oasdi_concurrent")

_COUNTY_KEYS = ["year", "state_or_area", "join_key", *OASDI_DIMS]
_STATE_KEYS = ["year", "state_or_area", *OASDI_DIMS]


def _drop_empty_duplicates(
    df: pd.DataFrame, keys: list[str], value_cols: list[str]
) -> pd.DataFrame:
    """Drop all-null rows that duplicate a populated row on the same key.

    A few editions carry a misspelled county alongside the correctly spelled
    one -- "San Jaun" beside "San Juan" in New Mexico 2001-2002, "Schnectady"
    beside "Schenectady" in New York 2001-2002 -- where the misspelled row has
    no values at all.  Once the misspelling is aliased onto the real county the
    two collide, so the empty one is dropped.  A duplicate that carries data is
    left in place, to surface rather than hide.
    """
    present = df[value_cols].notna().any(axis=1)
    dup = df.duplicated(keys, keep=False)
    drop = (
        dup
        & ~present
        & df.assign(_p=present)
        .groupby(keys, dropna=False)["_p"]
        .transform("any")
    )
    if drop.any():
        log.info(
            "dropped %d empty duplicate row(s) from the source",
            int(drop.sum()),
        )
    return df[~drop]


def _attach_state_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["state_id"] = df["state_or_area"].map(STATE_FIPS)
    return df


def build_oasdi_county(input_dir: Path) -> pd.DataFrame:
    """OASDI beneficiary counts and benefit amounts, by county and category."""
    meta4, rows4 = load_table(
        Path(input_dir) / "oasdi_state_county_table_4.json"
    )
    meta5, rows5 = load_table(
        Path(input_dir) / "oasdi_state_county_table_5.json"
    )
    county_xw, city_xw = build_ansi_crosswalk(rows4 + rows5)

    counts = _melt(
        rows4,
        meta4["measures"],
        ["persons_"],
        OASDI_CATEGORIES,
        OASDI_DIMS,
        "beneficiary_count",
        resolve_county_ids(rows4, county_xw, city_xw),
        COUNT_NOTES,
    )
    amounts = _melt(
        rows5,
        meta5["measures"],
        ["benefits_month_total_"],
        OASDI_CATEGORIES,
        OASDI_DIMS,
        "benefit_amount_month",
        resolve_county_ids(rows5, county_xw, city_xw),
    )
    df = _merge_count_and_amount(counts, amounts, _COUNTY_KEYS)

    # State totals live in oasdi_state; keeping them here would break the grain
    # and silently double every county sum.
    df = df[df["county_or_city"] != STATE_TOTAL_LABEL]
    df = _drop_empty_duplicates(
        df,
        ["year", "state_or_area", "county_id", *OASDI_DIMS],
        ["beneficiary_count", "benefit_amount_month"],
    )
    df = _attach_state_id(df)
    df = df.drop(columns=["join_key"])
    df = df.rename(
        columns={
            "state_or_area": "state_name",
            "county_or_city": "county_name",
        }
    )
    return df[
        [
            "year",
            "state_id",
            "county_id",
            "state_name",
            "county_name",
            *OASDI_DIMS,
            "beneficiary_count",
            "beneficiary_count_note",
            "benefit_amount_month",
            "benefit_amount_month_note",
        ]
    ].sort_values(
        ["year", "state_id", "county_id", *OASDI_DIMS], kind="stable"
    )


def build_oasdi_state(input_dir: Path) -> pd.DataFrame:
    """OASDI counts and amounts by state or other area, including territories."""
    meta2, rows2 = load_table(
        Path(input_dir) / "oasdi_state_county_table_2.json"
    )
    meta3, rows3 = load_table(
        Path(input_dir) / "oasdi_state_county_table_3.json"
    )
    meta4, rows4 = load_table(
        Path(input_dir) / "oasdi_state_county_table_4.json"
    )

    counts = _melt(
        rows2,
        meta2["measures"],
        ["persons_"],
        OASDI_CATEGORIES,
        OASDI_DIMS,
        "beneficiary_count",
        notes=COUNT_NOTES,
    )

    # SSA's flat file omits the whole 2010 edition of Table 2 (state counts),
    # although Table 3 (state amounts) and Table 4 (county counts) both have it.
    # The missing numbers are recoverable without inference: Table 4 carries
    # SSA's own "State total" row for 2010.  Verified identical to Table 2 in
    # every year where both exist.
    have = set(counts["year"].unique())
    recovered = _melt(
        [
            r
            for r in rows4
            if r.get("county_or_city") == STATE_TOTAL_LABEL
            and int(str(r["month"])[:4]) not in have
        ],
        meta4["measures"],
        ["persons_"],
        OASDI_CATEGORIES,
        OASDI_DIMS,
        "beneficiary_count",
        notes=COUNT_NOTES,
    )
    if not recovered.empty:
        log.info(
            "recovered %d state-count rows for year(s) %s missing from Table 2",
            len(recovered),
            sorted(recovered["year"].unique()),
        )
        counts = pd.concat([counts, recovered], ignore_index=True)

    amounts = _melt(
        rows3,
        meta3["measures"],
        ["benefits_month_total_"],
        OASDI_CATEGORIES,
        OASDI_DIMS,
        "benefit_amount_month",
    )
    drop = ["county_or_city", "county_id", "join_key"]
    counts = counts.drop(columns=drop, errors="ignore")
    amounts = amounts.drop(columns=drop, errors="ignore")
    df = _merge_count_and_amount(counts, amounts, _STATE_KEYS)
    df["state_id"] = df["state_or_area"].map(STATE_FIPS)
    return df[
        [
            "year",
            "state_id",
            "state_or_area",
            *OASDI_DIMS,
            "beneficiary_count",
            "beneficiary_count_note",
            "benefit_amount_month",
            "benefit_amount_month_note",
        ]
    ].sort_values(["year", "state_or_area", *OASDI_DIMS], kind="stable")


def build_oasdi_population_share(input_dir: Path) -> pd.DataFrame:
    """Resident population and the share of it receiving OASDI, by state."""
    _, rows = load_table(Path(input_dir) / "oasdi_state_county_table_1.json")
    groups = {
        "total": ("persons_us_pop", "percent_us_pop_oasdi"),
        "65_or_older": (
            "persons_us_pop_65_older",
            "percent_us_pop_65_older_oasdi",
        ),
    }
    out = []
    for r in rows:
        for group, (pop_col, pct_col) in groups.items():
            pop, pop_note = parse_value(r.get(pop_col), COUNT_NOTES)
            pct, pct_note = parse_value(r.get(pct_col))
            area = normalise_area(str(r["state_or_area"]))
            out.append(
                {
                    "year": int(str(r["month"])[:4]),
                    "state_id": STATE_FIPS.get(area),
                    "state_or_area": area,
                    "population_group": group,
                    "population": pop,
                    "population_note": pop_note,
                    "percentage_receiving_oasdi": pct,
                    "percentage_receiving_oasdi_note": pct_note,
                }
            )
    return pd.DataFrame(out).sort_values(
        ["year", "state_or_area", "population_group"], kind="stable"
    )


def build_ssi_county(input_dir: Path) -> pd.DataFrame:
    """SSI recipient counts and payment amounts, by county and category."""
    meta, rows = load_table(Path(input_dir) / "ssi_state_county_table_3.json")
    county_xw, city_xw = build_ansi_crosswalk(rows)
    person_measures = [m for m in meta["measures"] if m.startswith("persons_")]
    pay_measures = [m for m in meta["measures"] if m.startswith("payments_")]

    keys = ["year", "state_or_area", "join_key", *SSI_DIMS]
    ids = resolve_county_ids(rows, county_xw, city_xw)
    counts = _melt(
        rows,
        person_measures,
        ["persons_"],
        SSI_CATEGORIES,
        SSI_DIMS,
        "recipient_count",
        ids,
        COUNT_NOTES,
    )
    amounts = _melt(
        rows,
        pay_measures,
        ["payments_month_"],
        SSI_CATEGORIES,
        SSI_DIMS,
        "payment_amount_month",
        ids,
    )
    df = _merge_count_and_amount(counts, amounts, keys)
    df = df[df["county_or_city"] != STATE_TOTAL_LABEL]
    df = _drop_empty_duplicates(
        df,
        ["year", "state_or_area", "county_id", *SSI_DIMS],
        ["recipient_count", "payment_amount_month"],
    )
    df = _attach_state_id(df)
    df = df.drop(columns=["join_key"])
    df = df.rename(
        columns={
            "state_or_area": "state_name",
            "county_or_city": "county_name",
        }
    )
    return df[
        [
            "year",
            "state_id",
            "county_id",
            "state_name",
            "county_name",
            *SSI_DIMS,
            "recipient_count",
            "recipient_count_note",
            "payment_amount_month",
            "payment_amount_month_note",
        ]
    ].sort_values(["year", "state_id", "county_id", *SSI_DIMS], kind="stable")


def build_ssi_state(input_dir: Path) -> pd.DataFrame:
    """SSI recipient counts and payment amounts by state or other area."""
    meta1, rows1 = load_table(
        Path(input_dir) / "ssi_state_county_table_1.json"
    )
    meta2, rows2 = load_table(
        Path(input_dir) / "ssi_state_county_table_2.json"
    )
    keys = ["year", "state_or_area", *SSI_DIMS]
    counts = _melt(
        rows1,
        meta1["measures"],
        ["persons_"],
        SSI_CATEGORIES,
        SSI_DIMS,
        "recipient_count",
        notes=COUNT_NOTES,
    )
    amounts = _melt(
        rows2,
        meta2["measures"],
        ["payments_month_"],
        SSI_CATEGORIES,
        SSI_DIMS,
        "payment_amount_month",
    )
    drop = ["county_or_city", "county_id", "join_key"]
    counts = counts.drop(columns=drop, errors="ignore")
    amounts = amounts.drop(columns=drop, errors="ignore")
    df = _merge_count_and_amount(counts, amounts, keys)
    df["state_id"] = df["state_or_area"].map(STATE_FIPS)
    return df[
        [
            "year",
            "state_id",
            "state_or_area",
            *SSI_DIMS,
            "recipient_count",
            "recipient_count_note",
            "payment_amount_month",
            "payment_amount_month_note",
        ]
    ].sort_values(["year", "state_or_area", *SSI_DIMS], kind="stable")


# ---------------------------------------------------------------------------
# Reconciliation gate
# ---------------------------------------------------------------------------


class ReconciliationError(AssertionError):
    """A year failed to reconcile against SSA's own published totals."""


# SSA states that county values need not sum to the state total, nor state
# values to the national total, because of rounding and disclosure
# suppression.  So the gate is a tolerance -- and an asymmetric one, because
# the two directions mean different things.
#
# Measured over the full 1998-2025 archive:
#
#   direction              OASDI        SSI        cause
#   county sum > total     +0.022%      never      rounding of county values
#   county sum < total     -0.69%       -3.74%     rounding, plus suppression
#   state sum vs national  -0.0002%     -0.0002%   rounding only
#
# A shortfall is expected: a suppressed county contributes nothing to the sum.
# Hawaii has five counties and suppresses two, which alone accounts for the
# SSI worst case.  An *excess* has no benign explanation -- it means rows were
# duplicated or a state block was counted twice -- so that side is held tight.
MAX_POSITIVE_GAP = 0.005
MAX_SHORTFALL = 0.10
NATIONAL_SUM_TOLERANCE = 0.001


def reconcile_year(
    county: pd.DataFrame,
    state: pd.DataFrame,
    year: int,
    count_col: str,
    *,
    max_positive: float = MAX_POSITIVE_GAP,
    max_shortfall: float = MAX_SHORTFALL,
    national_tolerance: float = NATIONAL_SUM_TOLERANCE,
) -> dict:
    """Check one year's counties against SSA's state totals, and the states
    against SSA's published national total.

    This is the gate on accepting a newly published year.  A silent parse
    failure -- a shifted column, a dropped state block, a duplicated join, a
    decimal misread -- moves these ratios immediately, where a row count would
    not.  Raises :class:`ReconciliationError` on failure.
    """
    cat_col = (
        "benefit_type"
        if "benefit_type" in state.columns
        else "eligibility_category"
    )
    slice_col = "sex" if "sex" in state.columns else "oasdi_concurrent"

    def _total_slice(df: pd.DataFrame) -> pd.DataFrame:
        return df[
            (df["year"] == year)
            & (df[cat_col] == "total")
            & (df["age_group"] == "total")
            & (df[slice_col] == "total")
        ]

    published = _total_slice(state).set_index("state_or_area")[count_col]
    by_state = (
        _total_slice(county).groupby("state_name")[count_col].sum(min_count=1)
    )

    worst_state, worst_gap = None, 0.0
    for area, total in published.items():
        if area == NATIONAL_LABEL or pd.isna(total) or total == 0:
            continue
        got = by_state.get(area)
        # The District of Columbia and the territories publish no county detail.
        if got is None or pd.isna(got) or got == 0:
            continue
        gap = (got - total) / total
        if gap > max_positive:
            raise ReconciliationError(
                f"{year}: county sum for {area} EXCEEDS SSA's state total by "
                f"{gap:.3%} ({got:,.0f} vs {total:,.0f}); suppression can only "
                f"make the county sum too low, so this is a parse fault"
            )
        if abs(gap) > abs(worst_gap):
            worst_state, worst_gap = area, gap
    if -worst_gap > max_shortfall:
        raise ReconciliationError(
            f"{year}: county sum for {worst_state} falls {-worst_gap:.2%} short of "
            f"SSA's state total (limit {max_shortfall:.0%})"
        )

    national = published.get(NATIONAL_LABEL)
    nat_gap = None
    if national is not None and not pd.isna(national) and national:
        # "All areas" includes the territories and the residual "Other",
        # "Foreign countries" and "Unknown" rows, so sum every published area.
        states_sum = published.drop(index=NATIONAL_LABEL, errors="ignore").sum(
            min_count=1
        )
        nat_gap = abs(states_sum - national) / national
        if nat_gap > national_tolerance:
            raise ReconciliationError(
                f"{year}: the sum over areas ({states_sum:,.0f}) differs from SSA's "
                f"published national total ({national:,.0f}) by {nat_gap:.3%}, "
                f"above the {national_tolerance:.1%} tolerance"
            )
    return {
        "year": year,
        "worst_state": worst_state,
        "worst_state_gap": worst_gap,
        "national_gap": nat_gap,
    }


def reconcile_all(
    county: pd.DataFrame, state: pd.DataFrame, count_col: str
) -> pd.DataFrame:
    """Run :func:`reconcile_year` over every year present."""
    return pd.DataFrame(
        [
            reconcile_year(county, state, int(y), count_col)
            for y in sorted(state["year"].unique())
        ]
    )


# ---------------------------------------------------------------------------
# Dictionary
# ---------------------------------------------------------------------------

DICTIONARY_LABELS = {
    "benefit_type": {
        "total": ("Total", "Total", "Total"),
        "retired_worker": (
            "Aposentadoria: trabalhador aposentado",
            "Retirement: retired worker",
            "Jubilación: trabajador jubilado",
        ),
        "retired_spouse": (
            "Aposentadoria: cônjuge",
            "Retirement: spouse",
            "Jubilación: cónyuge",
        ),
        "retired_child": (
            "Aposentadoria: filho",
            "Retirement: child",
            "Jubilación: hijo",
        ),
        "survivor_widow_parent": (
            "Pensão por morte: viúvo(a) e pais",
            "Survivors: widow(er) and parent",
            "Sobrevivientes: viudo(a) y padres",
        ),
        "survivor_child": (
            "Pensão por morte: filho",
            "Survivors: child",
            "Sobrevivientes: hijo",
        ),
        "disabled_worker": (
            "Invalidez: trabalhador com deficiência",
            "Disability: disabled worker",
            "Discapacidad: trabajador con discapacidad",
        ),
        "disabled_spouse": (
            "Invalidez: cônjuge",
            "Disability: spouse",
            "Discapacidad: cónyuge",
        ),
        "disabled_child": (
            "Invalidez: filho",
            "Disability: child",
            "Discapacidad: hijo",
        ),
    },
    "age_group": {
        "total": ("Total", "Total", "Total"),
        "under_18": ("Menos de 18 anos", "Under 18", "Menos de 18 años"),
        "18_64": ("18 a 64 anos", "18 to 64", "18 a 64 años"),
        "65_or_older": ("65 anos ou mais", "65 or older", "65 años o más"),
    },
    "sex": {
        "total": ("Total", "Total", "Total"),
        "men": ("Homens", "Men", "Hombres"),
        "women": ("Mulheres", "Women", "Mujeres"),
    },
    "eligibility_category": {
        "total": ("Total", "Total", "Total"),
        "aged": ("Idoso", "Aged", "Anciano"),
        "blind_or_disabled": (
            "Cego ou com deficiência",
            "Blind or disabled",
            "Ciego o con discapacidad",
        ),
    },
    "oasdi_concurrent": {
        "total": ("Total", "Total", "Total"),
        "yes": (
            "Recebe também benefício do OASDI",
            "Also receiving OASDI benefits",
            "Recibe también beneficios del OASDI",
        ),
    },
    "population_group": {
        "total": ("População total", "Total population", "Población total"),
        "65_or_older": (
            "População de 65 anos ou mais",
            "Population aged 65 or older",
            "Población de 65 años o más",
        ),
    },
    "_note": {
        "suppressed_disclosure": (
            "Suprimido para evitar a divulgação de informações sobre indivíduos",
            "Suppressed to avoid disclosing information about particular individuals",
            "Suprimido para evitar la divulgación de información sobre individuos",
        ),
        "less_than_500_dollars": (
            "Menos de 500 dólares",
            "Less than $500",
            "Menos de 500 dólares",
        ),
        "not_available": (
            "Não disponível na fonte",
            "Not available in the source",
            "No disponible en la fuente",
        ),
    },
}

# Which coded column appears in which table.
DICTIONARY_SCOPE = {
    "oasdi_county": [
        "benefit_type",
        "age_group",
        "sex",
        "beneficiary_count_note",
        "benefit_amount_month_note",
    ],
    "oasdi_state": [
        "benefit_type",
        "age_group",
        "sex",
        "beneficiary_count_note",
        "benefit_amount_month_note",
    ],
    "oasdi_population_share": [
        "population_group",
        "population_note",
        "percentage_receiving_oasdi_note",
    ],
    "ssi_county": [
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
        "recipient_count_note",
        "payment_amount_month_note",
    ],
    "ssi_state": [
        "eligibility_category",
        "age_group",
        "oasdi_concurrent",
        "recipient_count_note",
        "payment_amount_month_note",
    ],
}


def build_dicionario(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Derive the dictionary from the built models, so it cannot drift.

    Only value/label pairs that actually occur in the data are emitted.
    """
    out = []
    for table, columns in DICTIONARY_SCOPE.items():
        df = tables.get(table)
        if df is None:
            continue
        for column in columns:
            labels = DICTIONARY_LABELS[
                "_note" if column.endswith("_note") else column
            ]
            present = {v for v in df[column].dropna().unique()}
            for key in labels:
                if key not in present:
                    continue
                pt, en, es = labels[key]
                out.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": pt,
                        "valor_en": en,
                        "valor_es": es,
                    }
                )
            unknown = present - set(labels)
            if unknown:
                raise KeyError(
                    f"{table}.{column}: undocumented values {sorted(unknown)}"
                )
    return pd.DataFrame(out).sort_values(
        ["id_tabela", "nome_coluna", "chave"], kind="stable"
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def read_architecture(
    table: str, architecture_dir: Path | str
) -> list[tuple[str, str]]:
    """Return [(column, bigquery_type)] in architecture order -- the source of truth."""
    arch = pd.read_csv(Path(architecture_dir) / f"{table}.csv", dtype=str)
    return list(zip(arch["name"], arch["bigquery_type"], strict=True))


def to_string_table(df: pd.DataFrame, spec: list[tuple[str, str]]) -> pa.Table:
    """Cast a frame to an all-STRING Arrow table in architecture column order.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s
    every column, so the parquet schema carries column *order*, not types.
    Two details are load-bearing:

    * cast through Arrow rather than ``astype(str)``, which renders NULL as the
      literal ``"nan"`` that ``safe_cast`` will not turn back into NULL;
    * pass the architecture's real type through first, so an INT64 ``year``
      serialises as ``"1999"`` and not ``"1999.0"``.
    """
    arrow_types = {
        "INT64": pa.int64(),
        "FLOAT64": pa.float64(),
        "STRING": pa.string(),
    }
    columns = {}
    for name, bq_type in spec:
        series = (
            df[name] if name in df.columns else pd.Series([None] * len(df))
        )
        target = arrow_types.get(bq_type, pa.string())
        if target is pa.string():
            arr = pa.array(
                series.astype(object).where(pd.notna(series), None),
                type=pa.string(),
            )
        else:
            arr = pa.array(pd.to_numeric(series, errors="coerce"), type=target)
            arr = arr.cast(pa.string())
        columns[name] = arr
    return pa.table(columns)


def write_partitioned(
    df: pd.DataFrame,
    table: str,
    output_dir: Path,
    architecture_dir: Path | str,
) -> int:
    """Write one table as Snappy parquet, hive-partitioned by year when present."""
    spec = read_architecture(table, architecture_dir)
    dest = Path(output_dir) / table
    dest.mkdir(parents=True, exist_ok=True)
    written = 0
    if "year" in df.columns:
        for year, chunk in df.groupby("year", sort=True):
            part = dest / f"year={year!s}"
            part.mkdir(parents=True, exist_ok=True)
            body = chunk.drop(columns=["year"])
            sub = [(n, t) for n, t in spec if n != "year"]
            pq.write_table(
                to_string_table(body, sub),
                part / "data.parquet",
                compression="snappy",
            )
            written += len(chunk)
    else:
        pq.write_table(
            to_string_table(df, spec),
            dest / "data.parquet",
            compression="snappy",
        )
        written = len(df)
    return written


BUILDERS = {
    "oasdi_county": build_oasdi_county,
    "oasdi_state": build_oasdi_state,
    "oasdi_population_share": build_oasdi_population_share,
    "ssi_county": build_ssi_county,
    "ssi_state": build_ssi_state,
}


def clean_all(
    input_dir: Path | str, architecture_dir: Path | str | None = None
) -> dict[str, pd.DataFrame]:
    """Build every table and run the reconciliation gate before returning.

    Raises :class:`ReconciliationError` if any year fails to reconcile against
    SSA's own published state and national totals, so a year that parsed wrong
    never reaches BigQuery.
    """
    input_dir = Path(input_dir)
    tables = {name: fn(input_dir) for name, fn in BUILDERS.items()}

    oasdi_report = reconcile_all(
        tables["oasdi_county"], tables["oasdi_state"], "beneficiary_count"
    )
    ssi_report = reconcile_all(
        tables["ssi_county"], tables["ssi_state"], "recipient_count"
    )
    log.info(
        "OASDI reconciliation: worst state gap %.4f%%, worst national gap %.5f%%",
        oasdi_report["worst_state_gap"].abs().max() * 100,
        (oasdi_report["national_gap"].max() or 0) * 100,
    )
    log.info(
        "SSI reconciliation: worst state gap %.4f%%, worst national gap %.5f%%",
        ssi_report["worst_state_gap"].abs().max() * 100,
        (ssi_report["national_gap"].max() or 0) * 100,
    )

    tables["dicionario"] = build_dicionario(tables)
    return tables
