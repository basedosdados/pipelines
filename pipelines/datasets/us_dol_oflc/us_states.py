"""Normalise the OFLC state columns to USPS abbreviations.

The source mixes the two forms in one column, and the mix is not marginal: the
redesigned PERM form writes full names, so 42% of PERM rows carry
``"CALIFORNIA"`` where the older rows carry ``"CA"``. Left alone the column is
unjoinable and its own description ("USPS two-letter abbreviation") is false for
those rows.

Some values also carry a suffix — ``"CALIFORNIA CA"``, ``"TEXAS TEXAS"``,
``"OHIO Gerber Poultry"`` — so matching is on the longest recognised state-name
prefix, not on equality.

A value that is neither a known abbreviation nor a recognised name is **left
exactly as the source wrote it**. Guessing would be worse than an unjoinable
row, and the dbt referential test's tolerance is what surfaces the residue.

The target vocabulary is the 60 abbreviations in
``br_bd_diretorios_us.state``, which covers the 50 states, DC, the territories
and the freely associated states.
"""

from __future__ import annotations

# abbreviation -> canonical name, mirroring br_bd_diretorios_us.state
STATES: dict[str, str] = {
    "AK": "ALASKA",
    "AL": "ALABAMA",
    "AR": "ARKANSAS",
    "AS": "AMERICAN SAMOA",
    "AZ": "ARIZONA",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DC": "DISTRICT OF COLUMBIA",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "FM": "FEDERATED STATES OF MICRONESIA",
    "GA": "GEORGIA",
    "GU": "GUAM",
    "HI": "HAWAII",
    "IA": "IOWA",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "MA": "MASSACHUSETTS",
    "MD": "MARYLAND",
    "ME": "MAINE",
    "MH": "MARSHALL ISLANDS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MO": "MISSOURI",
    "MP": "NORTHERN MARIANA ISLANDS",
    "MS": "MISSISSIPPI",
    "MT": "MONTANA",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "NE": "NEBRASKA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NV": "NEVADA",
    "NY": "NEW YORK",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "PR": "PUERTO RICO",
    "PW": "PALAU",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UM": "U.S. MINOR OUTLYING ISLANDS",
    "UT": "UTAH",
    "VA": "VIRGINIA",
    "VI": "U.S. VIRGIN ISLANDS",
    "VT": "VERMONT",
    "WA": "WASHINGTON",
    "WI": "WISCONSIN",
    "WV": "WEST VIRGINIA",
    "WY": "WYOMING",
}

# Spellings the source uses that differ from the directory's canonical name.
_ALIASES = {
    "VIRGIN ISLANDS": "VI",
    "US VIRGIN ISLANDS": "VI",
    "U.S. VIRGIN ISLANDS": "VI",
    "MICRONESIA": "FM",
    "FEDERATED STATES OF MICRONESIA": "FM",
    "MINOR OUTLYING ISLANDS": "UM",
    "US MINOR OUTLYING ISLANDS": "UM",
    "U.S. MINOR OUTLYING ISLANDS": "UM",
    "WASHINGTON DC": "DC",
    "WASHINGTON D.C.": "DC",
    "DISTRICT OF COLUMBIA": "DC",
}

NAME_TO_ABBR: dict[str, str] = {name: abbr for abbr, name in STATES.items()}
NAME_TO_ABBR.update(_ALIASES)

# Longest first, so "NORTH CAROLINA" is tried before "NORTH ..." can mis-hit and
# "NEW YORK" before "NEW".
_NAMES_BY_LENGTH = sorted(NAME_TO_ABBR, key=len, reverse=True)


def normalise(raw: object) -> object:
    """USPS abbreviation for a source state value, or the value unchanged.

    Args:
        raw: The value exactly as the source wrote it.

    Returns:
        The two-letter abbreviation when the value is one, or begins with a
        recognised state name; otherwise ``raw`` untouched.
    """
    if raw is None:
        return None
    token = " ".join(str(raw).strip().upper().split())
    if not token:
        return raw
    if token in STATES:
        return token
    for name in _NAMES_BY_LENGTH:
        if token == name or token.startswith(name + " "):
            return NAME_TO_ABBR[name]
    return raw
