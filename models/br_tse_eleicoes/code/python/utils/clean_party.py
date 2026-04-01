"""
Party name standardization (year-conditional mergers).
Equivalent of fnc/limpa_partido.do.
"""

# Year-specific renames (party mergers/rebrandings)
_YEAR_MAP: dict[int, dict[str, str]] = {
    2014: {
        "PTN": "PODE",
        "Pode": "PODE",
        "PEN": "PATRIOTA",
        "PT do B": "AVANTE",
    },
    2016: {
        "PRB": "REPUBLICANOS",
        "PMDB": "MDB",
        "PTN": "PODE",
        "Pode": "PODE",
        "PR": "PL",
        "PPS": "CIDADANIA",
        "PSDC": "DC",
        "PEN": "PATRIOTA",
        "PT do B": "AVANTE",
    },
    2018: {
        "PRB": "REPUBLICANOS",
        "PPS": "CIDADANIA",
        "PR": "PL",
    },
}

# Always-applied renames (after year-specific)
_ALWAYS_MAP = {
    "PATRI": "PATRIOTA",
    "SD": "SOLIDARIEDADE",
    "PCdoB": "PC do B",
    "PC DO B": "PC do B",
    "PTdoB": "PT do B",
    "PT DO B": "PT do B",
}


def clean_party(val: str, ano: int) -> str:
    if not val:
        return val
    # Year-specific
    year_map = _YEAR_MAP.get(ano, {})
    if val in year_map:
        val = year_map[val]
    # Always
    if val in _ALWAYS_MAP:
        val = _ALWAYS_MAP[val]
    return val


def clean_party_series(s, ano: int):
    """Apply clean_party to a pandas Series."""
    return s.map(lambda v: clean_party(v, ano) if isinstance(v, str) else v)
