"""Shared schema constants for the us_census_acs PUMS pipeline.

Single source of truth imported by parse_pums_dict, build_pums_architecture,
clean_pums and build_dicionario, so the identity-rename map cannot drift between
the scripts that must agree on it.
"""

# High-confidence pure identity renames (old -> canonical), applied before the
# column union so the old name never becomes a separate canonical column. Keep it
# minimal: do NOT add recoded vars (WKW->WKWN, REL->RELSHIPP) — those stay separate
# historical columns.
RENAME = {"ST": "STATE", "BDS": "BDSP", "RMS": "RMSP", "VAL": "VALP"}
