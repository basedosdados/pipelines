"""Resolve higher education provider labels to a stable HEP code.

The two sources name providers differently, and the Finance Publication renames
them over its 17-year span (University of Ballarat became Federation University
in 2014 and Federation University Australia in 2017; the University of Western
Sydney became Western Sydney University in 2015). HERDC, by contrast, back-casts
every provider to its current name and carries the department's own HEP code.

HERDC is therefore the authority for provider identity, and this module maps
every label the Finance Publication has ever used onto that code. Matching is
done on a normalised form first; only labels that genuinely differ in wording
need an explicit alias below.
"""

from __future__ import annotations

import re

# Column headers in the finance workbooks that are aggregates rather than
# providers: state subtotals (present up to 2021), the national total, and the
# Table A/Table B group totals. These are dropped from the provider-level tables
# because they are recomputable by summing the providers.
STATE_GROUPS = {
    "New South Wales",
    "Victoria",
    "Queensland",
    "South Australia",
    "Western Australia",
    "Tasmania",
    "Northern Territory",
    "Australian Capital Territory",
    "Multi-State",
    "Multi State",
    "Other",
    "Table B Institutions",
    "Table A Institutions",
    "Total",
}

AGGREGATE_LABELS = STATE_GROUPS | {
    "All Institutions",
    "All Institutions (Table A and B)",
    "All Institutions (PUB2)",
    "Table A",
    "Table B",
}

# Finance Publication label -> HERDC HEP code, for the labels that do not
# normalise onto a HERDC provider name. Each entry is a wording difference or a
# rename, verified against the HERDC provider list.
ALIASES = {
    # Renames within the finance series.
    "university of ballarat": "2154",  # -> Federation University Australia
    "federation university": "2154",
    "university of western sydney": "3004",  # -> Western Sydney University
    # Wording differences between the two sources.
    "curtin university of technology": "2236",  # HERDC: Curtin University
    "rmit university": "3034",  # HERDC: Royal Melbourne Institute of Technology
    "torrens university": "4449",  # HERDC: Torrens University Australia
    "university of sunshine coast": "3043",
    "australian catholic university limited": "3006",
    "batchelor institute of indigenous tertiary education": "2246",
    "avondale college of higher education": "2252",
    "melbourne college of divinity": "4331",  # -> University of Divinity
    "melbourne university private": "3036",
}


def normalise(name: str) -> str:
    """Collapse a provider label to a comparable key.

    Drops the leading article, punctuation and case, all of which drift between
    and within the sources ("University of Technology, Sydney" versus
    "University of Technology Sydney").
    """
    key = name.strip().lower()
    key = re.sub(r"^the\s+", "", key)
    key = re.sub(r"[^a-z0-9]+", " ", key)
    key = re.sub(r"\bthe\b", " ", key)
    return re.sub(r"\s+", " ", key).strip()


class ProviderIndex:
    """Maps any provider label onto a HEP code, using HERDC as the authority."""

    def __init__(self, herdc_providers: dict[str, dict]):
        """herdc_providers: {hep_code: {"name": str, "state": str, "cohort": str, ...}}"""
        self.providers = herdc_providers
        self._by_key: dict[str, str] = {}
        for code, meta in herdc_providers.items():
            self._by_key[normalise(meta["name"])] = code
        for label, code in ALIASES.items():
            if code in herdc_providers:
                self._by_key.setdefault(normalise(label), code)
        self.unmatched: set[str] = set()

    def is_aggregate(self, label: str) -> bool:
        return label.strip() in AGGREGATE_LABELS or label.strip().startswith(
            "All Institutions"
        )

    def code(self, label: str) -> str | None:
        """HEP code for a provider label, or None if it does not resolve.

        Records misses so the caller can fail loudly rather than silently
        dropping a provider's entire history.
        """
        code = self._by_key.get(normalise(label))
        if code is None:
            self.unmatched.add(label)
        return code
