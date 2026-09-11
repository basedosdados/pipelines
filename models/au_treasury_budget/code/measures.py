"""Resolve a historical-data column heading to a measure, a sector and a unit.

The eleven historical tables carry a small closed vocabulary -- 31 measures across
four units -- but they express it three different ways:

* tables 1-8 put the unit in the column heading (``Receipts(b) | Per cent of GDP``);
* tables 9 and 10 put the unit in the caption (``... by institutional sector ($m)``)
  and use the column heading for the *sector* instead;
* table 11 puts the unit in the caption as prose (``on a real per capita basis``).

Resolution is deliberately strict. A heading that does not map to a known measure
raises rather than being dropped, so a measure Treasury adds in a future release
stops the build instead of vanishing from a table nobody re-reads.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Units, as they will appear in the ``value_*`` column names.
DOLLARS_MILLION = "dollars_million"
PERCENT_GDP = "percent_gdp"
PERCENT_REAL_GROWTH = "percent_real_growth"
DOLLARS_PER_PERSON = "dollars_per_person_real"

#: Unit tokens that may appear inside a column heading, longest first so that
#: "per cent real growth" is not swallowed by "per cent".
_UNIT_TOKENS: tuple[tuple[str, str | None], ...] = (
    (r"per cent real growth", PERCENT_REAL_GROWTH),
    (r"per cent of gdp", PERCENT_GDP),
    (r"\$m", DOLLARS_MILLION),
    # Structural words that qualify a column without naming a unit.
    (r"end of year", None),
)

#: Institutional sectors, as named in the headings of tables 9 and 10.
_SECTORS: tuple[tuple[str, str], ...] = (
    ("non-financial public sector", "non_financial_public_sector"),
    ("public non-financial corporations", "public_non_financial_corporations"),
    ("general government", "general_government"),
)

GENERAL_GOVERNMENT = "general_government"

#: Every measure heading these documents use, normalised to lower case with
#: footnote markers and punctuation removed. Extending this map is the intended
#: way to admit a new measure; leaving it unextended is what makes an unknown
#: heading an error.
_MEASURES: dict[str, str] = {
    "receipts": "receipts",
    "payments": "payments",
    "net future fund earnings": "net_future_fund_earnings",
    "underlying cash balance": "underlying_cash_balance",
    # The 2019-20 FBO wraps the heading so the word "balance" lands in a row the
    # column does not span.
    "underlying cash": "underlying_cash_balance",
    "net cash flows from investments in financial assets for policy purposes": (
        "net_cash_flows_investments_policy_purposes"
    ),
    "headline cash balance": "headline_cash_balance",
    "taxation receipts": "taxation_receipts",
    "non-taxation receipts": "non_taxation_receipts",
    "total receipts": "total_receipts",
    "net debt": "net_debt",
    "net interest payments": "net_interest_payments",
    "face value of ags on issue total ags on issue": "ags_on_issue_total",
    "face value of ags on issue subject to treasurers direction": (
        "ags_on_issue_subject_to_treasurers_direction"
    ),
    "interest paid": "interest_paid",
    "revenue": "revenue",
    "expenses": "expenses",
    "net operating balance": "net_operating_balance",
    "net capital investment": "net_capital_investment",
    "fiscal balance": "fiscal_balance",
    "net worth": "net_worth",
    "net financial worth": "net_financial_worth",
    "taxation revenue": "taxation_revenue",
    "non-taxation revenue": "non_taxation_revenue",
    "total revenue": "total_revenue",
    "cash surplus": "cash_surplus",
}

#: Headings that name only a group and never a value -- the blank spacer columns
#: Treasury inserts between measure blocks. Recognising them explicitly keeps them
#: out of the "unknown heading" error path.
_SPACER_HEADINGS = {
    "",
    "face value of ags on issue",
    "subject to treasurers direction",
    "face value of ags on issue subject to treasurers direction total ags on issue",
}

#: Documented header defects, keyed by ``(release_id, table_number, column)``.
#:
#: The 2020-21 FBO's Table B.3 declares an 11-column header grid over 10 data
#: columns, so from the fourth column onward each group name sits one position
#: right of the data it labels and two columns are left carrying a bare ``$m``.
#: The values themselves are correctly ordered and are verified arithmetically --
#: total receipts equals taxation plus non-taxation receipts on every row -- so
#: the repair restores a label rather than inventing a number.
HEADER_OVERRIDES: dict[tuple[str, int, int], str] = {
    ("fbo_2020_21", 3, 4): "non_taxation_receipts",
    ("fbo_2020_21", 3, 7): "total_receipts",
}


@dataclass(frozen=True)
class Column:
    """One resolved data column of a historical table."""

    index: int
    measure: str
    sector: str
    unit: str


def _clean(heading: str) -> str:
    text = heading.replace("|", " ")
    text = re.sub(r"\([a-z]\)", " ", text, flags=re.I)
    # The apostrophe in "Treasurer's Direction" is a curly quote in some
    # editions and a straight one in others.
    text = text.replace("’", "").replace("'", "")  # noqa: RUF001
    return re.sub(r"\s+", " ", text).strip()


def _split_unit(heading: str) -> tuple[str, str | None]:
    """Strip any unit token from a heading, returning ``(residual, unit)``."""
    text = _clean(heading)
    unit: str | None = None
    lowered = text.lower()
    for pattern, token in _UNIT_TOKENS:
        match = re.search(pattern, lowered)
        if match:
            if token and unit is None:
                unit = token
            text = text[: match.start()] + " " + text[match.end() :]
            lowered = text.lower()
    return re.sub(r"\s+", " ", text).strip(), unit


def _split_sector(residual: str) -> tuple[str, str]:
    lowered = residual.lower()
    for name, slug in _SECTORS:
        if lowered.startswith(name):
            return residual[len(name) :].strip(), slug
    return residual, GENERAL_GOVERNMENT


def caption_unit(caption: str) -> str | None:
    """Default unit for a table whose columns do not name one."""
    lowered = caption.lower()
    if "real per capita" in lowered:
        return DOLLARS_PER_PERSON
    if "($m)" in lowered or "$m" in lowered:
        return DOLLARS_MILLION
    return None


class UnknownHeadingError(ValueError):
    """A column heading that carries values but resolves to no known measure."""


def resolve_columns(
    *,
    release_id: str,
    table_number: int,
    caption: str,
    labels: list[str],
    has_values: list[bool],
) -> list[Column]:
    """Map each grid column to a measure, sector and unit.

    ``has_values`` says whether the column holds at least one number anywhere in
    the table. A column with no values is a spacer and is skipped; a column with
    values that resolves to nothing raises ``UnknownHeading``.
    """
    default_unit = caption_unit(caption)
    resolved: list[Column] = []
    for index, heading in enumerate(labels):
        if index == 0 or not has_values[index]:
            continue
        residual, unit = _split_unit(heading)
        residual, sector = _split_sector(residual)
        unit = unit or default_unit
        key = (release_id, table_number, index)
        if key in HEADER_OVERRIDES:
            measure = HEADER_OVERRIDES[key]
        else:
            measure = _MEASURES.get(residual.lower(), "")
        if not measure or not unit:
            raise UnknownHeadingError(
                f"{release_id} table {table_number} column {index}: heading "
                f"{heading!r} resolved to measure={measure!r} unit={unit!r}. "
                "Add it to _MEASURES, or to HEADER_OVERRIDES with an arithmetic "
                "check justifying the repair."
            )
        resolved.append(
            Column(index=index, measure=measure, sector=sector, unit=unit)
        )
    return resolved


def is_spacer(heading: str) -> bool:
    residual, _ = _split_unit(heading)
    residual, _ = _split_sector(residual)
    return residual.lower() in _SPACER_HEADINGS
