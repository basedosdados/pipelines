"""Wage unit vocabulary and the annualisation factors applied to it.

The OFLC files report a wage amount next to a separate unit-of-pay field whose
vocabulary changes across years and programs: ``Year``, ``yr``, ``Y``, ``A``,
``Annual`` all mean the same thing, and ``Bi-Weekly`` means every two weeks, not
twice a week.

Both the source pair (amount + unit) and a derived annualised amount are
published. The annualised value is ``amount * FACTOR[unit]``; where the unit is
missing, blank, or not in this table the annualised value is **NULL** — never
guessed, and never silently defaulted to "yearly".

The hourly factor is 2,080 = 40 hours x 52 weeks, the convention DOL itself uses
in its wage determinations.
"""

# canonical unit -> annualisation factor. "piece rate" is a real unit in the
# H-2 data but is not a period, so it has no factor and never annualises.
FACTOR: dict[str, float] = {
    "hour": 2080.0,
    "day": 260.0,
    "week": 52.0,
    "bi-weekly": 26.0,
    "semi-monthly": 24.0,
    "month": 12.0,
    "year": 1.0,
}

# Tokens that stand for "no unit given" rather than for an unrecognised one.
# "Select Pay Range" is the unfilled default of the H-2 form's dropdown.
PLACEHOLDERS = frozenset(
    {
        "",
        "NA",
        "N/A",
        "NONE",
        "UNKNOWN",
        "-",
        "SELECT PAY RANGE",
        "SELECT",
    }
)

# raw source token (upper-cased, punctuation-stripped) -> canonical unit
UNIT_MAP: dict[str, str] = {}


def _add(canonical: str, *tokens: str) -> None:
    for t in tokens:
        UNIT_MAP[t] = canonical


_add("hour", "HOUR", "HOURLY", "HR", "H", "PER HOUR", "HOURLY WAGE")
_add("day", "DAY", "DAILY", "DAI", "DY", "PER DAY")
_add("week", "WEEK", "WEEKLY", "WK", "W", "PER WEEK")
_add(
    "bi-weekly",
    "BI-WEEKLY",
    "BIWEEKLY",
    "BI WEEKLY",
    "BI",
    "BW",
    "B",
    "EVERY TWO WEEKS",
)
_add(
    "semi-monthly",
    "SEMI-MONTHLY",
    "SEMIMONTHLY",
    "SEMI MONTHLY",
    "SM",
    "TWICE A MONTH",
)
_add("month", "MONTH", "MONTHLY", "MTH", "MO", "M", "PER MONTH")
_add(
    "year",
    "YEAR",
    "YEARLY",
    "YR",
    "Y",
    "A",
    "ANNUAL",
    "ANNUALLY",
    "PER YEAR",
    "SALARIED",
)
_add("piece rate", "PIECE RATE", "PIECE-RATE", "PIECERATE", "PC")


def normalise(raw: object) -> str | None:
    """Canonical unit for a raw source token, or None if unrecognised."""
    if raw is None:
        return None
    token = " ".join(str(raw).strip().upper().split())
    if not token or token in PLACEHOLDERS:
        return None
    return UNIT_MAP.get(token)


def annualise(amount: float | None, unit: str | None) -> float | None:
    """Annualised wage, or None when the amount or unit is unusable."""
    if amount is None or unit is None:
        return None
    factor = FACTOR.get(unit)
    if factor is None:
        return None
    return amount * factor


def is_placeholder(raw: object) -> bool:
    """Whether a raw token means "no unit given" rather than an unknown unit.

    Used to keep the cleaning report honest: a blank or an unfilled dropdown is
    not something the unit vocabulary is missing.
    """
    return " ".join(str(raw).strip().upper().split()) in PLACEHOLDERS
