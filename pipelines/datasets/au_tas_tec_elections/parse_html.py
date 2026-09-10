"""Parsers for the TEC's two result-table markup families.

The TEC serves every result table in one of exactly two shapes, told apart by CSS
class and not by election, year or chamber:

``ha-table``
    House of Assembly. One small table per party group, listing candidates with a
    vote figure and — in the distribution view — a status of "Elected N",
    "Excluded" or blank for still continuing. Hare-Clark uses Robson Rotation, which
    reorders candidates on every ballot paper, so there is no ballot position to
    read and none is published.

``lc-table``
    Legislative Council. One wide matrix per contest: in the first-preference view
    rows are polling places and columns are candidates; in the distribution view
    rows are counts and columns are candidates.

Elections up to 2018 predate the ``w3-include-html`` injection the TEC now uses and
carry the same markup inline in the division page instead of in a sibling fragment.
The classes are identical, so both are parsed by the same code — only the file the
markup is read from differs.
"""

from __future__ import annotations

import html as html_mod
import re
from dataclasses import dataclass, field

TAG = re.compile(r"<[^>]+>")
COMMENT = re.compile(r"<!--.*?-->", re.S)
NUM = re.compile(r"^-?[\d,]+$")


def strip(fragment: str) -> str:
    """Tag-strip one cell's inner HTML to plain text."""
    text = html_mod.unescape(TAG.sub(" ", fragment))
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def to_int(text: str) -> int | None:
    # The source uses a real minus sign for negative transfers and an en dash for
    # "no value", neither of which is an ASCII hyphen.
    text = text.replace(",", "").replace("\u2212", "-").strip()
    if not text or text in {"-", "\u2013"}:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def to_float(text: str) -> float | None:
    text = text.replace(",", "").replace("%", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


SUBTOTAL = re.compile(r"^\s*(totals?\b|total ordinary|%\s*formal)", re.I)


def is_subtotal(name: str) -> bool:
    """Is this row/column a roll-up rather than a real voting centre?

    Both the House of Assembly workbooks and the Legislative Council tables
    interleave subtotals with the venues: a "Total Ordinary" line after the
    election-day places, then the special-vote categories, then a grand "Total".
    Summing everything without excluding them roughly doubles every candidate's
    vote, and it does so consistently enough to look like a units problem rather
    than a parsing one.
    """
    return bool(SUBTOTAL.match(name))


def rows_of(table_html: str) -> list[list[tuple[str, str]]]:
    """Split a table into rows of ``(class_attr, cell_text)`` pairs."""
    out = []
    for tr in re.findall(r"<tr\b.*?</tr>", table_html, flags=re.S | re.I):
        cells = []
        for m in re.finditer(
            r"<(td|th)\b([^>]*)>(.*?)</\1>", tr, flags=re.S | re.I
        ):
            attrs, inner = m.group(2), m.group(3)
            cls = ""
            cm = re.search(r'class="([^"]*)"', attrs)
            if cm:
                cls = cm.group(1)
            cells.append((cls, strip(inner)))
        if cells:
            out.append(cells)
    return out


# --------------------------------------------------------------------------------------
# House of Assembly
# --------------------------------------------------------------------------------------


@dataclass
class HaCandidate:
    ballot_name: str
    party_name: str
    votes: int | None
    status: str = ""
    # The leading bullet marks a sitting member. The current fragments give it no
    # label at all — the meaning is recoverable only from the 2018-era markup,
    # which names the same element ``class="alignleft sitting"``.
    is_sitting: bool = False


@dataclass
class HaResult:
    quota: int | None = None
    count_number: str = ""
    candidates: list[HaCandidate] = field(default_factory=list)
    group_totals: dict[str, int] = field(default_factory=dict)
    group_percentage: dict[str, float] = field(default_factory=dict)
    group_quotas: dict[str, float] = field(default_factory=dict)
    # The trailing "Summary" panel carries the contest's own totals.
    summary: dict[str, float] = field(default_factory=dict)


def parse_ha(html: str) -> HaResult:
    """Parse a House of Assembly first-preference or distribution table."""
    res = HaResult()
    # The quota sits outside the tables, and commented-out progressive copies of the
    # same label appear alongside the live one — strip comments before reading it.
    live = COMMENT.sub("", html)
    m = re.search(r"Quota\s*:?\s*(?:&nbsp;)?\s*([\d,]+)", live)
    if m:
        res.quota = to_int(m.group(1))
    m = re.search(r"Results after count\s*:?\s*(?:&nbsp;)?\s*(\d+)", live)
    if m:
        res.count_number = m.group(1)

    for block in re.findall(
        r'<div class="group">(.*?)</table>', live, flags=re.S | re.I
    ):
        gm = re.search(r"<h3([^>]*)>(.*?)</h3>", block, flags=re.S | re.I)
        party = strip(gm.group(2)) if gm else ""
        # The last two panels are not party groups: "Summary" holds the contest
        # totals (formal, informal, enrolment, turnout) and "Polling places
        # reported" is a progress counter. Reading them as candidates invents
        # rows named "Turnout".
        if gm and (
            "summary-heading" in gm.group(1)
            or party.lower() in {"summary", "polling places reported"}
        ):
            for cells in rows_of(block + "</table>"):
                label = next(
                    (t2 for c, t2 in cells if "candidate-name" in c), ""
                )
                figure = next((t2 for c, t2 in cells if "figure" in c), None)
                if label and figure:
                    val = to_float(figure)
                    if val is not None:
                        res.summary[label.strip().lower()] = val
            continue
        for cells in rows_of(block + "</table>"):
            labels = {c: t for c, t in cells}
            name = next(
                (
                    t
                    for c, t in cells
                    if "candidate-name" in c and "extra-padding" not in c
                ),
                None,
            )
            figure = next((t for c, t in cells if "figure" in c), None)
            if name and figure is not None and name:
                res.candidates.append(
                    HaCandidate(
                        ballot_name=name,
                        party_name=party,
                        votes=to_int(figure),
                        status=next(
                            (t for c, t in cells if "status" in c), ""
                        ),
                        is_sitting="\u2022"
                        in next((t for c, t in cells if "column-1" in c), ""),
                    )
                )
                continue
            # Trailing summary rows carry their label in an extra-padding cell.
            label = next(
                (t for c, t in cells if "extra-padding" in c), ""
            ).lower()
            if not label or figure is None:
                continue
            if label.startswith("group total"):
                val = to_int(figure)
                if val is not None:
                    res.group_totals[party] = val
            elif label.startswith("percentage"):
                val_f = to_float(figure)
                if val_f is not None:
                    res.group_percentage[party] = val_f
            elif label.startswith("quotas"):
                val_f = to_float(figure)
                if val_f is not None:
                    res.group_quotas[party] = val_f
        _ = labels
    return res


def split_status(status: str) -> tuple[str, str | None]:
    """Map the TEC's status label onto (candidate_status, election_order)."""
    s = status.strip()
    m = re.match(r"Elected\s*(\d+)?", s, flags=re.I)
    if m:
        return "elected", m.group(1)
    if s.lower().startswith("excluded"):
        return "excluded", None
    return "continuing", None


# --------------------------------------------------------------------------------------
# Legislative Council
# --------------------------------------------------------------------------------------


@dataclass
class LcCandidate:
    ballot_name: str
    party_name: str


@dataclass
class LcFirstPreferences:
    candidates: list[LcCandidate] = field(default_factory=list)
    # polling place -> (per-candidate votes, formal, informal, total)
    places: dict[
        str, tuple[list[int | None], int | None, int | None, int | None]
    ] = field(default_factory=dict)
    totals: (
        tuple[list[int | None], int | None, int | None, int | None] | None
    ) = None
    enrolment: int | None = None
    turnout: float | None = None


def _lc_header(
    cells: list[tuple[str, str]], raw_row: str
) -> list[LcCandidate]:
    out = []
    for m in re.finditer(
        r"<(th|td)\b[^>]*>(.*?)</\1>", raw_row, flags=re.S | re.I
    ):
        inner = m.group(2)
        nm = re.search(
            r'class="candidate-name">(.*?)</span>', inner, flags=re.S
        )
        if not nm:
            continue
        # The name cell is "SURNAME<br>Given"; the party is a sibling span.
        name = strip(nm.group(1).replace("<br>", ", ").replace("<br/>", ", "))
        pm = re.search(
            r'class="candidate-party">(.*?)</span>', inner, flags=re.S
        )
        out.append(LcCandidate(name, strip(pm.group(1)) if pm else ""))
    _ = cells
    return out


def parse_lc_first_preferences(html: str) -> LcFirstPreferences:
    res = LcFirstPreferences()
    live = COMMENT.sub("", html)
    m = re.search(r"Enrolled\s*:?\s*(?:&nbsp;)?\s*([\d,]+)", live)
    if m:
        res.enrolment = to_int(m.group(1))
    m = re.search(r"turnout\s*:?\s*(?:<[^>]*>\s*)*([\d.]+)\s*%", live)
    if m:
        res.turnout = to_float(m.group(1))

    # Inline-era pages hold BOTH tables, so the class must be discriminated:
    # ``lc-table-<division>`` is first preferences, ``lc-dist-table-<division>``
    # is the distribution. Taking the first ``lc-`` table parses the distribution
    # as if it were polling places, which silently yields one "polling place" per
    # count.
    table = re.search(
        r'<table class="lc-table-[^"]*">(.*?)</table>', live, flags=re.S | re.I
    )
    if not table:
        return res
    body = table.group(1)
    raw_rows = re.findall(r"<tr\b.*?</tr>", body, flags=re.S | re.I)
    for raw in raw_rows:
        cells = rows_of(raw)[0] if rows_of(raw) else []
        if not res.candidates:
            cand = _lc_header(cells, raw)
            if cand:
                res.candidates = cand
                continue
        first = cells[0][1] if cells else ""
        if 'class="pp-name"' in raw:
            label = first
        elif first.upper() == "TOTALS":
            label = "TOTALS"
        else:
            continue
        if label != "TOTALS" and is_subtotal(label):
            continue
        figures = [to_int(t) for c, t in cells if "figure" in c]
        n = len(res.candidates)
        if len(figures) < n + 3:
            continue
        payload = (figures[:n], figures[n], figures[n + 1], figures[n + 2])
        if label.upper() == "TOTALS":
            res.totals = payload
        else:
            res.places[label] = payload
    return res


def _remark(cells: list[tuple[str, str]], n_candidates: int) -> str:
    """Read the free-text Remarks cell, which has no figure class."""
    if len(cells) <= n_candidates + 1:
        return ""
    tail = cells[-1][1].strip()
    if not tail or NUM.match(tail.replace(" ", "")):
        return ""
    return tail


@dataclass
class LcCount:
    count_number: str
    transferred: list[int | None]
    totals: list[int | None]
    exhausted: int | None
    formal: int | None
    remarks: str


@dataclass
class LcDistribution:
    candidates: list[LcCandidate] = field(default_factory=list)
    counts: list[LcCount] = field(default_factory=list)


def parse_lc_distribution(html: str) -> LcDistribution:
    """Parse the Legislative Council count matrix.

    Rows alternate "Count N, votes transferred" and "Total votes"; the transferred
    row carries the remark naming who was excluded or elected. The two are folded
    into one record per count so the long output keeps both measures side by side.
    """
    res = LcDistribution()
    live = COMMENT.sub("", html)
    table = re.search(
        r'<table class="lc-dist-table[^"]*">(.*?)</table>',
        live,
        flags=re.S | re.I,
    )
    if not table:
        return res
    body = table.group(1)
    pending: LcCount | None = None
    last_number = ""
    for raw in re.findall(r"<tr\b.*?</tr>", body, flags=re.S | re.I):
        cells = rows_of(raw)
        cells = cells[0] if cells else []
        if not res.candidates:
            cand = _lc_header(cells, raw)
            if cand:
                res.candidates = cand
                continue
        # Two layouts. Modern rows put the whole label in one cell ("Count 2,
        # votes transferred" / "Total votes"). The 2018-era rows split it across
        # two cells: "Count 2" | "Votes transferred", then "" | "Total votes", so
        # the count number appears only on the transfer row and must be carried
        # forward to the total row that follows it.
        first = cells[0][1] if cells else ""
        second = cells[1][1] if len(cells) > 1 else ""
        figures = [to_int(x) for c, x in cells if "figure" in c]
        n = len(res.candidates)
        legacy = re.fullmatch(
            r"(Count\s+[\w\s]+|\s*)", first, re.I
        ) and re.match(r"(votes transferred|total votes)", second, re.I)
        if legacy:
            cm = re.match(r"Count\s+([\w\s]+)", first, re.I)
            number = cm.group(1).strip() if cm else last_number
            last_number = number
            if second.lower().startswith("votes transferred"):
                pending = LcCount(
                    count_number=number,
                    transferred=figures[:n],
                    totals=[],
                    exhausted=None,
                    formal=None,
                    remarks=_remark(cells, n + 1),
                )
                continue
            totals = figures[:n]
            if pending is None:
                # "Count 1 | Total votes" is the first-preference row: no
                # transfer precedes it, so the totals are also the transfers.
                pending = LcCount(
                    count_number=number,
                    transferred=totals,
                    totals=totals,
                    exhausted=None,
                    formal=None,
                    remarks=_remark(cells, n + 1),
                )
            else:
                pending.totals = totals
            pending.exhausted = figures[n] if len(figures) > n else None
            pending.formal = figures[n + 1] if len(figures) > n + 1 else None
            tail = _remark(cells, n + 1)
            if tail and tail != pending.remarks:
                pending.remarks = (
                    f"{pending.remarks}; {tail}" if pending.remarks else tail
                )
            res.counts.append(pending)
            pending = None
            continue

        label = first
        m = re.match(
            r"Count\s+([\w\s]+?),\s*(?:votes transferred|first)", label, re.I
        )
        if m and len(figures) >= n:
            pending = LcCount(
                count_number=m.group(1).strip(),
                transferred=figures[:n],
                totals=[],
                exhausted=None,
                formal=None,
                remarks=_remark(cells, n),
            )
            if "first preference" in label.lower():
                pending.totals = figures[:n]
                pending.exhausted = figures[n] if len(figures) > n else None
                pending.formal = (
                    figures[n + 1] if len(figures) > n + 1 else None
                )
                res.counts.append(pending)
                pending = None
            continue
        if re.search(r"total votes", label, re.I) and pending is not None:
            pending.totals = figures[:n]
            pending.exhausted = figures[n] if len(figures) > n else None
            pending.formal = figures[n + 1] if len(figures) > n + 1 else None
            tail = _remark(cells, n)
            if tail and tail != pending.remarks:
                pending.remarks = (
                    f"{pending.remarks}; {tail}" if pending.remarks else tail
                )
            res.counts.append(pending)
            pending = None
            continue
    if pending is not None:
        res.counts.append(pending)
    return res


# --------------------------------------------------------------------------------------
# House of Assembly, 2018 and earlier
# --------------------------------------------------------------------------------------

# The 2018 results are laid out in divs rather than a table, and their figures use a
# space as the thousands separator ("10 830"). The sitting-member bullet carries an
# explicit ``sitting`` class here, which is what identifies the same unlabelled
# bullet in the modern fragments.
_HA18_ROW = re.compile(
    r'<p class="alignleft sitting">(.*?)</p>\s*<p class="alignleft">(.*?)</p>\s*'
    r'<p class="alignright"><span>(.*?)</span>'
    r'(?:<span class\s*="elecexcl">(.*?)</span>)?',
    re.S | re.I,
)


def split_ha_2018(html: str) -> dict[str, str]:
    """Split a 2018-era division page into its two result views.

    The page carries "Distribution of preferences" and "First preferences" one
    after the other, with identical group markup. Parsing the page whole doubles
    every candidate.
    """
    live = COMMENT.sub("", html)
    parts = re.split(r"<note>\s*(.*?)\s*</note>", live)
    out: dict[str, str] = {}
    for i in range(1, len(parts) - 1, 2):
        title = strip(parts[i]).lower()
        if title.startswith("distribution of preferences"):
            out.setdefault("dist", parts[i + 1])
        elif title.startswith("first preferences by polling place"):
            out.setdefault("fp_by_place", parts[i + 1])
        elif title.startswith("first preferences"):
            out.setdefault("fp", parts[i + 1])
    return out


def parse_ha_2018(html: str) -> HaResult:
    res = HaResult()
    live = COMMENT.sub("", html)
    m = re.search(r"Quota\s*:?\s*<span>([\d\s,]+)</span>", live)
    if m:
        res.quota = to_int(m.group(1).replace(" ", ""))
    m = re.search(r"Results after count\s*<span><strong>(\d+)</strong>", live)
    if m:
        res.count_number = m.group(1)

    for block in re.findall(
        r'<div class="group"[^>]*>(.*?)(?=<div class="group"|\Z)',
        live,
        flags=re.S | re.I,
    ):
        gm = re.search(r"<h2[^>]*>(.*?)</h2>", block, flags=re.S | re.I)
        party = strip(gm.group(1)) if gm else ""
        if not party or party.lower() in {
            "summary",
            "polling places reported",
        }:
            continue
        for bullet, name, figure, status in _HA18_ROW.findall(block):
            clean_name = strip(name)
            if not clean_name:
                continue
            res.candidates.append(
                HaCandidate(
                    ballot_name=clean_name,
                    party_name=party,
                    votes=to_int(strip(figure).replace(" ", "")),
                    status=strip(status or ""),
                    is_sitting="\u2022" in html_mod.unescape(bullet),
                )
            )
        gt = re.search(
            r"Group Total\s*&nbsp;&nbsp;<span>([\d\s,]+)</span>", block, re.I
        )
        if gt:
            val = to_int(gt.group(1).replace(" ", ""))
            if val is not None:
                res.group_totals[party] = val
    return res
