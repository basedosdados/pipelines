"""Pure functions for au_aph_hansard: enumerate, download and parse Hansard XML.

No Prefect imports here on purpose. The one-shot onboarding code under
``models/au_aph_hansard/code/`` imports these same functions, so the cleaning
transform lives in exactly one place.

The source ships **three** XML layouts, and a parser that assumes one will
silently drop the others:

===========  ==========================  ===================================
Era          Layout                      Where the words live
===========  ==========================  ===================================
1901-1980    lowercase, element-based    inside ``talk.start`` (``para``)
1981-1997    UPPERCASE, attribute-based  ``PARA`` under ``TALK.START``;
                                         speaker metadata on ``SPEECH`` attrs
1998-        lowercase, element-based    sibling ``talk.text``
===========  ==========================  ===================================

Source: ParlInfo (Parliament of Australia), CC BY-NC-ND 4.0.
"""

from __future__ import annotations

import csv
import io
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

from lxml import etree

from pipelines.datasets.au_aph_hansard.constants import constants

HOST = constants.PARLINFO_HOST.value
HEADERS = constants.HTTP_HEADERS.value
CHAMBERS = constants.CHAMBERS.value
MIRROR = constants.MIRROR_RAW_URL.value

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def http_get(url: str, timeout: int = 120, retries: int = 4) -> bytes:
    """GET with retry/backoff on transient failures only.

    A 4xx other than 429 is a settled answer - the server understood and
    refused - so retrying it cannot help and only multiplies load on the
    source. Retrying 403 four times turned a 490-probe run into 1,960 rejected
    requests against aph.gov.au.
    """
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500 and exc.code != 429:
                raise
            last = exc
        except Exception as exc:
            last = exc
        time.sleep(1.5 * (2**attempt))
    raise last  # type: ignore[misc]


def build_download_url(relative: str) -> str:
    """Turn a ParlInfo ``toc_unixml`` path into a fetchable absolute URL."""
    rel = urllib.parse.unquote(relative).split(";fileType")[0]
    if not rel.startswith("/"):
        rel = "/" + rel
    return HOST + urllib.parse.quote(rel, safe="/")


def build_mirror_url(house: str, year: str, filename: str) -> str:
    """URL for a sitting day in the 2016 GLAM Workbench mirror of ParlInfo.

    ParlInfo has since stopped serving most transcripts before 1998 - it answers
    with a "Missing File" page - so the mirror is the only working source for
    1901-1997. Verified 2026-09-02: the mirror is complete for every year the
    index lists, while ParlInfo returns a transcript for under 20% of pre-1998
    days.
    """
    return f"{MIRROR}/{house}/{year}/{filename}"


# ---------------------------------------------------------------------------
# XML loading
# ---------------------------------------------------------------------------


def _parser() -> etree.XMLParser:
    """Recovering parser.

    The 1981-1997 files declare a DTD with an internal subset and use
    ``&mdash;`` without defining it, both of which stop a strict parser dead.
    """
    return etree.XMLParser(
        recover=True,
        resolve_entities=False,
        load_dtd=False,
        no_network=True,
        huge_tree=True,
    )


def load_xml(payload: bytes):
    """Parse a Hansard payload, or return None when it is not a transcript."""
    try:
        root = etree.fromstring(payload, _parser())
    except etree.XMLSyntaxError:
        return None
    if root is None or not isinstance(root.tag, str):
        return None
    return root


def is_hansard_xml(payload: bytes) -> bool:
    """True only for a real Hansard transcript, in any of the three layouts.

    ParlInfo answers a request for an absent transcript with an HTML page
    titled "ParlInfo - Missing File" and **HTTP 200**, roughly 19 KB. Validating
    on the root element rather than status code or size is what keeps those
    pages out of the corpus.
    """
    head = payload[:400].lstrip()
    if head[:9].lower() == b"<!doctype" and b"html" in head[:60].lower():
        return False
    if head[:5].lower() == b"<html":
        return False
    root = load_xml(payload)
    return root is not None and root.tag.lower() == "hansard"


# ---------------------------------------------------------------------------
# Enumeration
# ---------------------------------------------------------------------------


def load_sitting_day_index() -> list[dict]:
    """Sitting days for 1901-2005, with keys house/year/date/url/file."""
    raw = http_get(constants.SITTING_DAY_INDEX_URL.value).decode(
        "utf-8", "ignore"
    )
    return list(csv.DictReader(io.StringIO(raw)))


class ProbeError(Exception):
    """ParlInfo could not be asked whether a chamber sat on a given day.

    Deliberately distinct from a None return. A refused request and "the
    chamber did not sit" are completely different answers, and collapsing them
    is how a run reports Completed after ingesting nothing: every probe 403s,
    each is read as a quiet no, and the flow concludes Parliament never sat.
    """


def find_sitting_day_xml(house: str, day: date) -> str | None:
    """Locate a sitting day's XML via a one-day ParlInfo search.

    Returns the relative ``toc_unixml`` path, or None when ParlInfo answered
    and the chamber did not sit. Raises ProbeError when ParlInfo could not be
    reached or refused the request - never None, so the caller cannot mistake a
    failure for a negative answer.
    """
    dataset = CHAMBERS[house]["modern"]
    stamp = f"{day.day:02d}%2F{day.month:02d}%2F{day.year}"
    query = f"Date%3A{stamp}%20>>%20{stamp}%20Dataset%3A{dataset}"
    url = (
        f"{HOST}/parlInfo/search/display/display.w3p;adv=yes;orderBy=date-eLast;"
        f"page=0;query={query};rec=0;resCount=100"
    )
    try:
        html = http_get(url).decode("utf-8", "ignore")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise ProbeError(f"{house} {day}: HTTP {exc.code}") from exc
    except Exception as exc:
        raise ProbeError(f"{house} {day}: {type(exc).__name__}") from exc
    match = re.search(r'href="([^"]*toc_unixml[^"]*)"', html)
    return match.group(1).split(";fileType")[0] if match else None


def daterange(start: date, end: date):
    """Yield every date from start to end inclusive."""
    step = timedelta(days=1)
    current = start
    while current <= end:
        yield current
        current += step


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

_WS = re.compile(r"\s+")
_ENTITY = re.compile(
    r"&(mdash|ndash|nbsp|amp|lt|gt|quot|apos|hellip|pound|deg);"
)
_ENTITY_MAP = {
    "mdash": "\u2014",
    "ndash": "\u2013",
    "nbsp": " ",
    "amp": "&",
    "lt": "<",
    "gt": ">",
    "quot": '"',
    "apos": "'",
    "hellip": "\u2026",
    "pound": "\u00a3",
    "deg": "\u00b0",
}


def _clean(text: str | None) -> str | None:
    """Collapse whitespace and resolve leftover entities; empty becomes None."""
    if text is None:
        return None
    out = _ENTITY.sub(lambda m: _ENTITY_MAP[m.group(1)], text)
    out = _WS.sub(" ", out).strip()
    return out or None


def _text_of(element, skip: set[str]) -> str:
    """Concatenate an element's text, skipping named child subtrees.

    Iterative rather than recursive on purpose. Some transcripts nest more than
    1,000 levels deep - a 1997 Senate file does - which overruns Python's
    recursion limit. The caller catches the error and drops the whole sitting
    day, so a recursive walk loses real data silently.
    """
    parts: list[str] = []
    stack: list[tuple[bool, object]] = [(False, element)]

    while stack:
        is_text, item = stack.pop()
        if is_text:
            parts.append(item)  # type: ignore[arg-type]
            continue
        node = item
        pending: list[tuple[bool, object]] = []
        if node.text:  # type: ignore[union-attr]
            pending.append((True, node.text))  # type: ignore[union-attr]
        for child in node:  # type: ignore[union-attr]
            if isinstance(child.tag, str) and child.tag.lower() not in skip:
                pending.append((False, child))
            if child.tail:
                pending.append((True, child.tail))
        stack.extend(reversed(pending))

    return " ".join(parts)


def _opens_utterance(element) -> bool:
    """Whether an element is, or contains, the start of another utterance.

    Checked structurally rather than against a list of tag names. The source
    wraps utterances in more elements than any fixed list captures - SPEECH,
    INTERJECT, CONTINUE, QUESTION, ANSWER, and in the 1991-1997 files a bare
    PARA - and each wrapper emits its own row. Absorbing one into a previous
    speaker's body therefore double-counts the text: before this check, a 1992
    sitting day parsed to 53.7x the text actually in the file, and the 1990s
    partitions were forty times the size of comparable 1980s ones.
    """
    if not isinstance(element.tag, str):
        return False
    if element.tag.lower() == "talk.start":
        return True
    return any(
        isinstance(node.tag, str) and node.tag.lower() == "talk.start"
        for node in element.iter()
    )


def _utterance_text(talk, parent) -> str | None:
    """Text of one utterance: the talk element plus its trailing siblings.

    A speaker's continuation paragraphs are captured; anything that opens
    another utterance ends the run, so no text is attributed to two speakers.
    """
    # Prune nested utterances at any depth: a TALK.START inside another
    # TALK.START emits its own row, so including its text here attributes the
    # same words to two speakers. The 1997 files nest them up to 371 levels
    # deep, which is what made that year parse to 9x its own text.
    parts = [_text_of(talk, skip={"talker", "talk.start"})]
    if parent is not None:
        children = [c for c in parent if isinstance(c.tag, str)]
        try:
            start = children.index(talk)
        except ValueError:
            start = len(children)
        for sibling in children[start + 1 :]:
            if _opens_utterance(sibling):
                break
            parts.append(_text_of(sibling, skip={"talk.start"}))
    return _clean(" ".join(parts))


_TIME_IN_BODY = re.compile(r"\(\s*(\d{1,2})[:.](\d{2})\s*\)\s*:")


def _time_from_body(body: str | None) -> str | None:
    """Recover a HH:MM stamp from the body prefix when the element is empty."""
    if not body:
        return None
    match = _TIME_IN_BODY.search(body[:400])
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def _flag(value: str | None) -> str | None:
    """Normalise the source's 0/1 flags, preserving blanks as NULL."""
    cleaned = _clean(value)
    return cleaned if cleaned in {"0", "1"} else None


def _iso_date(value: str | None) -> str | None:
    """Parse the attribute-era DATE, which uses both 4- and 2-digit years."""
    cleaned = _clean(value)
    if not cleaned:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
        # "04/02/97" must land in 1997, not 2097.
        if parsed.year > date.today().year:
            parsed = parsed.replace(year=parsed.year - 100)
        return parsed.isoformat()
    return None


def _row(**kwargs) -> dict:
    """Speech row with a fixed key order, so every era yields one schema."""
    keys = [
        "year",
        "date",
        "chamber",
        "parliament_number",
        "session_number",
        "period_number",
        "speech_order",
        "talk_type",
        "debate_type",
        "debate_title",
        "subdebate_title",
        "speaker_name",
        "speaker_id",
        "electorate",
        "party",
        "role",
        "in_government",
        "first_speech",
        "time_stamp",
        "page_number",
        "body",
        "word_count",
    ]
    return {key: kwargs.get(key) for key in keys}


# ---------------------------------------------------------------------------
# Parsers, one per layout
# ---------------------------------------------------------------------------


def _parse_lowercase(root, chamber: str) -> tuple[dict, list[dict]]:
    """1901-1980 and 1998-present: metadata in child elements."""
    header = root.find("session.header")

    def head(tag: str) -> str | None:
        if header is None:
            return None
        node = header.find(tag)
        return _clean(node.text) if node is not None else None

    sitting_date = _iso_date(head("date")) or head("date")
    rows: list[dict] = []

    for order, talk in enumerate(root.iter("talk.start"), start=1):
        talker = talk.find("talker")
        if talker is None:
            continue

        def field(tag: str, node=talker) -> str | None:
            found = node.find(tag)
            return _clean(found.text) if found is not None else None

        parent = talk.getparent()
        parent_tag = parent.tag if parent is not None else ""
        talk_type = {
            "interjection": "interjection",
            "continue": "continuation",
        }.get(parent_tag, "speech")

        body = _utterance_text(talk, parent)

        debate_title = subdebate_title = debate_type = None
        for anc in talk.iterancestors():
            if anc.tag == "subdebate.1" and subdebate_title is None:
                info = anc.find("subdebateinfo")
                if info is not None and info.find("title") is not None:
                    subdebate_title = _clean(info.find("title").text)
            elif anc.tag == "debate" and debate_title is None:
                info = anc.find("debateinfo")
                if info is not None:
                    if info.find("title") is not None:
                        debate_title = _clean(info.find("title").text)
                    if info.find("type") is not None:
                        debate_type = _clean(info.find("type").text)

        rows.append(
            _row(
                year=sitting_date[:4] if sitting_date else None,
                date=sitting_date,
                chamber=chamber,
                parliament_number=head("parliament.no"),
                session_number=head("session.no"),
                period_number=head("period.no"),
                speech_order=str(order),
                talk_type=talk_type,
                debate_type=debate_type,
                debate_title=debate_title,
                subdebate_title=subdebate_title,
                speaker_name=field("name"),
                speaker_id=field("name.id"),
                electorate=field("electorate"),
                party=field("party"),
                role=field("role"),
                in_government=_flag(field("in.gov")),
                first_speech=_flag(field("first.speech")),
                time_stamp=field("time.stamp") or _time_from_body(body),
                page_number=field("page.no"),
                body=body,
                word_count=str(len(body.split())) if body else "0",
            )
        )

    day = {
        "year": sitting_date[:4] if sitting_date else None,
        "date": sitting_date,
        "chamber": chamber,
        "parliament_number": head("parliament.no"),
        "session_number": head("session.no"),
        "period_number": head("period.no"),
        "is_proof": _flag(head("proof")),
        "speech_count": str(len(rows)),
        "page_count": head("page.no"),
    }
    return day, rows


def _parse_uppercase(root, chamber: str) -> tuple[dict, list[dict]]:
    """1981-1997: metadata on element attributes rather than child elements.

    Richer than the other two eras - the ``SPEECH`` element carries SPEAKER,
    NAMEID, PARTY, ELECTORATE, MINISTERIAL and GOV as attributes - so those are
    preferred, with the ``TALKER`` children as fallback (and the only source for
    interjections, which carry no attributes).
    """
    attrs = root.attrib
    sitting_date = _iso_date(attrs.get("DATE"))
    parliament_number = _clean(attrs.get("PARLIAMENT.NO"))
    session_number = _clean(attrs.get("SESSION.NO"))
    period_number = _clean(attrs.get("PERIOD.NO"))
    rows: list[dict] = []

    for order, talk in enumerate(root.iter("TALK.START"), start=1):
        parent = talk.getparent()
        parent_tag = parent.tag if parent is not None else ""
        talk_type = "interjection" if parent_tag == "INTERJECT" else "speech"

        talker = talk.find("TALKER")

        def child(tag: str, talker=talker) -> str | None:
            if talker is None:
                return None
            node = talker.find(tag)
            return _clean(node.text) if node is not None else None

        speech_attrs = parent.attrib if parent_tag == "SPEECH" else {}
        body = _utterance_text(talk, parent)

        debate_title = subdebate_title = debate_type = None
        for anc in talk.iterancestors():
            if anc.tag == "DEBATE.SUB1" and subdebate_title is None:
                node = anc.find("TITLE")
                subdebate_title = (
                    _clean(node.text) if node is not None else None
                )
            elif anc.tag == "DEBATE" and debate_title is None:
                node = anc.find("TITLE")
                debate_title = _clean(node.text) if node is not None else None
                debate_type = _clean(anc.attrib.get("TYPE"))

        rows.append(
            _row(
                year=sitting_date[:4] if sitting_date else None,
                date=sitting_date,
                chamber=chamber,
                parliament_number=parliament_number,
                session_number=session_number,
                period_number=period_number,
                speech_order=str(order),
                talk_type=talk_type,
                debate_type=debate_type,
                debate_title=debate_title,
                subdebate_title=subdebate_title,
                speaker_name=_clean(speech_attrs.get("SPEAKER"))
                or child("NAME"),
                speaker_id=_clean(speech_attrs.get("NAMEID")),
                electorate=_clean(speech_attrs.get("ELECTORATE"))
                or child("ELECTORATE"),
                party=_clean(speech_attrs.get("PARTY")),
                role=_clean(speech_attrs.get("MINISTERIAL")) or child("ROLE"),
                in_government=_flag(speech_attrs.get("GOV")),
                first_speech=None,
                time_stamp=_time_from_body(body),
                page_number=_clean(speech_attrs.get("PAGE")),
                body=body,
                word_count=str(len(body.split())) if body else "0",
            )
        )

    day = {
        "year": sitting_date[:4] if sitting_date else None,
        "date": sitting_date,
        "chamber": chamber,
        "parliament_number": parliament_number,
        "session_number": session_number,
        "period_number": period_number,
        "is_proof": "1"
        if (attrs.get("PROOF") or "").strip().lower() == "yes"
        else "0",
        "speech_count": str(len(rows)),
        "page_count": _clean(attrs.get("PAGE")),
    }
    return day, rows


_CARRIED_FIELDS = ("electorate", "party", "role")


def _fill_speaker_attributes(rows: list[dict]) -> int:
    """Complete each speaker's attributes from elsewhere in the same sitting day.

    The source states a member's electorate and party on the opening turn of a
    speech and then leaves them blank on that member's interjections and
    continuations. Left alone, ``electorate`` is populated on only about 10% of
    1901 rows and ``party`` on 13%, even though the transcript names the member
    every time.

    So for each ``speaker_id`` within the day, take the non-null value the
    source itself gives somewhere on that day and apply it to that member's
    other rows. This is completion, not imputation: a member's electorate and
    party cannot change within a sitting day, and the value is the source's own.

    Keyed on ``speaker_id`` rather than the enclosing speech on purpose — a
    ``continue`` element does not always belong to the member who opened the
    speech, so inheriting from the parent would attribute one member's party to
    another.
    """

    def key_of(row: dict) -> str | None:
        """Identify the speaker. Falls back to the exact name where the era
        gives no id - the 1981-1997 layout carries NAMEID on speeches but not
        on interjections. Exact-string only, since name forms vary across
        eras ("Mr SCHOLES" vs "Scholes The Hon G.G.D.") and loose matching
        would attribute one member's party to another."""
        return row.get("speaker_id") or row.get("speaker_name")

    known: dict[str, dict[str, str]] = {}
    for row in rows:
        speaker = key_of(row)
        if not speaker:
            continue
        seen = known.setdefault(speaker, {})
        for field in _CARRIED_FIELDS:
            if row.get(field) and field not in seen:
                seen[field] = row[field]

    filled = 0
    for row in rows:
        speaker = key_of(row)
        if not speaker or speaker not in known:
            continue
        for field, value in known[speaker].items():
            if not row.get(field):
                row[field] = value
                filled += 1
    return filled


def parse_sitting_day(
    xml_bytes: bytes, house: str, source_url: str
) -> tuple[dict, list[dict]]:
    """Parse one sitting-day XML file into a day row and its speech rows.

    One speech row per opening turn, interjection or continuation - a
    contiguous utterance by a single speaker. Dispatches on the root tag, which
    is the only reliable discriminator between the three layouts.
    """
    root = load_xml(xml_bytes)
    if root is None or root.tag.lower() != "hansard":
        raise ValueError("not a Hansard transcript")

    chamber = CHAMBERS[house]["chamber_name"]
    if root.tag == "HANSARD":
        day, rows = _parse_uppercase(root, chamber)
    else:
        day, rows = _parse_lowercase(root, chamber)
    _fill_speaker_attributes(rows)
    day["source_url"] = source_url
    return day, rows


# ---------------------------------------------------------------------------
# OpenAustralia mirror (recurring pipeline source)
# ---------------------------------------------------------------------------
#
# ParlInfo answers the Prefect worker with HTTP 403 on every request while
# serving the identical code and headers from an Australian connection, so the
# block is on the worker's egress IP rather than anything the code can change.
# OpenAustralia publishes a parsed mirror of the same Hansard, built for bulk
# access, covering 2006 onwards.
#
# It is a *fourth* layout, and a lossy one: the debate XML carries no
# parliament/session/period number, no page number, no debate type, and no
# in-government or first-speech flag. Electorate and party are recovered by
# joining the speaker id against OpenAustralia's own rosters.

_OA_DATE_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.xml$")


def list_openaustralia_days(house: str) -> dict[str, str]:
    """Map ISO date -> file URL for every sitting day the mirror publishes.

    One directory listing per chamber replaces the 490 single-day ParlInfo
    searches the previous implementation made per run.
    """
    directory = constants.OPENAUSTRALIA_DIRS.value[house]
    base = f"{constants.OPENAUSTRALIA_BASE.value}/{directory}/"
    html = http_get(base).decode("utf-8", "ignore")
    days: dict[str, str] = {}
    for href in re.findall(r'href="([^"?][^"]*\.xml)"', html):
        match = _OA_DATE_FILE.match(href)
        if match:
            days[match.group(1)] = base + href
    return days


def load_openaustralia_roster() -> dict[str, dict[str, str | None]]:
    """Speaker id -> electorate and party, from OpenAustralia's rosters.

    The debate XML names the speaker but not their seat or party. The rosters
    carry both, keyed by the same "member count" the speaker id encodes.

    ``party`` is the roster's *most recent* party, not the affiliation held on
    the day of the speech. For a pipeline appending the current year that is
    almost always the same thing, but it is not contemporaneous, and office
    holders are recorded by office (the Speaker shows as SPK, not their party).
    """
    roster: dict[str, dict[str, str | None]] = {}
    sources = (
        ("representatives.csv", 0),
        ("senators.csv", constants.OPENAUSTRALIA_SENATOR_ID_OFFSET.value),
    )
    for filename, offset in sources:
        url = f"{constants.OPENAUSTRALIA_ROSTER.value}/{filename}"
        text = http_get(url).decode("utf-8", "ignore")
        for row in csv.DictReader(io.StringIO(text)):
            raw_id = (row.get("member count") or "").strip()
            if not raw_id.isdigit():
                continue
            # Senators sit in the House's own division column only when they
            # have one; otherwise the state or territory is the constituency.
            seat = (row.get("Division") or "").strip() or (
                row.get("State/Territory") or ""
            ).strip()
            roster[str(int(raw_id) + offset)] = {
                "name": _clean(row.get("name")),
                "electorate": _clean(seat),
                "party": _clean(row.get("Most recent party")),
            }
    return roster


def parse_openaustralia_day(
    xml_bytes: bytes,
    house: str,
    source_url: str,
    roster: dict[str, dict[str, str | None]] | None = None,
) -> tuple[dict, list[dict]]:
    """Parse one OpenAustralia debate file into the dataset's row schema.

    The file is a flat sequence of headings and speeches in document order, so
    the current major and minor heading are carried forward as each speech is
    reached. Columns the mirror does not carry are left null rather than
    guessed.
    """
    root = load_xml(xml_bytes)
    if root is None or root.tag != "debates":
        raise ValueError("not an OpenAustralia debates file")

    roster = roster if roster is not None else {}
    chamber = CHAMBERS[house]["chamber_name"]
    sitting_date = None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", source_url)
    if match:
        sitting_date = match.group(1)

    rows: list[dict] = []
    debate_title = subdebate_title = None
    order = 0

    for element in root.iter():
        if not isinstance(element.tag, str):
            continue
        if element.tag == "major-heading":
            debate_title = _clean("".join(element.itertext()))
            subdebate_title = None
            continue
        if element.tag == "minor-heading":
            subdebate_title = _clean("".join(element.itertext()))
            continue
        if element.tag != "speech":
            continue

        order += 1
        speaker_id = (element.get("speakerid") or "").rsplit("/", 1)[
            -1
        ] or None
        person = roster.get(speaker_id or "", {})
        body = _clean("".join(element.itertext()))
        stamp = _clean(element.get("time"))
        if stamp and re.fullmatch(r"\d{1,2}:\d{2}", stamp):
            hour, minute = stamp.split(":")
            stamp = f"{int(hour):02d}:{minute}"
        else:
            stamp = _time_from_body(body)

        rows.append(
            _row(
                year=sitting_date[:4] if sitting_date else None,
                date=sitting_date,
                chamber=chamber,
                speech_order=str(order),
                talk_type=_clean(element.get("talktype")),
                # ParlInfo stores the same string in debate_type and
                # debate_title, and OpenAustralia's major-heading carries it
                # verbatim: on 2026-02-09 both give BILLS x51 and STATEMENTS BY
                # MEMBERS x48. So this column is not lost by the source switch.
                debate_type=debate_title,
                debate_title=debate_title,
                subdebate_title=subdebate_title,
                speaker_name=_clean(element.get("speakername")),
                speaker_id=speaker_id,
                electorate=person.get("electorate"),
                party=person.get("party"),
                time_stamp=stamp,
                body=body,
                word_count=str(len(body.split())) if body else "0",
            )
        )

    day = {
        "year": sitting_date[:4] if sitting_date else None,
        "date": sitting_date,
        "chamber": chamber,
        "parliament_number": None,
        "session_number": None,
        "period_number": None,
        "is_proof": None,
        "speech_count": str(len(rows)),
        "page_count": None,
        "source_url": source_url,
    }
    return day, rows


def parse_any(
    xml_bytes: bytes,
    house: str,
    source_url: str,
    roster: dict[str, dict[str, str | None]] | None = None,
) -> tuple[dict, list[dict]]:
    """Parse a transcript from either source, dispatching on the root element.

    ``hansard`` is ParlInfo in any of its three layouts; ``debates`` is the
    OpenAustralia mirror. Both yield the same row schema.
    """
    root = load_xml(xml_bytes)
    if root is None or not isinstance(root.tag, str):
        raise ValueError("not XML")
    if root.tag == "debates":
        return parse_openaustralia_day(xml_bytes, house, source_url, roster)
    return parse_sitting_day(xml_bytes, house, source_url)
