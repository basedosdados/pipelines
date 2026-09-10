"""Pure download + cleaning transform for au_qld_ecq_elections.

No Prefect imports: the one-shot onboarding bootstrap under
``models/au_qld_ecq_elections/code/`` imports these functions directly, so the
transform lives in exactly one place.

The Electoral Commission of Queensland publishes two unrelated bodies of data:

* **Results** — one zip of ``publicResults.xml`` per archive at
  ``resultsdata.elections.qld.gov.au``, plus a per-event ``electorates.json``
  and ``boundary_venues.json`` at ``results.elections.qld.gov.au``.
* **Disclosures** — the Electronic Disclosure System at
  ``disclosures.ecq.qld.gov.au``.

Six source defects shape this module; each is handled in the function named
below and none of them is cosmetic.

1. **45 archives serve 55 events, and one event is in two archives.** Election
   ``641`` (2025 Somerset) appears in both ``Mackay2025`` and ``Somerset2025``.
   The two copies are *not* duplicates: the Mackay copy is a 1,703-byte stub
   with zero voting centres, the Somerset copy is the real one. Deduplicating
   by "first archive seen" silently keeps the stub. ``choose_election_nodes``
   keeps the copy from the archive that ``elections.json`` *declares* for that
   event.
2. **Two incompatible local-government shapes.** Divided councils nest
   ``contest/districts/district/countRound``; undivided councils and every
   mayoral contest nest ``contest/countRound/districts/district``. Reading only
   the first shape drops 54,773 voting-centre result rows and 60,707
   preference-distribution rows. ``iter_contest_units`` and ``iter_booths``
   handle both.
3. **``votingSystem`` is null** for the two 2020 state by-elections and for all
   160 mayoral contests (mayors carry ``votingSystemMayor``). The contest master
   is therefore taken from ``electorates.json``, which is also richer on
   candidates, and the XML is used only for counts.
4. **``MASC23`` has no results archive** and is excluded explicitly.
5. **Preliminary and official first-preference counts differ** in all 93
   districts of the 2024 general election, so ``count_status`` is a real column
   and never a filter applied at load.
6. **``typeDescription`` is unreliable** — for some voting centres it repeats
   the centre's name instead of the type label. Only ``typeCode`` is published.
"""

from __future__ import annotations

import collections
import json
import re
import shutil
import zipfile
from collections.abc import Iterator
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import pyarrow as pa
import requests

from pipelines.datasets.au_qld_ecq_elections.constants import constants

# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------

_WS = re.compile(r"\s+")


def _norm(value: str | None) -> str:
    """Collapse whitespace and lowercase, for join keys."""
    return _WS.sub(" ", (value or "").strip()).lower()


def _text(node: ET.Element | None, tag: str) -> str | None:
    """Text of a direct child, or None."""
    if node is None:
        return None
    child = node.find(tag)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def _attr(node: ET.Element, name: str) -> str | None:
    """Value of an XML attribute, with blank treated as absent.

    ``ElementTree`` returns ``""`` for an attribute that is present but empty, and the
    ECQ venue block uses that for every unknown address component. Left as ``""`` those
    reach BigQuery as empty strings rather than NULL, which understates the null rate
    and — for ``state`` — breaks the directory foreign key, since ``""`` is not a state.
    """
    value = node.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _yesno(value: str | None) -> str | None:
    """ECQ writes YES/NO and JSON writes true/false; publish a single vocabulary."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "yes" if value else "no"
    lowered = str(value).strip().lower()
    if lowered in {"yes", "true", "y"}:
        return "yes"
    if lowered in {"no", "false", "n"}:
        return "no"
    return None


VOTING_SYSTEMS = {
    "compulsory preferential": "compulsory_preferential",
    "compulsory preferential voting": "compulsory_preferential",
    "optional preferential": "optional_preferential",
    "optional preferential voting": "optional_preferential",
    "first past the post": "first_past_the_post",
}

COUNT_STATUS = {
    "unofficial preliminary count": "preliminary_unofficial",
    "unofficial indicative count": "indicative_unofficial",
    "official first preference count": "first_preference_official",
    "official distribution of preferences count": "distribution_of_preferences_official",
}

CONTEST_TYPES = {
    "state": "state_district",
    "councillor": "councillor",
    "mayor": "mayor",
}


def _voting_system(raw: str | None) -> str | None:
    if not raw:
        return None
    key = _norm(raw)
    if key not in VOTING_SYSTEMS:
        raise ValueError(f"unknown voting system {raw!r}")
    return VOTING_SYSTEMS[key]


def _count_status(raw: str | None) -> str:
    key = _norm(raw)
    if key not in COUNT_STATUS:
        raise ValueError(f"unknown count round name {raw!r}")
    return COUNT_STATUS[key]


# --------------------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------------------


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": constants.USER_AGENT.value})
    return session


def download_results(input_dir: Path) -> Path:
    """Fetch elections.json, every per-event JSON companion and every results archive."""
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "json").mkdir(exist_ok=True)
    (input_dir / "zips").mkdir(exist_ok=True)
    session = _session()

    index_path = input_dir / "elections.json"
    if not index_path.exists():
        index_path.write_bytes(
            session.get(constants.ELECTIONS_INDEX.value, timeout=120).content
        )
    events = json.loads(index_path.read_text())["elections"]

    site = constants.RESULTS_SITE_URL.value
    for event in events:
        for key in ("electorates", "boundaryVenues"):
            name = event.get(key)
            if not name:
                continue
            target = input_dir / "json" / name
            if not target.exists():
                target.write_bytes(
                    session.get(f"{site}/data/{name}", timeout=120).content
                )
        url = event.get("archiveXML")
        if not url:
            continue
        target = input_dir / "zips" / url.rsplit("/", 1)[-1]
        if not target.exists():
            target.write_bytes(session.get(url, timeout=300).content)
    return index_path


def extract_results(input_dir: Path) -> None:
    """Unzip every results archive into ``xml/<archive-stem>/publicResults.xml``."""
    out = input_dir / "xml"
    out.mkdir(parents=True, exist_ok=True)
    for zip_path in sorted((input_dir / "zips").glob("*.zip")):
        target = out / zip_path.stem
        if (target / "publicResults.xml").exists():
            continue
        target.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(target)


def download_disclosures(input_dir: Path) -> None:
    """Fetch the Electronic Disclosure System exports that actually serve data.

    The ``/Report/<Type>Csv`` pattern advertised by the site's own JavaScript is dead
    code and 404s. ``/Report/<Type>/csv`` is a GET that needs no verification token but
    does need the full field set — a bare ``GET /Report/<Type>`` returns 500.
    """
    out = input_dir / "disclosures"
    out.mkdir(parents=True, exist_ok=True)
    session = _session()
    base = constants.DISCLOSURES_BASE_URL.value

    for name, (
        route,
        government_type,
    ) in constants.DISCLOSURE_MAP_EXPORTS.value.items():
        target = out / f"{name}.csv"
        if target.exists():
            continue
        response = session.post(
            f"{base}{route}",
            data={"GovernmentType": government_type},
            timeout=300,
        )
        response.raise_for_status()
        target.write_bytes(response.content)

    target = out / "expenditures.csv"
    if not target.exists():
        response = session.post(
            f"{base}{constants.DISCLOSURE_EXPENDITURE_EXPORT.value}",
            timeout=300,
        )
        response.raise_for_status()
        target.write_bytes(response.content)

    for report in constants.DISCLOSURE_REPORT_EXPORTS.value:
        target = out / f"report_{report}.csv"
        if target.exists():
            continue
        response = session.get(f"{base}/Report/{report}/csv", timeout=300)
        response.raise_for_status()
        target.write_bytes(response.content)


# --------------------------------------------------------------------------------------
# Election and contest master (electorates.json), plus archive selection
# --------------------------------------------------------------------------------------


def load_events(input_dir: Path) -> list[dict]:
    """Every event in elections.json that has a published results archive."""
    events = json.loads((input_dir / "elections.json").read_text())[
        "elections"
    ]
    excluded = set(constants.EXCLUDED_ELECTION_STUBS.value)
    kept = [
        e for e in events if e.get("archiveXML") and e["stub"] not in excluded
    ]
    dropped = [
        e["stub"] for e in events if e["stub"] not in {k["stub"] for k in kept}
    ]
    if sorted(dropped) != sorted(excluded):
        raise ValueError(
            f"unexpected events without an archive: {sorted(set(dropped) - excluded)}"
        )
    return kept


def choose_election_nodes(
    input_dir: Path, events: list[dict]
) -> dict[int, ET.Element]:
    """Map each event id to the ``<election>`` element from its *declaring* archive.

    Trap 1. Archives bundle several events and one event (2025 Somerset, XML id 641)
    also appears inside an archive that does not declare it, as a stub with no voting
    centres. Selecting by declaring archive is deterministic; selecting by first-seen
    is not, and silently drops real counts.
    """
    by_archive: dict[str, list[dict]] = collections.defaultdict(list)
    for event in events:
        stem = event["archiveXML"].rsplit("/", 1)[-1].removesuffix(".zip")
        by_archive[stem].append(event)

    chosen: dict[int, ET.Element] = {}
    for stem, declared in by_archive.items():
        path = input_dir / "xml" / stem / "publicResults.xml"
        root = ET.parse(path).getroot()
        for node in root.findall("election"):
            matches = [
                d
                for d in declared
                if d["electionDay"] == node.get("electionDay")
            ]
            if len(matches) > 1:
                # Same-day events in one archive: disambiguate on the event name. The
                # two spellings differ (elections.json drops a year prefix on Napranum
                # 2022 and doubles a space on Pormpuraaw 2026), so match on containment.
                name = _norm(node.get("electionName"))
                narrowed = [
                    d
                    for d in matches
                    if _norm(d["electionName"]) in name
                    or name in _norm(d["electionName"])
                ]
                matches = narrowed or matches
            if len(matches) == 1:
                chosen[matches[0]["id"]] = node

    missing = {e["id"] for e in events} - set(chosen)
    if missing:
        raise ValueError(
            f"no XML election node found for event ids {sorted(missing)}"
        )
    return chosen


def load_contests(input_dir: Path, events: list[dict]) -> dict[tuple, dict]:
    """Contest master from electorates.json, keyed (event id, contest type, name).

    Trap 3. electorates.json is the authority for contest identity, voting system and
    candidates; the XML's ``votingSystem`` is null for mayoral and some by-election
    contests, and its candidate records lack given names and party names.
    """
    register: dict[tuple, dict] = {}
    for event in events:
        payload = json.loads(
            (input_dir / "json" / event["electorates"]).read_text()
        )
        for row in payload["electorates"]:
            contest_type = CONTEST_TYPES[row["contestType"].lower()]
            key = (event["id"], contest_type, _norm(row["electorateName"]))
            if key in register:
                raise ValueError(f"duplicate contest key {key}")
            register[key] = {
                "event": event,
                "row": row,
                "contest_type": contest_type,
            }
    return register


def contest_candidates(row: dict) -> list[dict]:
    """The candidate array for a contest — three different keys by contest type."""
    if row["contestType"] == "State":
        return row.get("candidates") or []
    if row["contestType"] == "Mayor":
        return row.get("candidatesMayor") or []
    return row.get("candidatesCouncillor") or []


# --------------------------------------------------------------------------------------
# XML traversal
# --------------------------------------------------------------------------------------


class ContestUnit:
    """One seat race, and the XML node that holds its count rounds.

    A *seat race* is what a voter casts one ballot for. That is a state district, a
    division of a divided council, a whole undivided council's councillor contest, or a
    mayoralty. It is deliberately not the ``<contest>`` element, which spans several
    divisions in a divided council.
    """

    __slots__ = (
        "contest_type",
        "district_name",
        "lga_code",
        "lga_name",
        "node",
        "shape",
    )

    def __init__(
        self, node, contest_type, district_name, lga_name, lga_code, shape
    ):
        self.node = node
        self.contest_type = contest_type
        self.district_name = district_name
        self.lga_name = lga_name
        self.lga_code = lga_code
        self.shape = shape


def iter_contest_units(election: ET.Element) -> Iterator[ContestUnit]:
    """Yield every seat race in an election, across all three source shapes.

    Trap 2. Three shapes coexist:

    * ``election/districts/district``            — state elections, one district per seat
    * ``lga/contest/districts/district``         — divided council, one division per seat
    * ``lga/contest/countRound/districts``       — undivided council and every mayoralty,
                                                   where the whole contest is one seat and
                                                   the nested districts are reporting areas
    """
    districts = election.find("districts")
    if districts is not None:
        for district in districts.findall("district"):
            yield ContestUnit(
                district,
                "state_district",
                district.get("districtName"),
                None,
                None,
                "state",
            )

    for lga in election.findall("lga"):
        lga_name = lga.get("electorateName")
        lga_code = lga.get("areaCode")
        for contest in lga.findall("contest"):
            contest_type = CONTEST_TYPES[contest.get("contestType").lower()]
            inner = contest.find("districts")
            if inner is not None:
                for district in inner.findall("district"):
                    yield ContestUnit(
                        district,
                        contest_type,
                        district.get("districtName"),
                        lga_name,
                        lga_code,
                        "divided",
                    )
            else:
                yield ContestUnit(
                    contest,
                    contest_type,
                    lga_name,
                    lga_name,
                    lga_code,
                    "undivided",
                )


def iter_booths(
    count_round: ET.Element,
) -> Iterator[tuple[str | None, ET.Element]]:
    """Yield ``(reporting district name, booth)`` for a count round, in both shapes.

    Trap 2 again, and the expensive half: for undivided councils and mayoral contests
    the voting centres hang off ``countRound/districts/district/booths``, not off
    ``countRound/booths``. Reading only the direct child drops 54,773 result rows and
    60,707 preference-distribution rows — roughly a third of the voting-centre data.
    """
    direct = count_round.find("booths")
    if direct is not None:
        for booth in direct.findall("booth"):
            yield None, booth
    nested = count_round.find("districts")
    if nested is not None:
        for district in nested.findall("district"):
            booths = district.find("booths")
            if booths is None:
                continue
            for booth in booths.findall("booth"):
                yield district.get("districtName"), booth


def _candidate_rows(
    container: ET.Element | None, tag: str
) -> list[ET.Element]:
    if container is None:
        return []
    return container.findall(tag)


# --------------------------------------------------------------------------------------
# Directory crosswalks
# --------------------------------------------------------------------------------------

_LEGAL_FORMS = re.compile(
    r"\b(shire|regional|city|council|aboriginal|island|town|qld)\b"
)


def _lga_key(name: str) -> str:
    """Normalise an LGA name so ECQ's spelling and the ABS spelling agree.

    ECQ keeps the legal suffix ("Winton Shire", "Blackall-Tambo Regional"); the ABS
    directory drops it ("Winton", "Blackall Tambo") and adds a "(Qld)" disambiguator to
    two names that clash with interstate LGAs. Normalising both sides maps all 78 ECQ
    councils onto exactly one directory row each.
    """
    value = re.sub(r"\(.*?\)", " ", name.lower())
    value = _LEGAL_FORMS.sub(" ", value)
    return re.sub(r"[^a-z0-9]+", "", value)


def build_directory_crosswalk(
    cache: Path, billing_project: str = "basedosdados-dev"
) -> dict:
    """LGA-name and state-district-name lookups against ``br_bd_diretorios_au``.

    Cached to JSON so the transform is reproducible offline once fetched.
    """
    if cache.exists():
        return json.loads(cache.read_text())

    from google.cloud import bigquery

    client = bigquery.Client(project=billing_project)
    sed_vintage = constants.SED_VINTAGE.value
    lga_vintage = constants.LGA_VINTAGE.value

    sed_rows = client.query(
        "select id_state_electoral_division, name from "
        f"basedosdados.br_bd_diretorios_au.state_electoral_division_{sed_vintage} "
        "where abbreviation_state = 'QLD'"
    ).result()
    lga_rows = client.query(
        f"select id_lga, name from basedosdados.br_bd_diretorios_au.lga_{lga_vintage} "
        "where abbreviation_state = 'QLD'"
    ).result()

    lga: dict[str, str] = {}
    for lga_id, name in lga_rows:
        key = _lga_key(name)
        if key in lga:
            raise ValueError(f"ambiguous ABS LGA key {key!r}")
        lga[key] = lga_id

    payload = {
        "state_electoral_division": {
            _norm(name): sed_id for sed_id, name in sed_rows
        },
        "lga": lga,
    }
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(payload, indent=1, sort_keys=True))
    return payload


# --------------------------------------------------------------------------------------
# Results builders
# --------------------------------------------------------------------------------------


def _contest_identity(event, contest, unit, crosswalk) -> dict:
    """The 11-column identity block shared by every contest-grained results table."""
    row = contest["row"]
    contest_type = contest["contest_type"]
    government_level = "state" if contest_type == "state_district" else "local"

    if contest_type == "mayor":
        voting_system = row.get("votingSystemMayor")
    else:
        voting_system = row.get("votingSystem") or row.get(
            "votingSystemCouncillor"
        )

    lga_name = row.get("lgaName") or unit.lga_name
    if contest_type != "state_district" and not lga_name:
        lga_name = row["electorateName"]

    lga_id = None
    if lga_name:
        lga_id = crosswalk["lga"].get(_lga_key(lga_name))
        if lga_id is None:
            raise ValueError(f"no ABS LGA match for {lga_name!r}")

    sed_id = None
    if contest_type == "state_district":
        sed_id = crosswalk["state_electoral_division"].get(
            _norm(row["electorateName"])
        )

    return {
        "year": int(event["electionDay"][:4]),
        "election_id": str(event["id"]),
        "contest_id": str(row["electorateId"]),
        "lga_id": lga_id,
        "lga_code": str(row.get("areaCode") or unit.lga_code or "") or None,
        "state_electoral_division_id": sed_id,
        "government_level": government_level,
        "contest_type": contest_type,
        "voting_system": _voting_system(voting_system),
        "lga_name": lga_name,
        "district_name": row["electorateName"],
    }


def _party_lookup(contest) -> dict[str, tuple[str | None, str | None]]:
    """Ballot order number -> (party code, party name), from the contest master."""
    out = {}
    for candidate in contest_candidates(contest["row"]):
        out[str(candidate["ballotOrderNumber"])] = (
            candidate.get("partyCode"),
            candidate.get("party"),
        )
    return out


def build_results(input_dir: Path, crosswalk: dict) -> dict[str, pd.DataFrame]:
    """Every results table, from the XML archives and the JSON contest master."""
    events = load_events(input_dir)
    nodes = choose_election_nodes(input_dir, events)
    contests = load_contests(input_dir, events)
    events_by_id = {e["id"]: e for e in events}

    election_rows: list[dict] = []
    candidate_rows: list[dict] = []
    turnout_rows: list[dict] = []
    district_rows: list[dict] = []
    centre_rows: list[dict] = []
    preference_rows: list[dict] = []

    for event_id, election in nodes.items():
        event = events_by_id[event_id]
        election_type = event["electionType"]
        election_rows.append(
            {
                "year": int(event["electionDay"][:4]),
                "election_id": str(event["id"]),
                "election_stub": event["stub"],
                "election_name": event["electionName"].strip(),
                "election_type": election_type,
                "government_level": "state"
                if election_type.startswith("State")
                else "local",
                "election_date": event["electionDay"],
                "results_archive_url": event["archiveXML"],
            }
        )

        for unit in iter_contest_units(election):
            key = (event_id, unit.contest_type, _norm(unit.district_name))
            contest = contests.get(key)
            if contest is None:
                raise ValueError(
                    f"XML seat race with no contest master entry: {key}"
                )
            identity = _contest_identity(event, contest, unit, crosswalk)
            parties = _party_lookup(contest)

            # -- candidates ---------------------------------------------------------
            declared = {
                d.get("ballotOrderNumber")
                for d in unit.node.findall("declaredCandidate")
            }
            for candidate in contest_candidates(contest["row"]):
                order = str(candidate["ballotOrderNumber"])
                candidate_rows.append(
                    identity
                    | {
                        "ballot_order_number": order,
                        "ballot_name": candidate.get("ballotName"),
                        "candidate_surname": candidate.get("candidateSurname"),
                        "candidate_given_names": candidate.get(
                            "candidateGivenNames"
                        ),
                        "party_code": candidate.get("partyCode"),
                        "party_name": candidate.get("party"),
                        "is_declared_elected": "yes"
                        if order in declared
                        else "no",
                    }
                )

            count_rounds = unit.node.findall("countRound")

            # Trap: 64 contests were decided without a count. They carry no countRound
            # at all, so they would vanish from the contest register entirely.
            if not count_rounds:
                turnout_rows.append(
                    identity
                    | {
                        "count_status": "declared_unopposed",
                        "count_round_number": None,
                        "number_to_elect": _int(
                            contest["row"].get("numberToElect")
                        ),
                        "candidates_count": len(
                            contest_candidates(contest["row"])
                        ),
                        "enrolment": _int(contest["row"].get("enrolment")),
                        "votes_total": None,
                        "votes_formal": None,
                        "votes_informal": None,
                        "percentage_formal": None,
                        "percentage_informal": None,
                        "percentage_roll_counted": None,
                        "voting_method": contest["row"].get("votingMethod"),
                        "is_final": "yes",
                        "last_updated": None,
                    }
                )

            for count_round in count_rounds:
                status = _count_status(count_round.get("countName"))
                _emit_count_round(
                    identity,
                    parties,
                    unit,
                    contest,
                    count_round,
                    status,
                    turnout_rows,
                    district_rows,
                    centre_rows,
                    preference_rows,
                )

    frames = {
        "election": pd.DataFrame(election_rows),
        "candidate": pd.DataFrame(candidate_rows),
        "enrolment_turnout": pd.DataFrame(turnout_rows),
        "result_district": pd.DataFrame(district_rows),
        "result_voting_centre": pd.DataFrame(centre_rows),
        "distribution_of_preferences": pd.DataFrame(preference_rows),
        "voting_centre": build_voting_centres(
            input_dir, events, nodes, crosswalk
        ),
    }
    return frames


def _emit_count_round(
    identity,
    parties,
    unit,
    contest,
    count_round,
    status,
    turnout_rows,
    district_rows,
    centre_rows,
    preference_rows,
) -> None:
    """Expand one count round into turnout, district, voting-centre and preference rows."""
    formal = count_round.find("totalFormalVotes")
    informal = count_round.find("totalInformalVotes")
    node = unit.node

    turnout_rows.append(
        identity
        | {
            "count_status": status,
            "count_round_number": count_round.get("round"),
            "number_to_elect": _int(contest["row"].get("numberToElect")),
            "candidates_count": len(contest_candidates(contest["row"])),
            "enrolment": _int(
                node.get("enrolment") or contest["row"].get("enrolment")
            ),
            "votes_total": _int(_text(count_round, "totalVotes")),
            "votes_formal": _int(_text(formal, "count")),
            "votes_informal": _int(_text(informal, "count")),
            "percentage_formal": _float(_text(formal, "percentage")),
            "percentage_informal": _float(_text(informal, "percentage")),
            "percentage_roll_counted": _float(node.get("percentRollCounted")),
            "voting_method": node.get("votingMethod")
            or contest["row"].get("votingMethod"),
            "is_final": _yesno(node.get("final")),
            "last_updated": count_round.get("lastUpdated")
            or node.get("lastUpdated"),
        }
    )

    # -- contest-level candidate results ------------------------------------------------
    for tag, count_type in (
        ("primaryVoteResults", "first_preference"),
        ("twoCandidateVotes", "two_candidate_preferred"),
    ):
        for candidate in _candidate_rows(count_round.find(tag), "candidate"):
            order = str(candidate.get("ballotOrderNumber"))
            party_code, party_name = parties.get(order, (None, None))
            district_rows.append(
                identity
                | {
                    "count_status": status,
                    "count_type": count_type,
                    "ballot_order_number": order,
                    "ballot_name": candidate.get("ballotName"),
                    "party_code": party_code,
                    "party_name": party_name,
                    "votes": _int(_text(candidate, "count")),
                    "percentage": _float(_text(candidate, "percentage")),
                }
            )

    # -- contest-level distribution of preferences --------------------------------------
    summary = count_round.find("preferenceDistributionSummary")
    if summary is not None:
        for distribution in summary.findall("preferenceDistribution"):
            exhausted = distribution.find("exhausted")
            totals = {
                "votes_distributed": _int(
                    _text(distribution, "votesDistributed")
                ),
                "votes_exhausted": _int(_text(exhausted, "count")),
                "percentage_exhausted": _float(_text(exhausted, "percentage")),
                "votes_remaining_in_count": _int(
                    _text(distribution, "votesRemainingInCount")
                ),
            }
            for receiving in distribution.findall("candidatePreferences"):
                order = str(receiving.get("ballotOrderNumber"))
                party_code, party_name = parties.get(order, (None, None))
                preference_rows.append(
                    identity
                    | {
                        "count_status": status,
                        "distribution_number": distribution.get(
                            "distribution"
                        ),
                        "excluded_ballot_order_number": distribution.get(
                            "excludedBallotOrder"
                        ),
                        "ballot_order_number": order,
                        "excluded_ballot_name": distribution.get(
                            "excludedBallotName"
                        ),
                        "ballot_name": receiving.get("ballotName"),
                        "party_code": party_code,
                        "party_name": party_name,
                        "votes_transferred": _int(_text(receiving, "count")),
                        "percentage_transferred": _float(
                            _text(receiving, "percentage")
                        ),
                    }
                    | totals
                )

    # -- voting-centre results ----------------------------------------------------------
    for reporting_district, booth in iter_booths(count_round):
        centre_identity = identity | {
            "voting_centre_id": _attr(booth, "id"),
            "count_status": status,
            "voting_centre_name": _attr(booth, "name"),
            # typeDescription is unreliable (it repeats the centre name for some
            # booths), so only the code is published; labels live in dicionario.
            "voting_centre_type_code": _attr(booth, "typeCode"),
            "voting_centre_district_name": reporting_district
            or identity["district_name"],
            "votes_total": _int(_text(booth, "ballots")),
            "votes_formal": _int(_text(booth, "formalVotes")),
            "votes_informal": _int(_text(booth, "informalVotes")),
        }
        for tag, count_type in (
            ("primaryVoteResults", "first_preference"),
            ("twoCandidateVotes", "two_candidate_preferred"),
        ):
            for candidate in _candidate_rows(booth.find(tag), "candidate"):
                order = str(candidate.get("ballotOrderNumber"))
                party_code, party_name = parties.get(order, (None, None))
                centre_rows.append(
                    centre_identity
                    | {
                        "count_type": count_type,
                        "ballot_order_number": order,
                        "ballot_name": candidate.get("ballotName"),
                        "party_code": party_code,
                        "party_name": party_name,
                        "votes": _int(_text(candidate, "count")),
                        "percentage": _float(_text(candidate, "percentage")),
                    }
                )


def build_voting_centres(
    input_dir: Path,
    events: list[dict],
    nodes: dict[int, ET.Element],
    crosswalk: dict,
) -> pd.DataFrame:
    """One row per event per voting centre per district served.

    The ``<venues>`` block sits at the archive root and covers every event in that
    archive, so the same centre is emitted once per archive it appears in. Rows are
    deduplicated on (event, centre, district) — the natural grain — which removes the
    471 duplicates created by the seven multi-event archives.
    """
    # The venue block identifies its election by NAME only, and the XML spelling does
    # not always match elections.json: the 2022 Napranum by-election carries a year
    # prefix in the XML that elections.json omits, and the 2026 Pormpuraaw by-election
    # has a doubled space in elections.json. Keying off the XML nodes already selected
    # by choose_election_nodes uses the XML's own spelling and cannot drift. Matching on
    # the elections.json spelling instead silently loses that event's voting centres.
    events_by_id = {e["id"]: e for e in events}
    name_to_event: dict[str, dict] = {}
    for event_id, node in nodes.items():
        key = _norm(node.get("electionName"))
        if key in name_to_event and name_to_event[key]["id"] != event_id:
            raise ValueError(f"two events share the XML election name {key!r}")
        name_to_event[key] = events_by_id[event_id]
    for event in events:
        name_to_event.setdefault(_norm(event["electionName"]), event)
    seds = crosswalk["state_electoral_division"]

    rows: list[dict] = []
    for path in sorted((input_dir / "xml").glob("*/publicResults.xml")):
        venues = ET.parse(path).getroot().find("venues")
        if venues is None:
            continue
        for booth in venues.findall("booth"):
            for served in booth.findall("boothDistrict"):
                event = name_to_event.get(_norm(served.get("election")))
                if event is None:
                    # The venue block also names events the archive does not declare
                    # (and MASC23, which is excluded); skip rather than invent an id.
                    continue
                district_name = _attr(served, "districtName")
                rows.append(
                    {
                        "year": int(event["electionDay"][:4]),
                        "election_id": str(event["id"]),
                        "voting_centre_id": _attr(booth, "id"),
                        "state_electoral_division_id": seds.get(
                            _norm(district_name)
                        ),
                        "district_name": district_name,
                        "voting_centre_name": _attr(booth, "name"),
                        "building_name": _attr(booth, "buildingName"),
                        "street_number": _attr(booth, "streetNo"),
                        "street_name": _attr(booth, "streetName"),
                        "locality": _attr(booth, "locality"),
                        "postcode": _attr(booth, "postcode"),
                        "state_abbreviation": _attr(booth, "state"),
                        "latitude": _float(booth.get("latitude")),
                        "longitude": _float(booth.get("longitude")),
                        "joint_type": _attr(served, "jointType"),
                        "is_abolished": _yesno(booth.get("abolished")),
                    }
                )
    frame = pd.DataFrame(rows)
    return frame.drop_duplicates(
        subset=["election_id", "voting_centre_id", "district_name"]
    ).reset_index(drop=True)


# --------------------------------------------------------------------------------------
# Disclosure builders
# --------------------------------------------------------------------------------------


def _read_csv(path: Path) -> pd.DataFrame:
    """Read an Electronic Disclosure System export as all-text, blanks as empty string.

    ``expenditures.csv`` carries embedded newlines inside free-text fields (27,323
    physical lines for 27,225 logical rows), so it must never be line-counted.
    """
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def _blank_to_none(series: pd.Series) -> pd.Series:
    stripped = series.fillna("").astype(str).str.strip()
    return stripped.where(stripped != "", other=None)


def build_disclosure_gift(input_dir: Path) -> pd.DataFrame:
    """Gifts, from the two map exports rather than the Gifts report.

    ``report_Gifts.csv`` is multiset-identical to the two map exports on
    (date, donor, recipient, amount) — 28,328 rows either way — but it drops
    ``Political donation``, ``Electoral committee`` and ``Name of electoral committee``.
    More importantly **no column encodes government level**, so the only way to recover
    it is which export the row came from. That is why the map exports are authoritative.
    """
    disclosures = input_dir / "disclosures"
    frames = []
    for name, level in (
        ("map_gifts_state", "state"),
        ("map_gifts_local", "local"),
    ):
        frame = _read_csv(disclosures / f"{name}.csv")
        frame["government_level"] = level
        frames.append(frame)
    raw = pd.concat(frames, ignore_index=True)

    date = pd.to_datetime(
        _blank_to_none(raw["Date Gift Made"]),
        format=constants.DATE_FORMAT_GIFT.value,
        errors="raise",
    )
    out = pd.DataFrame(
        {
            "year": date.dt.year.astype("Int64"),
            "government_level": raw["government_level"],
            "date_gift_made": date.dt.date,
            "donor_name": _blank_to_none(raw["Donor"]),
            "recipient_name": _blank_to_none(raw["Recipient"]),
            "gift_value": pd.to_numeric(raw["Gift value"], errors="coerce"),
            "election_name": _blank_to_none(raw["Election"]),
            # '-' is a real third state (not applicable), not a blank: the field is
            # state-only, so every local row carries it. Kept verbatim as a code.
            "is_political_donation": _blank_to_none(raw["Political donation"]),
            "has_electoral_committee": _blank_to_none(
                raw["Electoral committee"]
            ),
            "electoral_committee_name": _blank_to_none(
                raw["Name of electoral committee"]
            ),
        }
    )
    return out


def build_disclosure_expenditure(input_dir: Path) -> pd.DataFrame:
    """Expenditure, from ``/Expenditures/ExportCsv``.

    ``report_Expenditure.csv`` has the same 27,225 rows but Candidate Type, Local
    Electorate, Election, Description of Goods or Services and Purpose of the
    Expenditure are 100% empty in it — five columns silently lost.
    """
    raw = _read_csv(input_dir / "disclosures" / "expenditures.csv")
    date = pd.to_datetime(
        raw["Date Incurred"],
        format=constants.DATE_FORMAT_EXPENDITURE.value,
        errors="raise",
    )
    year = date.dt.year

    # Two rows are dated 1924 and are unambiguous typos for 2024: both are small
    # consumer purchases attached to the 2024 Local Government Elections. Repaired so
    # the partition range is not dragged back a century; recorded in the metadata.
    repairs = constants.EXPENDITURE_YEAR_REPAIRS.value
    for wrong, right in repairs.items():
        mask = year == wrong
        if mask.any():
            date = date.mask(
                mask, date + pd.offsets.DateOffset(years=right - wrong)
            )
    year = date.dt.year

    if year.min() < min(repairs.values()) - 100:
        raise ValueError(
            f"unrepaired out-of-range expenditure year {year.min()}"
        )

    return pd.DataFrame(
        {
            "year": year.astype("Int64"),
            "date_incurred": date.dt.date,
            "incurred_by_name": _blank_to_none(raw["Incurred By"]),
            "expenditure_value": pd.to_numeric(raw["Value"], errors="coerce"),
            "candidate_type": _blank_to_none(raw["Candidate Type"]),
            "local_electorate_name": _blank_to_none(raw["Local Electorate"]),
            "election_name": _blank_to_none(raw["Election"]),
            "goods_or_services_description": _blank_to_none(
                raw["Description of Goods or Services"]
            ),
            "expenditure_purpose": _blank_to_none(
                raw["Purpose of the Expenditure"]
            ),
        }
    )


_PERIOD = re.compile(r"^(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})\s*(.*)$")


def build_disclosure_return(input_dir: Path) -> pd.DataFrame:
    """Periodic returns — the one clean return export.

    Unique on (For, Period) at 293/293, and every ``Period`` parses into a start/end
    pair across 20 half-year windows from 2016H2 to 2026H1.

    ``report_ElectionSummaries.csv`` is deliberately NOT merged in here. Its ``Period``
    concatenates an optional date with an event name in three inconsistent shapes, so
    253 distinct strings collapse to 60 event names and 8 names appear both with and
    without a date prefix — including "2024 State General Election". Grouping on it
    fragments the same election across up to four spellings.
    """
    raw = _read_csv(input_dir / "disclosures" / "report_PeriodicReturns.csv")
    parsed = raw["Period"].str.strip().str.extract(_PERIOD)
    if parsed[0].isna().any():
        bad = raw.loc[parsed[0].isna(), "Period"].unique()[:5]
        raise ValueError(f"unparsed reporting periods: {list(bad)}")

    start = pd.to_datetime(parsed[0], format="%d/%m/%Y", errors="raise")
    end = pd.to_datetime(parsed[1], format="%d/%m/%Y", errors="raise")
    created = pd.to_datetime(
        raw["Date Created"],
        format=constants.DATE_FORMAT_GIFT.value,
        errors="raise",
    )
    return pd.DataFrame(
        {
            "year": start.dt.year.astype("Int64"),
            "date_created": created.dt.date,
            "submitter_name": _blank_to_none(raw["Submitter"]),
            "return_for_name": _blank_to_none(raw["For"]),
            "period_start_date": start.dt.date,
            "period_end_date": end.dt.date,
            "period_label": _blank_to_none(parsed[2]),
            "amount_received": pd.to_numeric(
                raw["Amount Received"], errors="coerce"
            ),
            "amount_paid": pd.to_numeric(raw["Amount Paid"], errors="coerce"),
        }
    )


# --------------------------------------------------------------------------------------
# Dictionary
# --------------------------------------------------------------------------------------

YES_NO = {"yes": "Yes", "no": "No"}

VOCABULARIES: dict[str, dict[str, str]] = {
    "government_level": {
        "state": "Queensland state government election",
        "local": "Queensland local government election",
    },
    "contest_type": {
        "state_district": "Legislative Assembly district",
        "councillor": "Local government councillor",
        "mayor": "Local government mayor",
    },
    "voting_system": {
        "compulsory_preferential": "Compulsory preferential voting",
        "optional_preferential": "Optional preferential voting",
        "first_past_the_post": "First past the post",
    },
    "count_status": {
        "preliminary_unofficial": "Unofficial preliminary count",
        "indicative_unofficial": "Unofficial indicative count",
        "first_preference_official": "Official first preference count",
        "distribution_of_preferences_official": (
            "Official distribution of preferences count"
        ),
        "declared_unopposed": "Contest decided without a count, candidate declared elected",
    },
    "count_type": {
        "first_preference": "First preference votes",
        "two_candidate_preferred": "Two-candidate preferred votes",
    },
    "voting_centre_type_code": {
        "PB": "Polling booth",
        "EV": "Early voting centre",
        "DV1": "Postal declaration votes",
        "DV2": "In person declaration votes",
        "AB": "Absent vote, election day and early voting not distinguished",
        "AB1": "Absent vote cast on election day",
        "AB2": "Absent vote cast at early voting",
    },
    "joint_type": {
        "Host": "Voting centre hosting a shared arrangement with another district",
        "Guest": "Voting centre hosted by another district's centre",
    },
    "is_declared_elected": YES_NO,
    "is_final": YES_NO,
    "is_abolished": YES_NO,
    "is_political_donation": {
        "Yes": "Reported as a political donation",
        "No": "Not reported as a political donation",
        "Unknown": "Reported as unknown by the discloser",
        "-": "Not applicable, the field is collected only for state disclosures",
    },
    "has_electoral_committee": {
        "Yes": "An electoral committee is associated with the gift",
        "No": "No electoral committee is associated with the gift",
    },
    "election_type": {
        "State General": "State general election",
        "State By-election": "State by-election",
        "Local Quadrennial": "Local government quadrennial election",
        "Local Councillor By-election": "Local government councillor by-election",
        "Local Mayoral By-election": "Local government mayoral by-election",
    },
    "candidate_type": {
        "Councillor": "Councillor candidate",
        "Mayor": "Mayoral candidate",
        "Announced Candidate": "Announced candidate not yet formally nominated",
    },
    "voting_method": {
        "Attendance Ballot": "Attendance ballot, voters attend a voting centre",
        "Full Postal Ballot": "Full postal ballot",
        "Hybrid Ballot": "Hybrid ballot combining attendance and postal voting",
    },
}

# Which (table, column) pairs are dictionary-covered. Must match covered_by_dictionary
# in the architecture exactly.
DICTIONARY_COLUMNS: list[tuple[str, str]] = [
    ("election", "election_type"),
    ("election", "government_level"),
    ("candidate", "government_level"),
    ("candidate", "contest_type"),
    ("candidate", "voting_system"),
    ("candidate", "is_declared_elected"),
    ("enrolment_turnout", "government_level"),
    ("enrolment_turnout", "contest_type"),
    ("enrolment_turnout", "voting_system"),
    ("enrolment_turnout", "count_status"),
    ("enrolment_turnout", "voting_method"),
    ("enrolment_turnout", "is_final"),
    ("result_district", "government_level"),
    ("result_district", "contest_type"),
    ("result_district", "voting_system"),
    ("result_district", "count_status"),
    ("result_district", "count_type"),
    ("result_voting_centre", "government_level"),
    ("result_voting_centre", "contest_type"),
    ("result_voting_centre", "voting_system"),
    ("result_voting_centre", "count_status"),
    ("result_voting_centre", "count_type"),
    ("result_voting_centre", "voting_centre_type_code"),
    ("distribution_of_preferences", "government_level"),
    ("distribution_of_preferences", "contest_type"),
    ("distribution_of_preferences", "voting_system"),
    ("distribution_of_preferences", "count_status"),
    ("voting_centre", "joint_type"),
    ("voting_centre", "is_abolished"),
    ("disclosure_gift", "government_level"),
    ("disclosure_gift", "is_political_donation"),
    ("disclosure_gift", "has_electoral_committee"),
    ("disclosure_expenditure", "candidate_type"),
]


def build_dicionario(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Static label sets, verified against the values actually present in the data.

    The labels are declared here rather than harvested from the source, because ECQ's
    own label field is unreliable. The coverage assertion is what keeps the declaration
    honest: a value that appears in a cleaned table and not in ``VOCABULARIES`` fails
    the build rather than shipping an incomplete dictionary.
    """
    rows: list[dict] = []
    uncovered: list[str] = []
    for table, column in DICTIONARY_COLUMNS:
        vocabulary = VOCABULARIES[column]
        observed = frames[table][column].dropna()
        observed = set(
            observed[observed.astype(str) != ""].astype(str).unique()
        )
        missing = observed - set(vocabulary)
        if missing:
            uncovered.append(f"{table}.{column}: {sorted(missing)}")
        for key, value in vocabulary.items():
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": None,
                    "valor": value,
                }
            )
    if uncovered:
        raise ValueError(
            "dictionary does not cover observed values: "
            + "; ".join(uncovered)
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------------

# Tables permitted to emit a null partition, and why. Anything not listed here fails
# the build rather than silently losing rows to a sentinel directory.
NULL_PARTITION_ALLOWED = {"disclosure_gift"}

_ARROW_TYPES = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date32",
    "DATETIME": "timestamp",
}


def _to_all_string_table(frame: pd.DataFrame, columns) -> pa.Table:
    """Build an all-STRING Arrow table with a stable column order.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s every
    column, so the staging schema carries order, not types. Two details are load-bearing:

    * cast through Arrow, never ``astype(str)`` — the latter renders NULL as the literal
      ``"nan"``, which ``safe_cast`` will not turn back into NULL;
    * pass the architecture's real type through first, so ``year`` serialises as
      ``"2024"`` and not ``"2024.0"``.
    """
    import pyarrow as pa

    arrays = []
    names = []
    for column in columns:
        series = (
            frame[column.name]
            if column.name in frame
            else pd.Series([None] * len(frame))
        )
        kind = _ARROW_TYPES[column.bigquery_type]
        if kind == "int64":
            typed = pa.array(
                pd.to_numeric(series, errors="coerce").astype("Int64"),
                type=pa.int64(),
            )
        elif kind == "float64":
            typed = pa.array(
                pd.to_numeric(series, errors="coerce"), type=pa.float64()
            )
        elif kind == "date32":
            values = pd.to_datetime(series, errors="coerce")
            typed = pa.array(values.dt.date, type=pa.date32())
        elif kind == "timestamp":
            values = pd.to_datetime(series, errors="coerce")
            typed = pa.array(values, type=pa.timestamp("s"))
        else:
            values = series.where(series.notna(), other=None)
            typed = pa.array(
                [None if v is None else str(v) for v in values],
                type=pa.string(),
            )
        arrays.append(typed.cast(pa.string()))
        names.append(column.name)
    return pa.Table.from_arrays(arrays, names=names)


def write_partitioned(
    frame: pd.DataFrame, table: str, columns, output_dir: Path
) -> int:
    """Write one table as Snappy parquet, hive-partitioned by ``year``."""
    import pyarrow.parquet as pq

    target = output_dir / table
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    if table == "dicionario":
        arrow = _to_all_string_table(frame, columns)
        pq.write_table(arrow, target / "data.parquet", compression="snappy")
        return len(frame)

    written = 0
    for year, chunk in frame.groupby(frame["year"], dropna=False):
        if pd.isna(year):
            # Hive's own sentinel for a null partition key. 23 gift rows carry no gift
            # date and no election, so no year can be inferred for them. They are real
            # gifts with a donor, a recipient and an amount, so they are kept rather
            # than dropped, and kept null rather than given a fabricated year. The
            # sentinel makes BigQuery read the partition column as STRING, which the
            # dbt model's safe_cast turns back into NULL.
            if table not in NULL_PARTITION_ALLOWED:
                raise ValueError(
                    f"{table}: {len(chunk)} rows have a null partition year; decide "
                    "explicitly whether to drop or repair them"
                )
            partition = target / "year=__HIVE_DEFAULT_PARTITION__"
        else:
            partition = target / f"year={int(year)}"
        partition.mkdir(parents=True, exist_ok=True)
        body = chunk.drop(columns=["year"])
        arrow = _to_all_string_table(
            body, [c for c in columns if c.name != "year"]
        )
        if arrow.num_rows == 0:
            # An empty first partition makes dump_header infer INTEGER and poisons the
            # staging schema. Never emit one.
            continue
        pq.write_table(arrow, partition / "data.parquet", compression="snappy")
        written += arrow.num_rows
    return written


def clean_all(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Full transform. Returns a row count per table."""
    from pipelines.datasets.au_qld_ecq_elections import schema

    crosswalk = build_directory_crosswalk(
        input_dir / "directory_crosswalk.json"
    )
    frames = build_results(input_dir, crosswalk)
    frames["disclosure_gift"] = build_disclosure_gift(input_dir)
    frames["disclosure_expenditure"] = build_disclosure_expenditure(input_dir)
    frames["disclosure_return"] = build_disclosure_return(input_dir)
    frames["dicionario"] = build_dicionario(frames)

    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for table in constants.TABLES.value:
        columns = schema.TABLES[table]
        frame = frames[table]
        missing = [c.name for c in columns if c.name not in frame.columns]
        if missing:
            raise ValueError(f"{table}: cleaned frame is missing {missing}")
        extra = [
            c for c in frame.columns if c not in {col.name for col in columns}
        ]
        if extra:
            raise ValueError(
                f"{table}: cleaned frame has undeclared columns {extra}"
            )
        counts[table] = write_partitioned(
            frame[[c.name for c in columns]], table, columns, output_dir
        )
    return counts
