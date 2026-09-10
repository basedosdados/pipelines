"""Pure cleaning transform for au_sa_ecsa_elections.

No Prefect imports: the one-shot onboarding under ``models/`` and any later
recurring pipeline both import these functions rather than keeping a copy each.

Two shapes have to be reconciled. The 2026 general election uses a newer payload
with a full distribution of preferences and separate declaration blocks; the 2022
general election and all three by-elections use the legacy shape. Every builder
below handles both and records which fields are absent rather than filling them.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
from collections import defaultdict

import pandas as pd

HA = "house_of_assembly"
LC = "legislative_council"
STATE = "state"

# Count types kept long in result_district.
FIRST_PREFERENCE = "first_preference"
FIRST_PREFERENCE_ORDINARY = "first_preference_ordinary"
FIRST_PREFERENCE_DECLARATION = "first_preference_declaration"
TWO_CANDIDATE = "two_candidate_preferred"
TWO_CANDIDATE_DECLARATION = "two_candidate_preferred_declaration"
TWO_PARTY = "two_party_preferred"
TWO_PARTY_DECLARATION = "two_party_preferred_declaration"

RESULT_BASE_URL = "https://apim-ecsa-production.azure-api.net/results-display/"


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


def election_id(event_name: str, date: str) -> str:
    """Stable id for an electoral event.

    ``State General Election 2026`` becomes ``sge-2026`` and ``Bragg By-Election
    2022`` becomes ``by-bragg-2022``. The poll date disambiguates the two 2024
    by-elections only through their district name, which is already in the event
    name, so no date component is needed.
    """
    year = date[:4]
    if "by-election" in event_name.lower():
        district = event_name.lower().split("by-election")[0].strip()
        return f"by-{slugify(district)}-{year}"
    return f"sge-{year}"


def contest_id(chamber: str, district: str) -> str:
    return f"{'ha' if chamber == HA else 'lc'}-{slugify(district)}"


def name_key(name: str | None) -> frozenset[str]:
    """Order-insensitive key for a candidate name.

    The same person is published as ``SANDERSON, Rachel`` in the candidates file
    and as ``Rachel SANDERSON`` in the distribution and declaration blocks, so a
    literal comparison fails. Comparing the set of upper-cased word tokens is
    stable across both spellings.
    """
    if not name:
        return frozenset()
    tokens = re.findall(r"[A-Za-z']+", name.upper())
    return frozenset(tokens)


def split_ballot_name(name: str | None) -> tuple[str | None, str | None]:
    """Split ``SURNAME, Given names`` into its two halves."""
    if not name:
        return None, None
    if "," in name:
        surname, _, given = name.partition(",")
        return surname.strip() or None, given.strip() or None
    # ``Given names SURNAME``: the trailing all-caps run is the surname.
    tokens = name.split()
    upper = [t for t in tokens if t.isupper()]
    if upper:
        surname = " ".join(upper)
        given = " ".join(t for t in tokens if not t.isupper())
        return surname or None, given or None
    return name.strip() or None, None


def canonical_ballot_name(name: str | None) -> str | None:
    """Normalise any published spelling to ``SURNAME, Given names``."""
    surname, given = split_ballot_name(name)
    if surname and given:
        return f"{surname}, {given}"
    return surname or given


def to_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        value = value.replace(",", "").strip()
        if not value or not re.fullmatch(r"-?\d+", value):
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        value = re.sub(r"[^0-9.\-]", "", value)
        if not value:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def yes_no(value: object) -> str | None:
    if value is None:
        return None
    return "yes" if bool(value) else "no"


def share(part: int | None, whole: int | None) -> float | None:
    if part is None or not whole:
        return None
    return round(100.0 * part / whole, 4)


def load_sed_crosswalk(path: pathlib.Path) -> dict[str, str]:
    """Map a lower-cased district name to its ASGS 2021 code.

    One vintage is pinned for every election. The three ASGS vintages re-use
    division ids for different divisions — 18 ids mean different districts in
    2016 and 2021 — so matching each election to its contemporaneous vintage
    would let a group-by silently merge two unrelated districts.
    """
    frame = pd.read_csv(path, dtype=str)
    return {
        str(row["name"]).strip().lower(): str(
            row["id_state_electoral_division"]
        )
        for _, row in frame.iterrows()
    }


# --------------------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------------------


def load_api(root: pathlib.Path, date: str, stem: str) -> dict | None:
    path = root / "api" / date / f"{stem}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def elections(root: pathlib.Path) -> list[dict]:
    payload = json.loads(
        (root / "api" / "ElectionDates.json").read_text(encoding="utf-8")
    )
    return sorted(payload["elections"], key=lambda e: e["electionDate"])


def contest_block(
    event: dict, chamber: str, district: str, sed: dict[str, str]
) -> dict[str, object]:
    date = event["electionDate"]
    return {
        "year": int(date[:4]),
        "election_id": election_id(event["electionEvent"], date),
        "contest_id": contest_id(chamber, district),
        "state_electoral_division_id": (
            sed.get(district.strip().lower()) if chamber == HA else None
        ),
        "chamber": chamber,
        "government_level": STATE,
        "contest_type": "state_district"
        if chamber == HA
        else "state_at_large",
        "voting_system": (
            "compulsory_preferential"
            if chamber == HA
            else "single_transferable_vote"
        ),
        "district_name": district,
    }


# --------------------------------------------------------------------------------------
# Per-district assembly views, shared by several builders
# --------------------------------------------------------------------------------------


def _static_candidates(static: dict) -> dict[str, dict[str, dict]]:
    """district name -> ballot position -> candidate record."""
    out: dict[str, dict[str, dict]] = {}
    for district in static["districts"]:
        out[district["districtName"]] = {
            str(c["candidateId"]): c for c in district["candidates"]
        }
    return out


def _name_to_position(static: dict) -> dict[str, dict[frozenset[str], str]]:
    """district name -> candidate name key -> ballot position.

    The distribution block identifies candidates by a global candidate id whose
    value space does not intersect the ballot positions used everywhere else, so
    the only bridge between the two is district plus name.
    """
    out: dict[str, dict[frozenset[str], str]] = {}
    for district in static["districts"]:
        out[district["districtName"]] = {
            name_key(c["candidateName"]): str(c["candidateId"])
            for c in district["candidates"]
        }
    return out


def _district_first_preferences(
    district: dict, change_shape: str, positions: dict | None = None
) -> dict[str, int]:
    """Ballot position -> total first preferences for one Assembly district.

    In 2022 and at the by-elections ``ordinaryVotes + declarationVotes`` is the
    complete first-preference total. In 2026 it is not: the absent declaration
    votes sit in a separate block and are excluded from ``declarationVotes``, so
    round zero of the distribution is authoritative there.
    """
    if change_shape == "modern":
        for round_block in district.get("finalDistribution") or []:
            if str(round_block.get("roundNumber")) != "0":
                continue
            # candidateId here is the global candidate id (946-1381), not the
            # ballot position (1-12) that keys every other block. The two value
            # spaces do not overlap at all, so the only bridge is district plus
            # name. Keying on it directly produced first-preference rows that
            # joined to no candidate and carried a null name and party.
            lookup = positions or {}
            totals = {}
            for result in round_block.get("candidateResults") or []:
                position = lookup.get(name_key(result.get("candidateName")))
                if position is None:
                    raise ValueError(
                        f"{district.get('districtId')}: no ballot position for "
                        f"{result.get('candidateName')!r} in the distribution"
                    )
                totals[position] = to_int(result["progressiveTotal"]) or 0
            return totals
    return {
        str(c["candidateId"]): (to_int(c.get("ordinaryVotes")) or 0)
        + (to_int(c.get("declarationVotes")) or 0)
        for c in district.get("candidates") or []
    }


def _shape(change: dict) -> str:
    """``modern`` for the 2026 payload, ``legacy`` for 2022 and the by-elections."""
    districts = change.get("districts") or []
    if districts and "finalDistribution" in districts[0]:
        return "modern"
    return "legacy"


def _venue_blocks(district: dict) -> list[tuple[str, str, dict]]:
    """Every counted venue of a district as ``(name, type, block)``.

    The 2026 payload splits the count across ordinary polling places, declaration
    blocks and absent declaration blocks. All three are counted venues with the
    same grain, so they are published as rows of the same table, distinguished by
    the venue type.
    """
    blocks: list[tuple[str, str, dict]] = []
    for place in district.get("pollingPlaces") or []:
        blocks.append((place["pollingPlaceName"], "polling_place", place))
    for block in district.get("declarations") or []:
        blocks.append((block["declarationType"], "declaration", block))
    for block in district.get("absentOrdinary") or []:
        blocks.append(
            (block["absentOrdinaryType"], "absent_declaration", block)
        )
    return blocks


# --------------------------------------------------------------------------------------
# election
# --------------------------------------------------------------------------------------


def build_election(root: pathlib.Path) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        ha_static = load_api(root, date, "ha_static")
        ha_change = load_api(root, date, "ha_change")
        lc_static = load_api(root, date, "lc_static")
        if ha_static is None or ha_change is None:
            raise ValueError(f"{date}: Assembly payload missing")
        by_election = "by-election" in event["electionEvent"].lower()
        rows.append(
            {
                "year": int(date[:4]),
                "election_id": election_id(event["electionEvent"], date),
                "election_name": event["electionEvent"],
                "election_type": (
                    "state_by_election"
                    if by_election
                    else "state_general_election"
                ),
                "government_level": STATE,
                "election_date": date,
                "assembly_districts_contested": len(ha_static["districts"]),
                "council_seats_contested": (
                    to_int(lc_static.get("lcSeatsContested"))
                    if lc_static
                    else None
                ),
                "results_last_updated": ha_change.get("lastUpdated"),
                "results_data_version": str(ha_change.get("dataVersion")),
                "results_source_url": f"{RESULT_BASE_URL}HAChange/{date}/0",
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# candidate
# --------------------------------------------------------------------------------------


def _elected_positions(district: dict, shape: str, names: dict) -> set[str]:
    """Ballot positions elected in one Assembly district.

    2026 publishes an explicit flag on the distribution of preferences. The
    legacy payload publishes none, so election is derived from the final count:
    the two candidate preferred total where it exists, and the first preference
    total otherwise (an unopposed or two candidate contest has no distribution).
    """
    if shape == "modern":
        elected = set()
        for round_block in district.get("finalDistribution") or []:
            for result in round_block.get("candidateResults") or []:
                if result.get("isElected"):
                    position = names.get(name_key(result.get("candidateName")))
                    if position is not None:
                        elected.add(position)
        if elected:
            return elected

    rounds: dict[int, dict[str, int]] = defaultdict(dict)
    for candidate in district.get("candidates") or []:
        for step in candidate.get("distributionVotes") or []:
            round_number = to_int(step.get("round"))
            value = to_int(step.get("distributionVote"))
            if round_number is not None and value is not None:
                rounds[round_number][str(candidate["candidateId"])] = value
    if rounds:
        final = rounds[max(rounds)]
        best = max(final.values())
        return {position for position, value in final.items() if value == best}

    two_candidate = _district_two_preferred(district, "twoCandidatePref")
    pool = {k: v for k, v in two_candidate.items() if v}
    if not pool:
        pool = {
            k: v
            for k, v in _district_first_preferences(
                district, shape, names
            ).items()
        }
    if not pool:
        return set()
    best = max(pool.values())
    return {position for position, value in pool.items() if value == best}


def build_candidate(root: pathlib.Path, sed: dict[str, str]) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        change = load_api(root, date, "ha_change")
        shape = _shape(change)
        name_map = _name_to_position(static)
        change_districts = {d["districtId"]: d for d in change["districts"]}
        for district in static["districts"]:
            district_name = district["districtName"]
            block = contest_block(event, HA, district_name, sed)
            counted = change_districts.get(district_name, {})
            elected = _elected_positions(
                counted, shape, name_map.get(district_name, {})
            )
            for candidate in district["candidates"]:
                position = str(candidate["candidateId"])
                ballot_name = canonical_ballot_name(candidate["candidateName"])
                surname, given = split_ballot_name(candidate["candidateName"])
                rows.append(
                    {
                        **block,
                        "ballot_order_number": position,
                        "ballot_name": ballot_name,
                        "candidate_surname": surname,
                        "candidate_given_names": given,
                        "party_code": candidate.get("partyId"),
                        "party_name": candidate.get("partyName"),
                        "is_declared_elected": (
                            "yes" if position in elected else "no"
                        ),
                    }
                )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# result_district
# --------------------------------------------------------------------------------------


def _district_two_preferred(district: dict, field: str) -> dict[str, int]:
    """Ballot position -> preferred-count total across every counted venue."""
    totals: dict[str, int] = defaultdict(int)
    seen: set[str] = set()
    for place in district.get("pollingPlaces") or []:
        for entry in place.get("pollingCandidates") or []:
            value = to_int(entry.get(field))
            if value is None:
                continue
            position = str(entry["candidateId"])
            totals[position] += value
            seen.add(position)
    declaration_field = {
        "twoCandidatePref": (
            "twoCandidatePrefVoteCount",
            "twoCandidatePrefVotes",
        ),
        "twoPartyPref": ("twoPartyPrefVoteCount", "twoPartyPrefVotes"),
    }[field]
    for block in (district.get("declarations") or []) + (
        district.get("absentOrdinary") or []
    ):
        for entry in block.get("candidateVotes") or []:
            value = None
            for key in declaration_field:
                if entry.get(key) is not None:
                    value = to_int(entry.get(key))
                    break
            if value is None:
                continue
            position = str(entry["candidateId"])
            totals[position] += value
            seen.add(position)
    # Where the declaration blocks exist, the candidate-level declaration field
    # repeats them, so adding both double counts the declaration half. That
    # reversed Narungga in 2026: the two candidate preferred count came out at
    # 13,617 to 13,541 for the losing candidate instead of 12,001 to 12,078.
    if district.get("declarations") or district.get("absentOrdinary"):
        return {position: totals[position] for position in seen}
    # The legacy payload has no declaration blocks: it carries the declaration half
    # of each preferred count on the candidate record instead. Omitting it leaves
    # the count at roughly its ordinary component, which in 2022 reversed the
    # result in Dunstan and Finniss.
    legacy_key = {
        "twoCandidatePref": "twoCandidatePrefDeclarationVotes",
        "twoPartyPref": "twoPartyPrefDeclarationVotes",
    }[field]
    for candidate in district.get("candidates") or []:
        value = to_int(candidate.get(legacy_key))
        if value is None:
            continue
        position = str(candidate["candidateId"])
        totals[position] += value
        seen.add(position)
    return {position: totals[position] for position in seen}


def _legacy_declaration(district: dict, field: str) -> dict[str, int]:
    return {
        str(c["candidateId"]): to_int(c.get(field))
        for c in district.get("candidates") or []
        if to_int(c.get(field)) is not None
    }


def build_result_district(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        change = load_api(root, date, "ha_change")
        shape = _shape(change)
        static_candidates = _static_candidates(static)
        name_map = _name_to_position(static)
        for district in change["districts"]:
            district_name = district["districtId"]
            block = contest_block(event, HA, district_name, sed)
            catalogue = static_candidates.get(district_name, {})

            first = _district_first_preferences(
                district, shape, name_map.get(district_name, {})
            )
            ordinary = {
                str(c["candidateId"]): to_int(c.get("ordinaryVotes"))
                for c in district.get("candidates") or []
            }
            declaration = {
                str(c["candidateId"]): to_int(c.get("declarationVotes"))
                for c in district.get("candidates") or []
            }
            two_candidate = _district_two_preferred(
                district, "twoCandidatePref"
            )
            two_party = _district_two_preferred(district, "twoPartyPref")
            two_candidate_dec = _legacy_declaration(
                district, "twoCandidatePrefDeclarationVotes"
            )
            two_party_dec = _legacy_declaration(
                district, "twoPartyPrefDeclarationVotes"
            )
            # A preferred count published as zero for every candidate is the ECSA
            # not having produced it, not a genuine tie at nil. Publishing those
            # zeros would read as a real result.
            if two_candidate and not any(two_candidate.values()):
                two_candidate = {}
            if two_party and not any(two_party.values()):
                two_party = {}

            formal = sum(v for v in first.values() if v)
            series = (
                (FIRST_PREFERENCE, first),
                (FIRST_PREFERENCE_ORDINARY, ordinary),
                (FIRST_PREFERENCE_DECLARATION, declaration),
                (TWO_CANDIDATE, two_candidate),
                (TWO_CANDIDATE_DECLARATION, two_candidate_dec),
                (TWO_PARTY, two_party),
                (TWO_PARTY_DECLARATION, two_party_dec),
            )
            for count_type, values in series:
                for position, value in sorted(
                    values.items(), key=lambda kv: int(kv[0])
                ):
                    if value is None:
                        continue
                    if count_type != FIRST_PREFERENCE and value == 0:
                        # A preferred count is defined over the final two
                        # candidates only; the source writes 0 for everyone else
                        # and publishing those zeros would read as a real result.
                        continue
                    candidate = catalogue.get(position, {})
                    rows.append(
                        {
                            **block,
                            "count_type": count_type,
                            "contestant_id": position,
                            "ballot_order_number": position,
                            "ballot_name": canonical_ballot_name(
                                candidate.get("candidateName")
                            ),
                            "party_code": candidate.get("partyId"),
                            "party_name": candidate.get("partyName"),
                            "group_code": None,
                            "group_name": None,
                            "votes": value,
                            "percentage": (
                                share(value, formal)
                                if count_type == FIRST_PREFERENCE
                                else None
                            ),
                        }
                    )

        rows.extend(_council_district_rows(root, event, sed))
    return pd.DataFrame(rows)


def _council_parties(lc_static: dict) -> dict[str, dict]:
    return {str(p["id"]): p for p in lc_static.get("parties") or []}


def _council_district_rows(
    root: pathlib.Path, event: dict, sed: dict[str, str]
) -> list[dict]:
    date = event["electionDate"]
    lc_static = load_api(root, date, "lc_static")
    lc_change = load_api(root, date, "lc_change")
    if lc_static is None or lc_change is None:
        return []
    block = contest_block(event, LC, "State", sed)
    catalogue = _council_parties(lc_static)
    formal = to_int(lc_change.get("totalFormalVotes"))
    rows = []
    for party in lc_change.get("parties") or []:
        record = catalogue.get(str(party["id"]), {})
        votes = to_int(party.get("votes"))
        rows.append(
            {
                **block,
                "count_type": FIRST_PREFERENCE,
                "contestant_id": str(party["id"]),
                "ballot_order_number": None,
                "ballot_name": None,
                "party_code": record.get("partyId"),
                "party_name": record.get("partyName"),
                "group_code": party.get("groupId"),
                "group_name": party.get("groupName")
                or record.get("partyName"),
                "votes": votes,
                "percentage": share(votes, formal),
            }
        )
    return rows


# --------------------------------------------------------------------------------------
# result_voting_centre
# --------------------------------------------------------------------------------------

_VENUE_TYPE_LABEL = {
    "declaration": "Declaration",
    "absent_declaration": "Absent Declaration",
}

_TWO_CANDIDATE_KEYS = (
    "twoCandidatePref",
    "twoCandidatePrefVoteCount",
    "twoCandidatePrefVotes",
)
_TWO_PARTY_KEYS = (
    "twoPartyPref",
    "twoPartyPrefVoteCount",
    "twoPartyPrefVotes",
)


def _first_key(entry: dict, keys: tuple[str, ...]) -> int | None:
    for key in keys:
        if entry.get(key) is not None:
            return to_int(entry.get(key))
    return None


def build_result_voting_centre(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        change = load_api(root, date, "ha_change")
        static_candidates = _static_candidates(static)
        place_types = {
            (d["districtName"], p["pollingPlaceName"]): p.get(
                "pollingPlaceType"
            )
            for d in static["districts"]
            for p in d.get("pollingPlaces") or []
        }
        for district in change["districts"]:
            district_name = district["districtId"]
            block = contest_block(event, HA, district_name, sed)
            catalogue = static_candidates.get(district_name, {})
            for venue_name, venue_kind, venue in _venue_blocks(district):
                entries = (
                    venue.get("pollingCandidates")
                    or venue.get("candidateVotes")
                    or []
                )
                informal = to_int(venue.get("informalVotes"))
                formal = to_int(venue.get("formalVotes"))
                if formal is None:
                    formal = sum(
                        to_int(e.get("formalVotes"))
                        or to_int(e.get("votes"))
                        or 0
                        for e in entries
                    )
                venue_type = (
                    place_types.get((district_name, venue_name))
                    if venue_kind == "polling_place"
                    else _VENUE_TYPE_LABEL[venue_kind]
                )
                for entry in entries:
                    position = str(entry["candidateId"])
                    candidate = catalogue.get(position, {})
                    common = {
                        **block,
                        "voting_centre_district_name": district_name,
                        "voting_centre_name": venue_name,
                        "voting_centre_type": venue_type,
                        "contestant_id": position,
                        "ballot_order_number": position,
                        "ballot_name": canonical_ballot_name(
                            candidate.get("candidateName")
                        ),
                        "party_code": candidate.get("partyId"),
                        "party_name": candidate.get("partyName"),
                        "group_code": None,
                        "group_name": None,
                        "votes_formal": formal,
                        "votes_informal": informal,
                    }
                    measures = (
                        (
                            FIRST_PREFERENCE,
                            _first_key(entry, ("formalVotes", "votes")),
                        ),
                        (
                            TWO_CANDIDATE,
                            _first_key(entry, _TWO_CANDIDATE_KEYS),
                        ),
                        (TWO_PARTY, _first_key(entry, _TWO_PARTY_KEYS)),
                    )
                    for count_type, value in measures:
                        if value is None:
                            continue
                        if count_type != FIRST_PREFERENCE and value == 0:
                            # Only the two leading candidates carry a preferred
                            # count; the source writes 0 for everyone else, and in
                            # 26 districts of 2026 for everyone including them.
                            continue
                        rows.append(
                            {
                                **common,
                                "count_type": count_type,
                                "votes": value,
                            }
                        )

        rows.extend(_council_venue_rows(root, event, sed))
    return pd.DataFrame(rows)


def _council_venue_rows(
    root: pathlib.Path, event: dict, sed: dict[str, str]
) -> list[dict]:
    date = event["electionDate"]
    lc_static = load_api(root, date, "lc_static")
    lc_change = load_api(root, date, "lc_change")
    if lc_static is None or lc_change is None:
        return []
    block = contest_block(event, LC, "State", sed)
    catalogue = _council_parties(lc_static)
    rows = []
    for district in lc_change.get("districts") or []:
        district_name = district.get("districtName")
        venues = [
            (
                p["pollingPlaceName"],
                p.get("pollingPlaceType"),
                p,
                p.get("parties"),
            )
            for p in district.get("pollingPlaces") or []
        ] + [
            (
                b["declarationType"],
                _VENUE_TYPE_LABEL["declaration"],
                b,
                b.get("partyVotes"),
            )
            for b in district.get("declarations") or []
        ]
        for venue_name, venue_type, venue, entries in venues:
            entries = entries or []
            informal = to_int(venue.get("informalVotes"))
            formal = to_int(venue.get("formalVotes"))
            if formal is None:
                formal = sum(to_int(e.get("votes")) or 0 for e in entries)
            for entry in entries:
                record = catalogue.get(str(entry["id"]), {})
                rows.append(
                    {
                        **block,
                        "voting_centre_district_name": district_name,
                        "voting_centre_name": venue_name,
                        "voting_centre_type": venue_type,
                        "count_type": FIRST_PREFERENCE,
                        "contestant_id": str(entry["id"]),
                        "ballot_order_number": None,
                        "ballot_name": None,
                        "party_code": record.get("partyId"),
                        "party_name": record.get("partyName"),
                        "group_code": entry.get("groupId"),
                        "group_name": entry.get("groupName")
                        or record.get("partyName"),
                        "votes": to_int(entry.get("votes")),
                        "votes_formal": formal,
                        "votes_informal": informal,
                    }
                )
    return rows


# --------------------------------------------------------------------------------------
# distribution_of_preferences
# --------------------------------------------------------------------------------------


def build_distribution_of_preferences(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        change = load_api(root, date, "ha_change")
        shape = _shape(change)
        static_candidates = _static_candidates(static)
        name_map = _name_to_position(static)
        for district in change["districts"]:
            district_name = district["districtId"]
            block = contest_block(event, HA, district_name, sed)
            catalogue = static_candidates.get(district_name, {})
            positions = name_map.get(district_name, {})

            if shape == "modern":
                for round_block in district.get("finalDistribution") or []:
                    excluded = canonical_ballot_name(
                        (
                            round_block.get("excludedCandidateName") or ""
                        ).strip()
                        or None
                    )
                    for result in round_block.get("candidateResults") or []:
                        position = positions.get(
                            name_key(result.get("candidateName"))
                        )
                        candidate = catalogue.get(position or "", {})
                        rows.append(
                            {
                                **block,
                                "round_number": str(
                                    round_block.get("roundNumber")
                                ),
                                "round_type": round_block.get("roundType"),
                                "excluded_ballot_name": excluded,
                                "votes_excluded": to_int(
                                    round_block.get("excludedCandidateVotes")
                                ),
                                "ballot_order_number": position,
                                "ballot_name": canonical_ballot_name(
                                    result.get("candidateName")
                                ),
                                "party_code": candidate.get("partyId"),
                                "party_name": candidate.get("partyName"),
                                "votes_transferred": to_int(
                                    result.get("voteChange")
                                ),
                                "votes_progressive_total": to_int(
                                    result.get("progressiveTotal")
                                ),
                                "is_excluded": yes_no(
                                    result.get("isExcluded")
                                ),
                                "is_elected": yes_no(result.get("isElected")),
                            }
                        )
                continue

            for candidate_block in district.get("candidates") or []:
                position = str(candidate_block["candidateId"])
                candidate = catalogue.get(position, {})
                for step in candidate_block.get("distributionVotes") or []:
                    rows.append(
                        {
                            **block,
                            "round_number": str(step.get("round")),
                            "round_type": None,
                            "excluded_ballot_name": None,
                            "votes_excluded": None,
                            "ballot_order_number": position,
                            "ballot_name": canonical_ballot_name(
                                candidate.get("candidateName")
                            ),
                            "party_code": candidate.get("partyId"),
                            "party_name": candidate.get("partyName"),
                            "votes_transferred": None,
                            "votes_progressive_total": to_int(
                                step.get("distributionVote")
                            ),
                            "is_excluded": None,
                            "is_elected": None,
                        }
                    )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# voting_centre
# --------------------------------------------------------------------------------------


def build_voting_centre(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        for district in static["districts"]:
            district_name = district["districtName"]
            block = contest_block(event, HA, district_name, sed)
            for place in district.get("pollingPlaces") or []:
                rows.append(
                    {
                        "year": block["year"],
                        "election_id": block["election_id"],
                        "state_electoral_division_id": block[
                            "state_electoral_division_id"
                        ],
                        "district_name": district_name,
                        "voting_centre_name": place["pollingPlaceName"],
                        "voting_centre_type": place.get("pollingPlaceType"),
                    }
                )
    frame = pd.DataFrame(rows)
    return frame.drop_duplicates(
        subset=["year", "election_id", "district_name", "voting_centre_name"]
    )


# --------------------------------------------------------------------------------------
# enrolment_turnout
# --------------------------------------------------------------------------------------


def build_enrolment_turnout(
    root: pathlib.Path, sed: dict[str, str]
) -> pd.DataFrame:
    rows = []
    for event in elections(root):
        date = event["electionDate"]
        static = load_api(root, date, "ha_static")
        change = load_api(root, date, "ha_change")
        shape = _shape(change)
        enrolment = {
            d["districtName"]: to_int(d.get("districtEnrolled"))
            for d in static["districts"]
        }
        candidate_counts = {
            d["districtName"]: len(d.get("candidates") or [])
            for d in static["districts"]
        }
        name_map = _name_to_position(static)
        for district in change["districts"]:
            district_name = district["districtId"]
            block = contest_block(event, HA, district_name, sed)
            first = _district_first_preferences(
                district, shape, name_map.get(district_name, {})
            )
            formal = sum(v for v in first.values() if v)
            informal = sum(
                to_int(p.get("informalVotes")) or 0
                for p in district.get("pollingPlaces") or []
            )
            informal += to_int(district.get("informalDeclarationVotes")) or 0
            for venue_kind in ("declarations", "absentOrdinary"):
                informal += sum(
                    to_int(b.get("informalVotes")) or 0
                    for b in district.get(venue_kind) or []
                )
            total = formal + informal
            enrolled = enrolment.get(district_name)
            rows.append(
                {
                    **block,
                    "enrolment": enrolled,
                    "candidates_count": candidate_counts.get(district_name),
                    "votes_formal": formal,
                    "votes_informal": informal,
                    "votes_total": total,
                    "percentage_informal": share(informal, total),
                    "percentage_roll_counted": share(total, enrolled),
                    "polling_places_counted": None,
                    "polling_places_total": None,
                }
            )

        lc_static = load_api(root, date, "lc_static")
        lc_change = load_api(root, date, "lc_change")
        if lc_static is None or lc_change is None:
            continue
        block = contest_block(event, LC, "State", sed)
        formal = to_int(lc_change.get("totalFormalVotes"))
        informal = to_int(lc_change.get("totalInformalVotes"))
        total = (formal or 0) + (informal or 0)
        enrolled = sum(v for v in enrolment.values() if v) or None
        rows.append(
            {
                **block,
                "enrolment": enrolled,
                "candidates_count": len(lc_change.get("parties") or []),
                "votes_formal": formal,
                "votes_informal": informal,
                "votes_total": total,
                "percentage_informal": share(informal, total),
                "percentage_roll_counted": share(total, enrolled),
                "polling_places_counted": to_int(
                    lc_change.get("pollingPlacesCounted")
                ),
                "polling_places_total": to_int(
                    lc_change.get("totalPollingPlaces")
                ),
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# disclosure_return
# --------------------------------------------------------------------------------------

_ROW = re.compile(
    r'<tr[^>]*data-href="view\.php\?ID=(\d+)"[^>]*>(.*?)</tr>', re.S
)
_CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG = re.compile(r"<[^>]+>")

# The current portal carries a recipient column the archive does not.
_COLUMNS = {
    "funding2024": [
        "return_type",
        "date_lodged",
        "submitter_name",
        "return_for_name",
        "recipient_name",
        "period_start_date",
        "period_end_date",
        "declared_value",
    ],
    "fdarchive": [
        "return_type",
        "date_lodged",
        "submitter_name",
        "return_for_name",
        "period_start_date",
        "period_end_date",
        "declared_value",
    ],
}


def _text_cell(html: str) -> str | None:
    text = _TAG.sub(" ", html)
    text = (
        text.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&#039;", "'")
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _to_date(value: str | None) -> str | None:
    """Parse a day-first date written with either separator."""
    if not value:
        return None
    match = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", value)
    if not match:
        return None
    day, month, year = (int(g) for g in match.groups())
    try:
        return dt.date(year, month, day).isoformat()
    except ValueError:
        return None


_DETAIL_TYPE = re.compile(r"<h2>(.*?)</h2>", re.S)
_DETAIL_FIELD = re.compile(
    r'<span class="record-field"><b>(.*?)\s*</b>\s*<[^>]*>(.*?)</span>', re.S
)
_DETAIL_PERIOD = re.compile(r"(\d{4}-\d{2}-\d{2})\s*to\s*(\d{4}-\d{2}-\d{2})")

# Labels as the detail page spells them, mapped to the table's columns.
_DETAIL_LABELS = {
    "For Party/Organisation/Individual": "return_for_name",
    "Agent": "submitter_name",
    "Date Lodged": "date_lodged",
}


def _parse_detail(html: str) -> dict[str, str | None]:
    record: dict[str, str | None] = {}
    kind = _DETAIL_TYPE.search(html)
    record["return_type"] = _text_cell(kind.group(1)) if kind else None
    for label, value in _DETAIL_FIELD.findall(html):
        label = _text_cell(label) or ""
        if label in _DETAIL_LABELS:
            record[_DETAIL_LABELS[label]] = _text_cell(value)
        elif label == "Period":
            period = _DETAIL_PERIOD.search(value)
            if period:
                record["period_start_date"] = period.group(1)
                record["period_end_date"] = period.group(2)
    record["date_lodged"] = _to_date(record.get("date_lodged"))
    return record


def build_disclosure_return(root: pathlib.Path) -> pd.DataFrame:
    rows = []
    for portal, columns in _COLUMNS.items():
        directory = root / "disclosure" / portal
        pages = sorted(directory.glob("page_*.html"))
        if not pages:
            raise ValueError(f"{directory}: no harvested pages")
        index: dict[str, dict] = {}
        for page in pages:
            html = page.read_text(encoding="utf-8")
            for return_id, body in _ROW.findall(html):
                cells = [_text_cell(c) for c in _CELL.findall(body)]
                # The row carries a trailing links cell the column list does not
                # name, so the pairing is deliberately not strict.
                index[return_id] = dict(zip(columns, cells, strict=False))

        # The current portal paginates on a non-unique key, so consecutive pages
        # overlap and 93 of its 924 returns are never served by the index at all.
        # The detail pages are keyed on the id and are therefore the complete
        # spine; the index still supplies the two fields the detail page omits.
        detail: dict[str, dict] = {}
        detail_dir = root / "disclosure" / f"{portal}_detail"
        for page in sorted(detail_dir.glob("*.html")):
            detail[page.stem] = _parse_detail(page.read_text(encoding="utf-8"))

        for return_id in sorted(set(index) | set(detail), key=int):
            record = index.get(return_id, {})
            full = detail.get(return_id, {})
            start = full.get("period_start_date") or _to_date(
                record.get("period_start_date")
            )
            rows.append(
                {
                    "year": int(start[:4]) if start else None,
                    "return_id": return_id,
                    "portal": portal,
                    "return_type": full.get("return_type")
                    or record.get("return_type"),
                    "date_lodged": full.get("date_lodged")
                    or _to_date(record.get("date_lodged")),
                    "submitter_name": full.get("submitter_name")
                    or record.get("submitter_name"),
                    "return_for_name": full.get("return_for_name")
                    or record.get("return_for_name"),
                    "recipient_name": record.get("recipient_name"),
                    "period_start_date": start,
                    "period_end_date": full.get("period_end_date")
                    or _to_date(record.get("period_end_date")),
                    "declared_value": to_float(record.get("declared_value")),
                }
            )
    frame = pd.DataFrame(rows).drop_duplicates(subset=["portal", "return_id"])
    missing = int(frame["year"].isna().sum())
    if missing:
        # The partition key has to be complete: a null year cannot be written to a
        # partitioned table, and silently dropping the rows would understate the
        # table. Fall back to the lodgement year, which is always present.
        fallback = frame["date_lodged"].str.slice(0, 4)
        frame["year"] = frame["year"].fillna(
            pd.to_numeric(fallback, errors="coerce")
        )
    return frame


# --------------------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------------------

_DICTIONARY: list[tuple[str, str, str, str]] = [
    ("chamber", "house_of_assembly", "House of Assembly", ""),
    ("chamber", "legislative_council", "Legislative Council", ""),
    ("government_level", "state", "State government", ""),
    (
        "contest_type",
        "state_district",
        "Single member state electoral district",
        "",
    ),
    (
        "contest_type",
        "state_at_large",
        "Statewide multi member contest for the Legislative Council",
        "",
    ),
    (
        "voting_system",
        "compulsory_preferential",
        "Compulsory preferential voting, every square must be numbered",
        "",
    ),
    (
        "voting_system",
        "single_transferable_vote",
        "Proportional single transferable vote with above and below the line voting",
        "",
    ),
    ("election_type", "state_general_election", "State general election", ""),
    (
        "election_type",
        "state_by_election",
        "State by-election for a single district",
        "",
    ),
    (
        "count_type",
        "first_preference",
        "Total first preference votes",
        "",
    ),
    (
        "count_type",
        "first_preference_ordinary",
        "First preference votes cast at a voting centre on or before polling day",
        "",
    ),
    (
        "count_type",
        "first_preference_declaration",
        "First preference votes cast by declaration",
        "",
    ),
    (
        "count_type",
        "two_candidate_preferred",
        "Votes after preferences are distributed between the two leading candidates",
        "",
    ),
    (
        "count_type",
        "two_candidate_preferred_declaration",
        "Declaration component of the two candidate preferred count",
        "",
    ),
    (
        "count_type",
        "two_party_preferred",
        "Votes after preferences are distributed between the two largest parties",
        "",
    ),
    (
        "count_type",
        "two_party_preferred_declaration",
        "Declaration component of the two party preferred count",
        "",
    ),
    (
        "round_type",
        "FirstPreference",
        "Opening round, before any exclusion",
        "2026(1)2026",
    ),
    (
        "round_type",
        "ExclusionRound",
        "Round in which the lowest polling candidate is excluded and their votes redistributed; the trailing number is the ordinal of the exclusion",
        "2026(1)2026",
    ),
    ("is_declared_elected", "yes", "Elected in the contest", ""),
    ("is_declared_elected", "no", "Not elected in the contest", ""),
    ("is_excluded", "yes", "Excluded at this round", ""),
    ("is_excluded", "no", "Not excluded at this round", ""),
    ("is_elected", "yes", "Elected at this round", ""),
    ("is_elected", "no", "Not elected at this round", ""),
    (
        "portal",
        "funding2024",
        "Funding disclosure portal in force from 2023",
        "",
    ),
    (
        "portal",
        "fdarchive",
        "Archived funding disclosure portal covering the 2018 to 2023 periods",
        "",
    ),
]

# Which tables actually carry each coded column, so the dictionary lists real pairs.
_DICTIONARY_TABLES = {
    "chamber": [
        "candidate",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "enrolment_turnout",
    ],
    "government_level": [
        "election",
        "candidate",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "enrolment_turnout",
    ],
    "contest_type": [
        "candidate",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "enrolment_turnout",
    ],
    "voting_system": [
        "candidate",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "enrolment_turnout",
    ],
    "election_type": ["election"],
    "count_type": ["result_district", "result_voting_centre"],
    "round_type": ["distribution_of_preferences"],
    "is_declared_elected": ["candidate"],
    "is_excluded": ["distribution_of_preferences"],
    "is_elected": ["distribution_of_preferences"],
    "portal": ["disclosure_return"],
}

# result_voting_centre publishes only the three headline counts.
_VENUE_COUNT_TYPES = {FIRST_PREFERENCE, TWO_CANDIDATE, TWO_PARTY}


def build_dicionario() -> pd.DataFrame:
    rows = []
    for column, key, value, coverage in _DICTIONARY:
        for table in _DICTIONARY_TABLES[column]:
            if (
                column == "count_type"
                and table == "result_voting_centre"
                and key not in _VENUE_COUNT_TYPES
            ):
                continue
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": coverage or None,
                    "valor": value,
                }
            )
    return pd.DataFrame(rows)
