"""One-shot transform: TEC source artefacts -> the eight au_tas_tec_elections tables.

Pure functions, no Prefect imports, so a later recurring pipeline can reuse them
rather than duplicating the transform.

The reshape that matters is Hare-Clark. The TEC publishes its count in wide form —
one column per candidate, one row per count — and the target
``distribution_of_preferences`` table is long, one row per count and candidate. The
Legislative Council's HTML matrix and the House of Assembly's scrutiny spreadsheet
are the same shape in different containers, so both fold into the same long output.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

from pipelines.datasets.au_tas_tec_elections import parse_excel, parse_html
from pipelines.datasets.au_tas_tec_elections.constants import constants

Row = dict[str, Any]


def _norm(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def _split_name(ballot_name: str) -> tuple[str, str]:
    surname, _, given = ballot_name.partition(",")
    return surname.strip(), given.strip()


def _pct(part: int | None, whole: int | None) -> float | None:
    if part is None or not whole:
        return None
    return round(100.0 * part / whole, 4)


def contest_block(
    election_id: str, division: str, meta: tuple, seats: int
) -> Row:
    """The columns every contest-level table shares."""
    _name, chamber, _etype, date, _prefix = meta
    is_ha = chamber == "house_of_assembly"
    district = division.replace("_", " ").title()
    district = {"Mcintyre": "McIntyre"}.get(district, district)
    return {
        "year": int(date[:4]),
        "election_id": election_id,
        "contest_id": f"{election_id}-{division}",
        "chamber": chamber,
        "government_level": "state",
        "contest_type": (
            "house_of_assembly_division"
            if is_ha
            else "legislative_council_division"
        ),
        "voting_system": "hare_clark" if is_ha else "preferential",
        "district_name": district,
        "commonwealth_electoral_division_id": (
            constants.HOA_DIVISION_TO_CED.value.get(division)
            if is_ha
            else None
        ),
        # Always NULL: the ABS state electoral division layer for Tasmania is the
        # intersection of the two chambers, matching neither. See schema.py.
        "state_electoral_division_id": None,
        "_seats": seats,
    }


def _strip_private(row: Row) -> Row:
    return {k: v for k, v in row.items() if not k.startswith("_")}


def clean_all(input_root: Path, output_root: Path) -> dict[str, int]:
    """Build every table and write partitioned parquet. Returns row counts."""
    manifest = json.loads((input_root / "manifest.json").read_text())[
        "elections"
    ]
    tables: dict[str, list[Row]] = {t: [] for t in constants.TABLES.value}

    for election_id, entry in manifest.items():
        meta = constants.ELECTION_META.value.get(election_id)
        if meta is None:
            # Downloaded but deliberately not published — see ELECTION_META for
            # why the 2017 Pembroke by-election is excluded.
            continue
        name, chamber, etype, date, _prefix = meta
        year = int(date[:4])
        is_ha = chamber == "house_of_assembly"
        seats = (
            constants.SEATS_PER_HOA_DIVISION.value.get(year, 5) if is_ha else 1
        )
        tables["election"].append(
            {
                "year": year,
                "election_id": election_id,
                "election_name": name,
                "chamber": chamber,
                "election_type": etype,
                "election_date": date,
                "seats_per_division": seats,
                "results_index_url": entry["contests"][
                    next(iter(entry["contests"]))
                ]["page_url"].rsplit("/results/", 1)[0]
                + "/",
            }
        )
        for division, contest in entry["contests"].items():
            block = contest_block(election_id, division, meta, seats)
            if is_ha:
                _house_of_assembly(
                    input_root / election_id, division, contest, block, tables
                )
            else:
                _legislative_council(
                    input_root / election_id, division, contest, block, tables
                )
        _voting_centres(
            input_root / election_id, entry, year, election_id, tables
        )

    tables["dicionario"] = _dictionary()
    return _write(tables, output_root)


# --------------------------------------------------------------------------------------
# House of Assembly
# --------------------------------------------------------------------------------------


def _house_of_assembly(
    root: Path,
    division: str,
    contest: Row,
    block: Row,
    tables: dict[str, list[Row]],
) -> None:
    files = contest["files"]
    inline = contest["inline"]
    raw_page = (root / files["page"]).read_text(errors="replace")
    sections = parse_html.split_ha_2018(raw_page) if inline else {}

    fp = dist = None
    if inline:
        if sections.get("fp"):
            fp = parse_html.parse_ha_2018(sections["fp"])
        if sections.get("dist"):
            dist = parse_html.parse_ha_2018(sections["dist"])
    else:
        if "fp" in files:
            fp = parse_html.parse_ha(
                (root / files["fp"]).read_text(errors="replace")
            )
        if "dist" in files:
            dist = parse_html.parse_ha(
                (root / files["dist"]).read_text(errors="replace")
            )

    quota = (dist.quota if dist else None) or (fp.quota if fp else None)
    party_of: dict[str, str] = {}
    for source in (fp, dist):
        if source:
            for cand in source.candidates:
                party_of.setdefault(cand.ballot_name, cand.party_name)

    formal = fp.summary.get("total formal votes") if fp else None
    informal = fp.summary.get("informal ballot papers") if fp else None
    total = fp.summary.get("total ballot papers counted") if fp else None
    enrolment = fp.summary.get("electors on roll") if fp else None
    turnout = fp.summary.get("turnout") if fp else None

    if fp:
        for cand in fp.candidates:
            tables["result_district"].append(
                _strip_private(block)
                | {
                    "count_type": "first_preference",
                    "ballot_name": cand.ballot_name,
                    "party_name": cand.party_name,
                    "votes": cand.votes,
                    "percentage_formal_votes": _pct(
                        cand.votes, int(formal) if formal else None
                    ),
                    "quotas": (
                        round(cand.votes / quota, 4)
                        if cand.votes is not None and quota
                        else None
                    ),
                    "candidate_status": None,
                }
            )
    if dist:
        for cand in dist.candidates:
            status, order = parse_html.split_status(cand.status)
            tables["result_district"].append(
                _strip_private(block)
                | {
                    "count_type": "final_distribution",
                    "ballot_name": cand.ballot_name,
                    "party_name": cand.party_name,
                    "votes": cand.votes,
                    "percentage_formal_votes": _pct(
                        cand.votes, int(formal) if formal else None
                    ),
                    "quotas": (
                        round(cand.votes / quota, 4)
                        if cand.votes is not None and quota
                        else None
                    ),
                    "candidate_status": status,
                }
            )
            tables["candidate"].append(
                _strip_private(block)
                | {
                    "ballot_name": cand.ballot_name,
                    "candidate_surname": _split_name(cand.ballot_name)[0],
                    "candidate_given_names": _split_name(cand.ballot_name)[1],
                    "party_name": cand.party_name,
                    "is_elected": "yes" if status == "elected" else "no",
                    "election_order": order,
                    "candidate_status": status,
                }
            )

    n_cands = len(
        {c.ballot_name for c in (dist.candidates if dist else [])}
    ) or len(party_of)
    tables["enrolment_turnout"].append(
        _strip_private(block)
        | {
            "enrolment": int(enrolment) if enrolment else None,
            "votes_total": int(total) if total else None,
            "votes_formal": int(formal) if formal else None,
            "votes_informal": int(informal) if informal else None,
            "percentage_turnout": turnout,
            "percentage_informal": _pct(
                int(informal) if informal else None,
                int(total) if total else None,
            ),
            "quota": quota,
            "seats_to_elect": block["_seats"],
            "candidates_count": n_cands,
        }
    )

    # Distribution of preferences comes from the count export, published as a
    # spreadsheet only from 2024. Earlier elections publish it as PDF only.
    for doc in contest.get("files", {}).get("docs", []):
        path = root / doc
        if "Export" not in path.name:
            continue
        scrutiny = parse_excel.parse_scrutiny(path)
        for count in scrutiny.counts:
            for surname, group in scrutiny.candidates:
                ballot = _match_ballot(surname, party_of)
                tables["distribution_of_preferences"].append(
                    _strip_private(block)
                    | {
                        "count_number": count.count_number,
                        "ballot_name": ballot or surname,
                        "party_name": party_of.get(ballot or "", group),
                        "votes_transferred": count.transferred.get(surname),
                        "votes_progressive_total": count.totals.get(surname),
                        "votes_exhausted": None,
                        "remarks": count.remarks or None,
                    }
                )

    # First preferences by polling place live in a separate workbook, keyed by a
    # sheet named for the division in every year except 2018, whose per-division
    # files use sheet names like "RO Return" and "v2".
    for doc in contest.get("files", {}).get("docs", []):
        path = root / doc
        if (
            "polling-place" not in path.name
            and "polling_place" not in path.name
        ):
            continue
        if "first-pref" not in path.name and "first_pref" not in path.name:
            continue
        sheets = parse_excel.parse_polling_place_results(path)
        key = _pick_sheet(sheets, division, path.name)
        if key is None:
            continue
        _emit_voting_centre_rows(sheets[key], block, tables)


def _match_ballot(surname: str, party_of: dict[str, str]) -> str | None:
    """Map a scrutiny-sheet surname column onto the full ballot name.

    The count export names columns by surname alone; every other source uses
    "SURNAME, Given". Where two candidates in one division share a surname the
    match is ambiguous, so it is left unresolved rather than guessed.
    """
    matches = [n for n in party_of if n.split(",")[0].strip() == surname]
    return matches[0] if len(matches) == 1 else None


def _pick_sheet(sheets: dict, division: str, filename: str) -> str | None:
    for name in sheets:
        if _norm(name) == _norm(division):
            return name
    if _norm(division) in _norm(filename) and len(sheets) == 1:
        return next(iter(sheets))
    return None


def _emit_voting_centre_rows(
    result: parse_excel.PollingPlaceResult,
    block: Row,
    tables: dict[str, list[Row]],
) -> None:
    for i, place in enumerate(result.places):
        if place.strip().lower() == "total":
            continue
        formal = result.formal[i] if i < len(result.formal) else None
        informal = result.informal[i] if i < len(result.informal) else None
        total = result.total[i] if i < len(result.total) else None
        for ballot, values in result.candidates.items():
            tables["result_voting_centre"].append(
                _strip_private(block)
                | {
                    "voting_centre_name": place,
                    "ballot_name": ballot,
                    "party_name": result.party_of.get(ballot),
                    "votes": values[i] if i < len(values) else None,
                    "votes_formal": formal,
                    "votes_informal": informal,
                    "votes_total": total,
                }
            )


# --------------------------------------------------------------------------------------
# Legislative Council
# --------------------------------------------------------------------------------------


def _legislative_council(
    root: Path,
    division: str,
    contest: Row,
    block: Row,
    tables: dict[str, list[Row]],
) -> None:
    files = contest["files"]
    inline = contest["inline"]
    page = (
        (root / files["page"]).read_text(errors="replace")
        if "page" in files
        else ""
    )
    fp_html = (
        (root / files["fp"]).read_text(errors="replace")
        if "fp" in files
        else page
    )
    dist_html = (
        (root / files["dist"]).read_text(errors="replace")
        if "dist" in files
        else page
    )
    _ = inline

    fp = parse_html.parse_lc_first_preferences(fp_html)
    dist = parse_html.parse_lc_distribution(dist_html)

    unopposed = constants.UNOPPOSED.value.get((block["election_id"], division))
    if unopposed and not fp.candidates:
        # No poll was held, so there is no result to publish beyond the fact of
        # the election. Emitting the candidate keeps the seat visible instead of
        # silently dropping the contest.
        ballot, party = unopposed
        tables["candidate"].append(
            _strip_private(block)
            | {
                "ballot_name": ballot,
                "candidate_surname": _split_name(ballot)[0],
                "candidate_given_names": _split_name(ballot)[1],
                "party_name": party,
                "is_elected": "yes",
                "election_order": "1",
                "candidate_status": "elected",
            }
        )
        tables["enrolment_turnout"].append(
            _strip_private(block)
            | {
                "enrolment": None,
                "votes_total": None,
                "votes_formal": None,
                "votes_informal": None,
                "percentage_turnout": None,
                "percentage_informal": None,
                "quota": None,
                "seats_to_elect": 1,
                "candidates_count": 1,
            }
        )
        return

    names = [c.ballot_name for c in fp.candidates] or [
        c.ballot_name for c in dist.candidates
    ]
    party = {c.ballot_name: c.party_name for c in fp.candidates}
    for c in dist.candidates:
        party.setdefault(c.ballot_name, c.party_name)

    final = dist.counts[-1].totals if dist.counts else []
    if fp.totals:
        fp_votes, formal, informal, total = fp.totals
    elif fp.places:
        # Some contests publish no TOTALS row — Elwick and McIntyre 2022 end with
        # a "% Formal Vote" line and a bare winner declaration instead. Summing
        # the venues reproduces it exactly, and without it the contest would
        # silently carry no result and no elected member.
        cols = list(zip(*(v[0] for v in fp.places.values()), strict=False))
        fp_votes = [sum(x for x in col if x is not None) for col in cols]
        formal = (
            sum(v[1] for v in fp.places.values() if v[1] is not None) or None
        )
        informal = (
            sum(v[2] for v in fp.places.values() if v[2] is not None) or None
        )
        total = (
            sum(v[3] for v in fp.places.values() if v[3] is not None) or None
        )
    else:
        fp_votes, formal, informal, total = [], None, None, None

    # The winner is the candidate holding the most votes at the final count. Where
    # a candidate takes an absolute majority on first preferences the TEC
    # publishes no distribution at all — the missing file is the result, not a
    # scrape failure — so the first-preference totals stand in.
    basis = final if final else fp_votes
    winner_idx = None
    if basis:
        pairs = [(v, i) for i, v in enumerate(basis) if v is not None]
        if pairs:
            winner_idx = max(pairs)[1]

    for i, ballot in enumerate(names):
        elected = winner_idx is not None and i == winner_idx
        tables["candidate"].append(
            _strip_private(block)
            | {
                "ballot_name": ballot,
                "candidate_surname": _split_name(ballot)[0],
                "candidate_given_names": _split_name(ballot)[1],
                "party_name": party.get(ballot),
                "is_elected": "yes" if elected else "no",
                "election_order": "1" if elected else None,
                "candidate_status": "elected" if elected else "excluded",
            }
        )
        if i < len(fp_votes):
            tables["result_district"].append(
                _strip_private(block)
                | {
                    "count_type": "first_preference",
                    "ballot_name": ballot,
                    "party_name": party.get(ballot),
                    "votes": fp_votes[i],
                    "percentage_formal_votes": _pct(fp_votes[i], formal),
                    "quotas": None,
                    "candidate_status": None,
                }
            )
        if i < len(final):
            tables["result_district"].append(
                _strip_private(block)
                | {
                    "count_type": "final_distribution",
                    "ballot_name": ballot,
                    "party_name": party.get(ballot),
                    "votes": final[i],
                    "percentage_formal_votes": _pct(final[i], formal),
                    "quotas": None,
                    "candidate_status": "elected" if elected else "excluded",
                }
            )

    for place, (votes, pf, pi, pt) in fp.places.items():
        for i, ballot in enumerate(names):
            tables["result_voting_centre"].append(
                _strip_private(block)
                | {
                    "voting_centre_name": place,
                    "ballot_name": ballot,
                    "party_name": party.get(ballot),
                    "votes": votes[i] if i < len(votes) else None,
                    "votes_formal": pf,
                    "votes_informal": pi,
                    "votes_total": pt,
                }
            )

    for count in dist.counts:
        for i, ballot in enumerate(names):
            tables["distribution_of_preferences"].append(
                _strip_private(block)
                | {
                    "count_number": count.count_number,
                    "ballot_name": ballot,
                    "party_name": party.get(ballot),
                    "votes_transferred": (
                        count.transferred[i]
                        if i < len(count.transferred)
                        else None
                    ),
                    "votes_progressive_total": (
                        count.totals[i] if i < len(count.totals) else None
                    ),
                    "votes_exhausted": count.exhausted,
                    "remarks": count.remarks or None,
                }
            )

    tables["enrolment_turnout"].append(
        _strip_private(block)
        | {
            "enrolment": fp.enrolment,
            "votes_total": total,
            "votes_formal": formal,
            "votes_informal": informal,
            "percentage_turnout": fp.turnout or _pct(total, fp.enrolment),
            "percentage_informal": _pct(informal, total),
            # One seat, so the Droop quota is an absolute majority of the formal
            # vote. The Legislative Council tables do not publish it directly.
            "quota": (formal // 2 + 1) if formal else None,
            "seats_to_elect": 1,
            "candidates_count": len(names),
        }
    )


# --------------------------------------------------------------------------------------
# Voting centres and dictionary
# --------------------------------------------------------------------------------------

_PP_KEYS = {
    "pollingplacename": "voting_centre_name",
    "locality": "locality",
    "premisename": "premise_name",
    "premiseaddress1": "premise_address",
    "premiseaddressline1": "premise_address",
    "premisepostcode": "postcode",
    "premisestate": "state_abbreviation",
    "locationwithinpremise": "location_within_premise",
    "disabledaccess": "disabled_access",
    "division": "district_name",
}


def _voting_centres(
    root: Path,
    entry: Row,
    year: int,
    election_id: str,
    tables: dict[str, list[Row]],
) -> None:
    """Read the event's polling place workbook, in either of its two layouts.

    Up to 2023 each division is its own sheet; from 2024 a single sheet carries a
    Division column instead.
    """
    seen: set[tuple[str, str]] = set()
    docs = {
        d
        for c in entry["contests"].values()
        for d in c.get("files", {}).get("docs", [])
    }
    for doc in sorted(docs):
        path = root / doc
        if "polling-places" not in path.name or "first-pref" in path.name:
            continue
        for record in parse_excel.parse_polling_place_list(path):
            row = {v: "" for v in _PP_KEYS.values()}
            for key, value in record.items():
                target = _PP_KEYS.get(_norm(key))
                if target:
                    row[target] = value
            if not row["district_name"]:
                row["district_name"] = record.get("_sheet", "")
            if not row["voting_centre_name"]:
                continue
            key2 = (row["voting_centre_name"], row["district_name"])
            if key2 in seen:
                continue
            seen.add(key2)
            tables["voting_centre"].append(
                {"year": year, "election_id": election_id}
                | {k: (v or None) for k, v in row.items()}
            )


_DICTIONARY = {
    ("chamber", "house_of_assembly"): "Casa de Assembleia",
    ("chamber", "legislative_council"): "Conselho Legislativo",
    ("government_level", "state"): "Estadual",
    (
        "contest_type",
        "house_of_assembly_division",
    ): "Divisão da Casa de Assembleia",
    (
        "contest_type",
        "legislative_council_division",
    ): "Divisão do Conselho Legislativo",
    ("voting_system", "hare_clark"): "Hare-Clark, voto único transferível",
    (
        "voting_system",
        "preferential",
    ): "Voto preferencial em divisão de cadeira única",
    ("election_type", "state_general"): "Eleição geral da Casa de Assembleia",
    (
        "election_type",
        "state_periodic",
    ): "Eleição periódica do Conselho Legislativo",
    ("election_type", "state_by_election"): "Eleição suplementar",
    ("count_type", "first_preference"): "Votos de primeira preferência",
    (
        "count_type",
        "final_distribution",
    ): "Votos após a distribuição completa de preferências",
    ("candidate_status", "elected"): "Eleita",
    ("candidate_status", "excluded"): "Excluída da contagem",
    (
        "candidate_status",
        "continuing",
    ): "Permanecia na contagem ao encerramento",
    ("is_elected", "yes"): "Sim",
    ("is_elected", "no"): "Não",
}

_DICT_TABLES = {
    "chamber": [
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "election",
    ],
    "government_level": [
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
    ],
    "contest_type": [
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
    ],
    "voting_system": [
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
    ],
    "election_type": ["election"],
    "count_type": ["result_district"],
    "candidate_status": ["candidate", "result_district"],
    "is_elected": ["candidate"],
}


def _dictionary() -> list[Row]:
    rows = []
    for (column, key), value in _DICTIONARY.items():
        for table in _DICT_TABLES[column]:
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": None,
                    "valor": value,
                }
            )
    return rows


# --------------------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------------------


def _write(tables: dict[str, list[Row]], output_root: Path) -> dict[str, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from pipelines.datasets.au_tas_tec_elections import schema

    counts: dict[str, int] = {}
    for table, rows in tables.items():
        names = schema.column_names(table)
        target = output_root / table
        # Clear the table's tree first. Parquet partitions are written per year and
        # are not overwritten when a year disappears from the input, so a rerun
        # that drops an election leaves its partition behind and the stale rows
        # keep passing every downstream check.
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        partitioned = "year" in schema.PARTITION_COLUMNS[table]

        groups: dict[Any, list[Row]] = {}
        for row in rows:
            groups.setdefault(
                row.get("year") if partitioned else None, []
            ).append(row)

        # Staging is all-STRING by house convention: the dbt model safe_casts every
        # column, and a typed external table here would collide with a later
        # pipeline's all-STRING overwrite. Cast through arrow rather than
        # astype(str), which renders NULL as the literal "nan".
        arrow_schema = pa.schema([pa.field(n, pa.string()) for n in names])
        for key, group in groups.items():
            columns = {
                n: pa.array(
                    [_as_text(r.get(n)) for r in group], type=pa.string()
                )
                for n in names
            }
            table_arrow = pa.Table.from_pydict(columns, schema=arrow_schema)
            if partitioned:
                out = target / f"year={key}"
                out.mkdir(parents=True, exist_ok=True)
                pq.write_table(
                    table_arrow, out / "data.parquet", compression="snappy"
                )
            else:
                pq.write_table(
                    table_arrow, target / "data.parquet", compression="snappy"
                )
        counts[table] = len(rows)
    return counts


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None
