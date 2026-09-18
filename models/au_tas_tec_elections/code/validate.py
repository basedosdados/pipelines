"""Cross-foot the cleaned tables against the TEC's own published results.

Row counts prove nothing about correctness. This script reproduces the seat
allocation from the dataset's own tables and compares it with the TEC's published
list of elected members, and checks that the polling-place votes add back up to
the division totals. On a sibling state the same check caught a "TOTAL FORMAL
VOTES" row parsed as a candidate, which doubled every district total while leaving
row counts entirely plausible.

Run: PYTHONPATH=. ~/.venvs/bd-pipelines-tas/bin/python \
        models/au_tas_tec_elections/code/validate.py
"""

from __future__ import annotations

import collections
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds

from pipelines.datasets.au_tas_tec_elections.constants import (
    constants,
    data_root,
)

# The TEC's own "Retiring member / Elected candidate" tables, from
# legislative-council/LCHistory/Summary/{2009-2017,2018-present}.html. External
# ground truth: not derived from anything this pipeline computes.
TEC_ELECTED = {
    ("lc2018", "hobart"): "Rob Valentine",
    ("lc2018", "prosser"): "Jane Howlett",
    ("lc2019", "montgomery"): "Leonie Hiscutt",
    ("lc2019", "nelson"): "Meg Webb",
    ("lc2019", "pembroke"): "Jo Siejka",
    ("lc2020", "huon"): "Bastian Seidel",
    ("lc2020", "rosevears"): "Jo Palmer",
    ("lc2021", "derwent"): "Craig Farrell",
    ("lc2021", "mersey"): "Michael Gaffney",
    ("lc2021", "windermere"): "Nick Duigan",
    ("lc2022", "elwick"): "Josh Willie",
    ("lc2022", "huon"): "Dean Harriss",
    ("lc2022", "mcintyre"): "Tania Rattray",
    ("lc2022pembroke", "pembroke"): "Luke Edmunds",
    ("lc2023", "launceston"): "Rosemary Armitage",
    ("lc2023", "murchison"): "Ruth Forrest",
    ("lc2023", "rumney"): "Sarah Lovell",
    ("lc2024", "elwick"): "Bec Thomas",
    ("lc2024", "hobart"): "Cassy O'Connor",
    ("lc2024", "prosser"): "Kerry Vincent",
    ("lc2025", "montgomery"): "Casey Hiscutt",
    ("lc2025", "nelson"): "Meg Webb",
    ("lc2025", "pembroke"): "Luke Edmunds",
    ("lc2026", "huon"): "Clare Glade-Wright",
    ("lc2026", "rosevears"): "Jo Palmer",
}


def load(root: Path, table: str) -> list[dict]:
    return ds.dataset(root / table, format="parquet").to_table().to_pylist()


def norm_person(name: str) -> str:
    """Compare "SURNAME, Given" against the TEC's "Given Surname" form."""
    surname, _, given = name.partition(",")
    return (
        f"{given.strip()} {surname.strip()}".lower()
        .replace("'", "")
        .replace("-", " ")
    )


def main() -> int:
    root = data_root() / "output"
    failures: list[str] = []
    notes: list[str] = []

    candidates = load(root, "candidate")
    turnout = load(root, "enrolment_turnout")
    district = load(root, "result_district")
    centre = load(root, "result_voting_centre")

    # 1. Seats filled per contest must equal the seats the division elects.
    seats_by_contest = {
        r["contest_id"]: int(r["seats_to_elect"]) for r in turnout
    }
    elected = collections.Counter(
        r["contest_id"] for r in candidates if r["is_elected"] == "yes"
    )
    print("=== seats filled per contest ===")
    for contest_id, expected in sorted(seats_by_contest.items()):
        got = elected.get(contest_id, 0)
        flag = "ok" if got == expected else "MISMATCH"
        if got != expected:
            failures.append(
                f"{contest_id}: {got} elected, division returns {expected}"
            )
        print(f"  {contest_id:26s} elected={got} expected={expected}  {flag}")

    # 2. The elected member must be the person the TEC says was elected.
    print("\n=== elected member vs the TEC's published record ===")
    by_contest: dict[str, list[str]] = collections.defaultdict(list)
    for r in candidates:
        if r["is_elected"] == "yes":
            by_contest[r["contest_id"]].append(r["ballot_name"])
    checked = 0
    for (election_id, division), expected_name in sorted(TEC_ELECTED.items()):
        contest_id = f"{election_id}-{division}"
        got = by_contest.get(contest_id, [])
        ok = len(got) == 1 and norm_person(
            got[0]
        ) == expected_name.lower().replace("'", "").replace("-", " ")
        checked += 1
        if not ok:
            failures.append(
                f"{contest_id}: dataset says {got}, TEC says {expected_name}"
            )
            print(f"  {contest_id:26s} MISMATCH got={got} tec={expected_name}")
        else:
            print(f"  {contest_id:26s} ok  {expected_name}")
    print(f"  {checked} Legislative Council contests checked")

    # 3. Polling-place votes must add back to the division first-preference total.
    print("\n=== polling place votes vs division totals ===")
    fp = {
        (r["contest_id"], r["ballot_name"]): r["votes"]
        for r in district
        if r["count_type"] == "first_preference" and r["votes"] is not None
    }
    summed: dict[tuple[str, str], int] = collections.defaultdict(int)
    for r in centre:
        if r["votes"] is not None:
            summed[(r["contest_id"], r["ballot_name"])] += int(r["votes"])
    contests_with_centres = {k[0] for k in summed}
    agree = disagree = 0
    bad_contests: collections.Counter = collections.Counter()
    for key, total in summed.items():
        want = fp.get(key)
        if want is None:
            continue
        if int(want) == total:
            agree += 1
        else:
            disagree += 1
            bad_contests[key[0]] += 1
    print(
        f"  {len(contests_with_centres)} contests carry polling-place results"
    )
    print(f"  {agree} candidate totals agree, {disagree} disagree")
    for contest_id, n in bad_contests.most_common(10):
        print(f"    {contest_id}: {n} candidates disagree")
    if disagree:
        notes.append(
            f"{disagree} candidate polling-place sums differ from the division total"
        )

    # 4. Formal votes must equal the sum of first preferences.
    print("\n=== formal votes vs sum of first preferences ===")
    fp_sum: dict[str, int] = collections.defaultdict(int)
    for r in district:
        if r["count_type"] == "first_preference" and r["votes"] is not None:
            fp_sum[r["contest_id"]] += int(r["votes"])
    ok = bad = 0
    for r in turnout:
        formal = r["votes_formal"]
        if formal is None or r["contest_id"] not in fp_sum:
            continue
        if int(formal) == fp_sum[r["contest_id"]]:
            ok += 1
        else:
            bad += 1
            print(
                f"    {r['contest_id']}: formal={formal} "
                f"sum(first preferences)={fp_sum[r['contest_id']]}"
            )
    print(f"  {ok} contests agree, {bad} disagree")
    if bad:
        failures.append(
            f"{bad} contests where formal votes != sum of first preferences"
        )

    # 5. The last count of the distribution must land on the same number the
    #    final-distribution rows report. These come from different sources for the
    #    House of Assembly — the count export spreadsheet and the results
    #    fragment — so agreement is a real check, not a tautology.
    print("\n=== last count of distribution vs final distribution total ===")
    dop = load(root, "distribution_of_preferences")
    final = {
        (r["contest_id"], r["ballot_name"]): r["votes"]
        for r in district
        if r["count_type"] == "final_distribution"
    }
    running: dict[tuple[str, str], Any] = {}
    for r in dop:
        running[(r["contest_id"], r["ballot_name"])] = r[
            "votes_progressive_total"
        ]
    ok = bad = 0
    for key, value in running.items():
        want = final.get(key)
        if want is None or value is None:
            continue
        if int(value) == int(want):
            ok += 1
        else:
            bad += 1
            print(f"    {key}: distribution={value} final={want}")
    print(f"  {ok} candidate totals agree, {bad} disagree")
    if bad:
        failures.append(
            f"{bad} candidates where the distribution and final totals differ"
        )

    # 6. Every distribution row must name a full candidate, not a bare surname.
    #    The count export labels its columns by surname alone; where a division
    #    has two candidates sharing one, the join is ambiguous and is left
    #    unresolved rather than guessed — so any bare surname here is a real gap.
    bare = [r for r in dop if "," not in (r["ballot_name"] or "")]
    print(
        f"\n=== distribution rows with an unresolved surname: {len(bare)} ==="
    )
    if bare:
        names = sorted({r["ballot_name"] for r in bare})
        print(f"    {names[:10]}")
        notes.append(
            f"{len(bare)} distribution rows carry a surname that matched no unique candidate"
        )

    # 7. Coverage summary.
    print("\n=== coverage ===")
    per_election: collections.Counter = collections.Counter()
    for r in candidates:
        per_election[r["election_id"]] += 1
    for election_id in constants.ELECTION_META.value:
        meta = constants.ELECTION_META.value[election_id]
        print(
            f"  {election_id:16s} {meta[3]}  {per_election.get(election_id, 0):4d} candidates"
        )

    print("\n" + "=" * 70)
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(f"  - {f}")
    else:
        print("all hard checks passed")
    for n in notes:
        print(f"NOTE: {n}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
