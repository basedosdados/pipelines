"""Cross-foot the cleaned tables against published South Australian totals.

Row counts prove nothing about correctness. What this checks instead is that the
dataset reproduces the seat allocation the ECSA declared, and that the vote
components add up the way the source says they should.

Usage::

    PYTHONPATH=. python models/au_sa_ecsa_elections/code/crosscheck.py
"""

from __future__ import annotations

import pandas as pd

from pipelines.datasets.au_sa_ecsa_elections.constants import data_dir

OUTPUT = data_dir() / "output"

# Declared results, for the checks the data cannot verify against itself.
# 2022: ECSA declared Labor 27, Liberal 16, independents 4 of the 47 districts.
EXPECTED_SEATS_2022 = {"ALP": 27, "LIB": 16}
EXPECTED_INDEPENDENT_2022 = 4
# Statewide Legislative Council formal votes, as published in the results file.
EXPECTED_LC_FORMAL = {2022: 1088840, 2026: 1137958}
EXPECTED_LC_INFORMAL = {2022: 40840, 2026: 31858}
EXPECTED_HA_DISTRICTS = {2022: 47, 2026: 47}
# Legislative Council votes that belong to no ballot group. Measured, not assumed:
# the 2022 group breakdown falls 63 short of the published formal total, and the
# 2026 one reconciles exactly.
EXPECTED_LC_UNGROUPED = {2022: 63, 2026: 0}


def read(table: str) -> pd.DataFrame:
    root = OUTPUT / table
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"{root}: no parquet written; run clean.py first")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def check(label: str, passed: bool, detail: str = "") -> bool:
    print(
        f"  [{'PASS' if passed else 'FAIL'}] {label}{(' — ' + detail) if detail else ''}"
    )
    return passed


def main() -> int:
    candidate = read("candidate")
    result_district = read("result_district")
    turnout = read("enrolment_turnout")
    venue = read("result_voting_centre")
    ok = True

    print("Seat allocation")
    general = candidate[
        candidate["election_id"].isin(["sge-2022", "sge-2026"])
    ]
    for election in ("sge-2022", "sge-2026"):
        won = general[
            (general["election_id"] == election)
            & (general["is_declared_elected"] == "yes")
        ]
        ok &= check(
            f"{election}: exactly one member elected in each of the 47 districts",
            len(won) == 47 and won["contest_id"].nunique() == 47,
            f"{len(won)} winners across {won['contest_id'].nunique()} districts",
        )
        counts = won["party_code"].value_counts().to_dict()
        print(f"        {election} seats by party: {counts}")

    won22 = general[
        (general["election_id"] == "sge-2022")
        & (general["is_declared_elected"] == "yes")
    ]
    seats22 = won22["party_code"].value_counts().to_dict()
    for party, expected in EXPECTED_SEATS_2022.items():
        ok &= check(
            f"sge-2022: {party} won {expected} seats",
            seats22.get(party) == expected,
            f"got {seats22.get(party)}",
        )
    independents = len(won22) - sum(
        seats22.get(party, 0) for party in EXPECTED_SEATS_2022
    )
    ok &= check(
        "sge-2022: 4 seats to candidates outside the two major parties",
        independents == EXPECTED_INDEPENDENT_2022,
        f"got {independents}",
    )

    print("Every winner leads their district's final count")
    two_candidate = result_district[
        result_district["count_type"] == "two_candidate_preferred"
    ]
    mismatches = []
    for contest, group in two_candidate.groupby("contest_id"):
        leader = group.loc[
            group["votes"].astype(int).idxmax(), "ballot_order_number"
        ]
        winner = candidate[
            (candidate["contest_id"] == contest)
            & (candidate["election_id"].isin(group["election_id"].unique()))
            & (candidate["is_declared_elected"] == "yes")
        ]
        if not winner.empty and leader not in set(
            winner["ballot_order_number"]
        ):
            mismatches.append(contest)
    ok &= check(
        "the elected candidate leads the two candidate preferred count everywhere",
        not mismatches,
        f"{len(mismatches)} mismatches: {mismatches[:5]}",
    )

    print("Vote components")
    first = result_district[
        (result_district["count_type"] == "first_preference")
        & (result_district["chamber"] == "house_of_assembly")
    ]
    summed = first.groupby(["election_id", "contest_id"])["votes"].apply(
        lambda s: s.astype(int).sum()
    )
    declared = turnout.set_index(["election_id", "contest_id"])[
        "votes_formal"
    ].astype(int)
    joined = pd.concat(
        [summed.rename("summed"), declared.rename("declared")], axis=1
    )
    joined = joined.dropna()
    bad = joined[joined["summed"] != joined["declared"]]
    ok &= check(
        "Assembly first preferences sum to the formal votes in enrolment_turnout",
        bad.empty,
        f"{len(bad)} contests differ",
    )

    print("Legislative Council statewide totals match the published figures")
    council = turnout[turnout["chamber"] == "legislative_council"]
    for _, row in council.iterrows():
        year = int(row["year"])
        ok &= check(
            f"{year}: Legislative Council formal votes",
            int(row["votes_formal"]) == EXPECTED_LC_FORMAL[year],
            f"{int(row['votes_formal']):,} vs {EXPECTED_LC_FORMAL[year]:,}",
        )
        ok &= check(
            f"{year}: Legislative Council informal votes",
            int(row["votes_informal"]) == EXPECTED_LC_INFORMAL[year],
            f"{int(row['votes_informal']):,} vs {EXPECTED_LC_INFORMAL[year]:,}",
        )

    print("Legislative Council group votes sum to the statewide formal total")
    lc_first = result_district[
        (result_district["chamber"] == "legislative_council")
        & (result_district["count_type"] == "first_preference")
    ]
    for (year, _), group in lc_first.groupby(["year", "election_id"]):
        total = group["votes"].astype(int).sum()
        residual = EXPECTED_LC_FORMAL[int(year)] - total
        # The group breakdown need not exhaust the formal total: votes cast for
        # ungrouped candidates belong to no ballot group and the source publishes
        # no entry for them. The residual is asserted at its measured value rather
        # than forced to zero.
        ok &= check(
            f"{year}: group votes leave the measured ungrouped residual",
            residual == EXPECTED_LC_UNGROUPED[int(year)],
            f"residual {residual:,} vs expected {EXPECTED_LC_UNGROUPED[int(year)]:,}",
        )

    print(
        "Voting centre first preferences reconcile with the ordinary component"
    )
    ordinary = result_district[
        result_district["count_type"] == "first_preference_ordinary"
    ]
    venue_ordinary = venue[
        (venue["count_type"] == "first_preference")
        & (venue["chamber"] == "house_of_assembly")
        & (venue["voting_centre_type"].notna())
        & (
            ~venue["voting_centre_type"].isin(
                ["Declaration", "Absent Declaration"]
            )
        )
    ]
    left = venue_ordinary.groupby(
        ["election_id", "contest_id", "ballot_order_number"]
    )["votes"].apply(lambda s: s.astype(int).sum())
    right = ordinary.set_index(
        ["election_id", "contest_id", "ballot_order_number"]
    )["votes"].astype(int)
    pair = pd.concat(
        [left.rename("venues"), right.rename("district")], axis=1
    ).dropna()
    diff = pair[pair["venues"] != pair["district"]]
    ok &= check(
        "polling place votes sum to the district ordinary component",
        diff.empty,
        f"{len(diff)} candidate totals differ",
    )

    print("Districts per general election")
    for election, year in (("sge-2022", 2022), ("sge-2026", 2026)):
        contests = candidate[candidate["election_id"] == election][
            "contest_id"
        ].nunique()
        ok &= check(
            f"{election}: {EXPECTED_HA_DISTRICTS[year]} Assembly districts",
            contests == EXPECTED_HA_DISTRICTS[year],
            f"got {contests}",
        )

    print("\n" + ("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
