"""
Main entry point for the br_tse_eleicoes pipeline.
Equivalent of build.do.

Usage:
    python build.py                  # run full pipeline
    python build.py candidates       # run a single step
    python build.py normalize        # run normalization + partitioning
    python build.py aggregate        # run aggregation only
    python build.py --list           # list available steps
"""

import sys
import time

from sub.campaign_finance import build_all as build_campaign_finance
from sub.candidates import build_all as build_candidates
from sub.parties import build_all as build_parties
from sub.results_mun_zone import build_all as build_results_mun_zone
from sub.results_section import build_all as build_results_section
from sub.results_state import build_all as build_results_state
from sub.vacancies import build_all as build_vacancies
from sub.voter_profile_mun_zone import (
    build_all as build_voter_profile_mun_zone,
)
from sub.voter_profile_polling_place import (
    build_all as build_voter_profile_polling_place,
)
from sub.voter_profile_section import build_all as build_voter_profile_section
from sub.voting_details_mun_zone import (
    build_all as build_voting_details_mun_zone,
)
from sub.voting_details_section import (
    build_all as build_voting_details_section,
)
from sub.voting_details_state import build_all as build_voting_details_state

# Phase 2 table builders (order matches build.do)
STEPS = {
    "candidates": build_candidates,
    "parties": build_parties,
    "voting_details_mun_zone": build_voting_details_mun_zone,
    "voting_details_section": build_voting_details_section,
    "voting_details_state": build_voting_details_state,
    "voter_profile_mun_zone": build_voter_profile_mun_zone,
    "voter_profile_section": build_voter_profile_section,
    "voter_profile_polling_place": build_voter_profile_polling_place,
    "vacancies": build_vacancies,
    "results_mun_zone": build_results_mun_zone,
    "results_section": build_results_section,
    "results_state": build_results_state,
    "campaign_finance": build_campaign_finance,
}


def _run_step(name: str, func):
    print(f"\n{'=' * 60}")
    print(f"  STEP: {name}")
    print(f"{'=' * 60}")
    t0 = time.time()
    func()
    elapsed = time.time() - t0
    print(f"  {name} done in {elapsed:.1f}s")


def run_normalize():
    from normalization_partition import build_all

    _run_step("normalize_partition", build_all)


def run_aggregate():
    from aggregation import build_all

    _run_step("aggregation", build_all)


def run_all():
    """Run the full pipeline."""
    t0 = time.time()

    # Phase 2: table builders
    for name, func in STEPS.items():
        _run_step(name, func)

    # Phase 3: normalization + partitioning
    run_normalize()

    # Phase 3: aggregation
    run_aggregate()

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE in {elapsed:.1f}s")
    print(f"{'=' * 60}")


def main():
    args = sys.argv[1:]

    if not args:
        run_all()
        return

    if args[0] == "--list":
        print("Available steps:")
        for name in STEPS:
            print(f"  {name}")
        print("  normalize")
        print("  aggregate")
        return

    name = args[0]
    if name in STEPS:
        _run_step(name, STEPS[name])
    elif name == "normalize":
        run_normalize()
    elif name == "aggregate":
        run_aggregate()
    else:
        print(f"Unknown step: {name}")
        print(f"Available: {', '.join(STEPS.keys())}, normalize, aggregate")
        sys.exit(1)


if __name__ == "__main__":
    main()
