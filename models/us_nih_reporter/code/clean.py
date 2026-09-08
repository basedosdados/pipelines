"""One-shot bootstrap: download and clean the full us_nih_reporter corpus.

Calls the same transform the recurring pipeline calls, so the two cannot drift.
Writes partitioned, all-STRING parquet under ``$NIH_REPORTER_DATA_DIR/output``.

Run: uv run python models/us_nih_reporter/code/clean.py [--skip-download]
     uv run python models/us_nih_reporter/code/clean.py --fiscal 2024 2025
"""

import argparse
import sys
import time

import requests
from common import (
    CALENDAR_YEARS,
    FISCAL_YEARS,
    INPUT,
    OUTPUT,
    build_dicionario,
    clean_abstract_year,
    clean_clinical_studies,
    clean_patents,
    clean_project_year,
    clean_publication_link_year,
    clean_publication_year,
    download_family,
    download_funding_supplement,
    load_funding_supplement,
)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--skip-download", action="store_true")
    p.add_argument("--fiscal", type=int, nargs="*", default=None)
    p.add_argument("--calendar", type=int, nargs="*", default=None)
    p.add_argument(
        "--skip-all-year",
        action="store_true",
        help="skip patents and clinical studies, which carry no year",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    fiscal = args.fiscal if args.fiscal is not None else FISCAL_YEARS
    calendar = args.calendar if args.calendar is not None else CALENDAR_YEARS

    if not args.skip_download:
        session = requests.Session()
        print("downloading ...", flush=True)
        download_funding_supplement(INPUT, session)
        download_family("projects", fiscal, INPUT, session)
        download_family("abstracts", fiscal, INPUT, session)
        download_family("publications", calendar, INPUT, session)
        download_family("linktables", calendar, INPUT, session)
        if not args.skip_all_year:
            download_family("patents", None, INPUT, session)
            download_family("clinicalstudies", None, INPUT, session)

    totals = {
        t: 0
        for t in (
            "project",
            "project_abstract",
            "publication",
            "publication_link",
        )
    }
    supplement = load_funding_supplement(INPUT)
    print(f"funding supplement rows: {len(supplement)}", flush=True)

    for year in fiscal:
        t0 = time.time()
        n_prj = clean_project_year(year, INPUT, OUTPUT, supplement)
        n_abs = clean_abstract_year(year, INPUT, OUTPUT)
        totals["project"] += n_prj
        totals["project_abstract"] += n_abs
        print(
            f"FY{year} project={n_prj:>7} abstract={n_abs:>7} "
            f"({time.time() - t0:.1f}s)",
            flush=True,
        )

    for year in calendar:
        t0 = time.time()
        n_pub = clean_publication_year(year, INPUT, OUTPUT)
        n_lnk = clean_publication_link_year(year, INPUT, OUTPUT)
        totals["publication"] += n_pub
        totals["publication_link"] += n_lnk
        print(
            f"CY{year} publication={n_pub:>7} link={n_lnk:>7} "
            f"({time.time() - t0:.1f}s)",
            flush=True,
        )

    if not args.skip_all_year:
        totals["patent_link"] = clean_patents(INPUT, OUTPUT)
        totals["clinical_study_link"] = clean_clinical_studies(INPUT, OUTPUT)
        print(
            f"patent_link={totals['patent_link']} "
            f"clinical_study_link={totals['clinical_study_link']}",
            flush=True,
        )

    totals["dicionario"] = build_dicionario(OUTPUT)
    print(f"dicionario={totals['dicionario']}", flush=True)

    print("\n=== totals ===")
    for k, v in totals.items():
        print(f"  {k:<22} {v:>10,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
