"""Recover the finance years that download.py could not reach in one pass.

The Wayback CDX endpoint throttles a process that queries it repeatedly and then
answers with an empty body, which download.py records as "no capture" even
though the year is archived. This walks only the years still missing, one at a
time with a long pause between them, and reports what each attempt actually saw
so a genuine absence is distinguishable from a throttle.
"""

from __future__ import annotations

import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import download as dl  # pyrefly: ignore [missing-import]

PAUSE_BETWEEN_YEARS = 20


def main() -> None:
    dl.FINANCE_DIR.mkdir(parents=True, exist_ok=True)
    missing = [
        year
        for year in sorted(dl.ARCHIVED_FINANCE, reverse=True)
        if not dl.workbook_path(year)
    ]
    print(f"missing: {missing}\n")

    recovered, still_missing = [], []
    for year in missing:
        slug = dl.ARCHIVED_FINANCE[year]
        stamps = dl.captures(
            f"education.gov.au/higher-education-publications/resources/{slug}"
        )
        print(f"{year}: {len(stamps)} capture(s) {stamps[:3]}")
        if not stamps:
            still_missing.append((year, "CDX returned nothing"))
            time.sleep(PAUSE_BETWEEN_YEARS)
            continue

        reason = "no workbook link on any capture"
        dest = dl.FINANCE_DIR / f"finance_{year}.xlsx"
        for stamp in stamps[:3]:
            page = dl.curl(
                f"https://web.archive.org/web/{stamp}/{dl.RESOURCES}/{slug}",
                timeout=120,
            )
            links = dl.workbook_links(page)
            print(f"   capture {stamp}: {len(links)} workbook link(s)")
            for link in links:
                suffix = ".xls" if link.lower().endswith("xls") else ".xlsx"
                dest = dl.FINANCE_DIR / f"finance_{year}{suffix}"
                dl.curl(
                    f"https://web.archive.org/web/{stamp}id_/{link}",
                    dest,
                    timeout=240,
                )
                if dl.is_workbook(dest):
                    print(
                        f"   -> saved {dest.name} ({dest.stat().st_size:,} bytes)"
                    )
                    break
                dest.unlink(missing_ok=True)
                reason = "workbook link did not return a workbook"
            if dl.is_workbook(dest):
                break
            time.sleep(5)

        if dl.is_workbook(dest):
            recovered.append(year)
        else:
            still_missing.append((year, reason))
        time.sleep(PAUSE_BETWEEN_YEARS)

    print(f"\nrecovered: {sorted(recovered)}")
    for year, reason in sorted(still_missing):
        print(f"still missing {year}: {reason}")


if __name__ == "__main__":
    main()
