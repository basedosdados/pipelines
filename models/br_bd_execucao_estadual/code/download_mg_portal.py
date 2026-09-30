"""Download the Minas Gerais `portal_*` flat exports from GitHub.

These are CGE/DCTA's "NOVA CONSULTA" spreadsheets, converted to CSV and committed to
`github.com/transparencia-mg`. They are a different export of SIAD from the
`compras_contratos` dimensional model that `download_mg.py` fetches from
dados.mg.gov.br, and they are here for one reason above all: the dimensional model
anonymises the counterparty and these files do not.

Two practical consequences of the data living in Git rather than behind a portal:

  * No Brazilian-IP requirement and no User-Agent games. `dados.mg.gov.br` returns 403
    to a bare requests/curl UA (see `download_mg.py`) and is unreachable from a
    consumer VPN; raw.githubusercontent.com is neither.
  * A refresh is reproducible. Files are fetched at a pinned commit
    (`MG_PORTAL_REFS`), so a rebuild months later reads exactly the same bytes.
    `--ref main` takes the current tip instead, which is what you want when
    deliberately extending coverage.

DO NOT take column lists from the repos' `dataset/datapackage.json`. Those schemas are
generated from the upstream Excel and are stale: they declare `unnamed_*` columns that
do not exist in the published CSV, and for `portal_contratos` they also MISS a real
column (`indicador_fornecedor_estrangeiro`). Read the CSV header.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from pathlib import Path

import requests

from models.br_bd_execucao_estadual.code.constants import (
    BROWSER_UA,
    INPUT_DIR,
    MG_PORTAL_FIRST_YEAR,
    MG_PORTAL_IN_USE,
    MG_PORTAL_MES,
    MG_PORTAL_MONTHLY_FIRST,
    MG_PORTAL_MONTHLY_IN_USE,
    MG_PORTAL_MONTHLY_REPO,
    MG_PORTAL_MONTHLY_TABLES,
    MG_PORTAL_RAW,
    MG_PORTAL_REFS,
    MG_PORTAL_REPOS,
    MG_PORTAL_STATIC_IN_USE,
    MG_PORTAL_STATIC_REPOS,
    MG_PORTAL_STATIC_TABLES,
    MG_PORTAL_TABLES,
    MG_SEP,
)

MG_INPUT = INPUT_DIR / "mg"
CHUNK = 1 << 20


def is_intact(path: Path) -> bool:
    """True when the file exists and its header parses as the expected CSV dialect.

    A truncated transfer leaves a readable prefix, so size alone is not enough. The
    header is the cheap invariant: these files are semicolon-delimited with a BOM, so a
    single-field header means we got HTML (a 404 page) or a partial first line.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open(encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh, delimiter=MG_SEP), [])
    except (OSError, UnicodeDecodeError, StopIteration):
        return False
    return len(header) > 1


def fetch(session: requests.Session, url: str, dest: Path) -> bool:
    """Stream one CSV to `dest`. Returns False when the year does not exist (404)."""
    with session.get(url, stream=True, timeout=300) as r:
        if r.status_code == 404:
            return False
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
        tmp.replace(dest)
    return True


def main(
    only: str | None = None,
    ref: str | None = None,
    last_year: int | None = None,
) -> None:
    MG_INPUT.mkdir(parents=True, exist_ok=True)
    known = {
        **MG_PORTAL_TABLES,
        **MG_PORTAL_MONTHLY_TABLES,
        **MG_PORTAL_STATIC_TABLES,
    }
    stems = (
        [only]
        if only
        else [
            *MG_PORTAL_IN_USE,
            *MG_PORTAL_MONTHLY_IN_USE,
            *MG_PORTAL_STATIC_IN_USE,
        ]
    )
    unknown = [s for s in stems if s not in known]
    if unknown:
        raise SystemExit(f"unknown stem(s) {unknown}; known: {sorted(known)}")

    # The current year is always attempted: the repos are refreshed in-year, so the
    # latest file is partial by nature rather than missing.
    end = last_year or dt.date.today().year
    session = requests.Session()
    session.headers["User-Agent"] = BROWSER_UA

    # Whole-table files: one fetch each, no period in the name.
    for stem in (s for s in stems if s in MG_PORTAL_STATIC_TABLES):
        repo = MG_PORTAL_STATIC_REPOS[stem]
        use_ref = ref or MG_PORTAL_REFS[repo]
        dest = MG_INPUT / f"{stem}.csv"
        if is_intact(dest):
            print(f"  {stem} ({repo} @ {use_ref[:8]}): already present")
            continue
        url = MG_PORTAL_RAW.format(repo=repo, ref=use_ref, stem=stem, year="")
        ok = fetch(session, url, dest)
        print(
            f"  {stem} ({repo} @ {use_ref[:8]}): {'downloaded' if ok else 'NOT FOUND'}"
        )

    for stem in (s for s in stems if s in MG_PORTAL_MONTHLY_TABLES):
        use_ref = ref or MG_PORTAL_REFS[MG_PORTAL_MONTHLY_REPO]
        got = skipped = absent = 0
        y0, m0 = MG_PORTAL_MONTHLY_FIRST
        today = dt.date.today()
        for year in range(y0, end + 1):
            for month in range(1, 13):
                if (year, month) < (y0, m0) or (year, month) > (
                    today.year,
                    today.month,
                ):
                    continue
                token = f"{MG_PORTAL_MES[month - 1]}{year % 100:02d}"
                dest = MG_INPUT / f"{stem}{token}.csv"
                if is_intact(dest):
                    skipped += 1
                    continue
                url = MG_PORTAL_RAW.format(
                    repo=MG_PORTAL_MONTHLY_REPO,
                    ref=use_ref,
                    stem=stem,
                    year=token,
                )
                if fetch(session, url, dest):
                    got += 1
                else:
                    absent += 1
        print(
            f"  {stem} ({MG_PORTAL_MONTHLY_REPO} @ {use_ref[:8]}): "
            f"{got} downloaded, {skipped} already present, {absent} not published"
        )

    for stem in (s for s in stems if s in MG_PORTAL_TABLES):
        repo = MG_PORTAL_REPOS[stem]
        use_ref = ref or MG_PORTAL_REFS[repo]
        got = skipped = absent = 0
        for year in range(MG_PORTAL_FIRST_YEAR, end + 1):
            dest = MG_INPUT / f"{stem}{year}.csv"
            if is_intact(dest):
                skipped += 1
                continue
            url = MG_PORTAL_RAW.format(
                repo=repo, ref=use_ref, stem=stem, year=year
            )
            if fetch(session, url, dest):
                got += 1
            else:
                absent += 1
        print(
            f"  {stem} ({repo} @ {use_ref[:8]}): "
            f"{got} downloaded, {skipped} already present, {absent} not published"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--only",
        help="one stem of the annual or monthly maps (default: those in use)",
    )
    ap.add_argument(
        "--ref",
        help="git ref to fetch at; overrides the pinned SHA. Use 'main' for the tip.",
    )
    ap.add_argument("--last-year", type=int, help="last year to attempt")
    args = ap.parse_args()
    main(only=args.only, ref=args.ref, last_year=args.last_year)
