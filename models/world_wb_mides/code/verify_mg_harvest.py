"""Audit a completed TCE-MG harvest against what the portal says it published.

The harvester verifies each file as it lands. That is not the same as verifying
the SET of files: a unit that was never enumerated, or never attempted, leaves no
trace in the ledger and no gap that per-file checking can see. A harvest can be
100% successful on every file it tried and still be missing a whole exercise.

So this re-reads the portal's own per-exercise counts and reconciles three views:

  1. what `buscarCategoriaDownload` advertises  (qtdArquivos, per year/category)
  2. what the manifest enumerated               (per year/category)
  3. what is actually on disk and opens as a zip

Disagreements are the point. A municipality that genuinely filed nothing is
absent from (1), (2) and (3) alike and is not a gap -- in 2017 only 696 of 853
municipalities filed despesa. A municipality present in (2) but missing from (3)
is a real gap; one in (1) but not (2) means enumeration missed it.

Usage:
    python verify_mg_harvest.py                 # offline: manifest vs disk
    python verify_mg_harvest.py --portal        # also re-query the portal
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import BROWSER_UA, MG_API, MG_STATIC_BEARER

# pyrefly: ignore [missing-import]  # sibling module via sys.path
from harvest_mg import (
    CATEGORIES,
    LEDGER,
    MANIFEST,
    MG_INPUT,
    TokenFile,
    ca_bundle,
)

# The portal's bulk display labels, mapped to the per-file slugs the harvester
# uses. Needed only for --portal, because the two endpoints disagree on spelling.
LABEL_TO_SLUG = {
    "Despesas": "despesa",
    "Empenhos": "empenho",
    "Contratos": "contrato",
    "Licitações": "licitacao",
}


def portal_counts(token_file: TokenFile, years) -> dict[tuple[int, str], int]:
    session = requests.Session()
    session.verify = ca_bundle()
    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "*/*",
            "Authorization": f"Bearer {MG_STATIC_BEARER}",
        }
    )
    counts: dict[tuple[int, str], int] = {}
    for year in years:
        response = session.get(
            MG_API + "/buscarCategoriaDownload",
            params={"exercicio": year, "origem": "SICOM"},
            headers={"AuthorizationProxy": f"token {token_file.get()}"},
            timeout=120,
        )
        response.raise_for_status()
        for entry in response.json():
            slug = LABEL_TO_SLUG.get(str(entry.get("categoria")))
            if slug:
                counts[(year, slug)] = int(entry.get("qtdArquivos") or 0)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--portal", action="store_true")
    parser.add_argument(
        "--token-file", default=os.environ.get("MG_TOKEN_FILE")
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="decompress every member (slow; TLS already rules out bit rot)",
    )
    args = parser.parse_args()

    if not MANIFEST.exists():
        raise SystemExit(f"no manifest at {MANIFEST} -- nothing harvested yet")
    manifest = json.loads(MANIFEST.read_text())
    entries = manifest.get("entries", {})

    # `found` maps municipality -> list of files, and a municipality can publish
    # more than one. Counting municipalities and comparing that with files on
    # disk lets a surplus in one municipality hide a gap in another, so keep
    # both counts.
    planned: dict[tuple[int, str], set[str]] = {}
    planned_files: dict[tuple[int, str], int] = {}
    for key, found in entries.items():
        year, categoria = key.split("|")
        planned[(int(year), categoria)] = set(found)
        planned_files[(int(year), categoria)] = sum(
            len(files) for files in (found or {}).values()
        )

    ledger_ok: dict[tuple[int, str], set[str]] = defaultdict(set)
    if LEDGER.exists():
        for line in LEDGER.read_text().splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("status") == "ok":
                ledger_ok[(record["year"], record["categoria"])].add(
                    record["municipio"]
                )

    # Pairs `bulk_mg.py` fetched whole never reach the manifest -- `harvest_mg.py`
    # skips enumerating them by design. Iterating over `planned` alone therefore
    # audited nothing for those pairs while still printing the final
    # "harvest complete and verified". Take the union of every view instead:
    # the manifest, the portal, and the directories that exist on disk.
    on_disk_keys: set[tuple[int, str]] = set()
    for directory in MG_INPUT.glob("*/*"):
        if not directory.is_dir() or not directory.parent.name.isdigit():
            continue
        if directory.name in CATEGORIES:
            on_disk_keys.add((int(directory.parent.name), directory.name))
    years = sorted({y for y, _ in set(planned) | on_disk_keys})
    advertised: dict[tuple[int, str], int] = {}
    if args.portal:
        if not args.token_file:
            raise SystemExit("--portal needs --token-file or $MG_TOKEN_FILE")
        advertised = portal_counts(TokenFile(Path(args.token_file)), years)

    print(
        f"{'year':<6}{'categoria':<11}{'portal':>7}{'enum':>7}{'ok':>7}"
        f"{'ondisk':>8}{'bad':>5}{'GB':>7}  status"
    )
    total_bad: list[str] = []
    unverified: list[str] = []
    total_missing = 0
    total_bytes = 0
    total_files = 0
    for year in years:
        for categoria in CATEGORIES:
            key = (year, categoria)
            if (
                key not in planned
                and key not in advertised
                and key not in on_disk_keys
            ):
                continue
            directory = MG_INPUT / str(year) / categoria
            on_disk = (
                sorted(directory.glob("*.zip")) if directory.exists() else []
            )
            bad = 0
            size = 0
            for path in on_disk:
                size += path.stat().st_size
                try:
                    with zipfile.ZipFile(path) as archive:
                        if args.deep and archive.testzip() is not None:
                            raise zipfile.BadZipFile("member failed CRC")
                        archive.namelist()
                except Exception as exc:
                    bad += 1
                    total_bad.append(f"{path.relative_to(MG_INPUT)}: {exc}")
            enumerated = len(planned.get(key, ()))
            # Compare FILES with FILES. Where the pair was never enumerated --
            # `bulk_mg.py` took it whole -- fall back to the portal's own count,
            # and say so when there is nothing to compare against rather than
            # reporting OK.
            expected_files = planned_files.get(key)
            source = "manifest"
            if expected_files is None:
                expected_files = advertised.get(key)
                source = "portal"
            confirmed = len(ledger_ok.get(key, ()))
            missing = (
                expected_files - len(on_disk)
                if expected_files is not None
                else 0
            )
            total_missing += max(missing, 0)
            total_bytes += size
            total_files += len(on_disk)
            flag = "OK"
            if bad:
                flag = f"{bad} CORRUPT"
            elif expected_files is None:
                flag = "UNVERIFIED (not enumerated; re-run with --portal)"
                unverified.append(f"{year} {categoria}")
            elif missing > 0:
                flag = f"{missing} MISSING vs {source}"
            elif (
                key in advertised
                and key in planned_files
                and advertised[key] != expected_files
            ):
                flag = f"enum {expected_files} vs portal {advertised[key]}"
            print(
                f"{year:<6}{categoria:<11}{advertised.get(key, '-'):>7}"
                f"{enumerated:>7}{confirmed:>7}{len(on_disk):>8}{bad:>5}"
                f"{size / 1e9:>7.2f}  {flag}"
            )

    print(
        f"\nTOTAL {total_files:,} files, {total_bytes / 1e9:.2f} GB, "
        f"{total_missing:,} missing, {len(total_bad)} corrupt"
    )
    for line in total_bad[:20]:
        print(f"  CORRUPT {line}")
    if unverified:
        print(
            f"  {len(unverified)} pair(s) had no count to check against: "
            f"{', '.join(unverified[:8])}"
        )
    if total_bad or total_missing:
        raise SystemExit(1)
    if unverified:
        raise SystemExit(
            "harvest looks intact, but the pairs above were not verified "
            "against any count -- re-run with --portal"
        )
    print("harvest complete and verified")


if __name__ == "__main__":
    main()
