"""Turn hand-downloaded TCE-MG per-municipality files into packages `clean_mg.py` reads.

WHY THIS EXISTS. Every TCE-MG route is behind one gateway that requires a session token
with a 120-minute life, so the backfill cannot be scheduled and is done by hand. The
portal offers two shapes of the same data:

    bulk, per exercise     `2025_DESPESAS.zip`            583 MB - 1.6 GB
    per municipality       `SICOM.2025.3100104.despesa.zip`  ~1-2 MB

The bulk route is the one that fails. The portal's UI buffers the whole archive in page
memory and writes the file only when the transfer finishes, with no resume; at the ~80
KB/s the source sustains, a multi-gigabyte transfer has hours in which to be cut. Pulling
exercise 2025 that way produced a 54.8 MB "EMPENHOS" of 583 MB (191 of 853
municipalities) and a 52.4 MB "DESPESAS" of 1,607 MB (165 of 853) -- both with a valid
PK header, a plausible name, and no central directory, both reported as completed
downloads.

The per-municipality files avoid that: each is seconds rather than hours, so there is no
long connection to drop, and each can be checked and re-fetched on its own.

THE LUCKY PART. The per-municipality file name is byte-identical to the member name
inside the bulk package -- `SICOM.<exercicio>.<ibge7>.<categoria>.zip`, exactly
`clean_mg.py`'s NESTED_RE. So they need no renaming and no cleaner change: collected into
a `<label>_<year>.zip` they are indistinguishable from the package download_mg.py writes.

WHAT IS CHECKED, AND WHY EACH CHECK EARNS ITS PLACE.

1.  **Every file parses as a zip.** The central directory is the one part that cannot be
    written until the whole stream has arrived, so parsing it is the proof a download
    completed. This is the check the browser does not do.
2.  **Every file carries the CSV members its category promises.** A zip can be whole and
    still be the wrong thing -- the gateway answers a bad session with HTTP 200 and an
    error body.
3.  **Coverage is reported against 853, and missing municipalities are NAMED.** A count
    cannot see which ones are absent, and the absent ones are never a random sample: in
    Ceará the mirror was missing Fortaleza, Sobral and Maracanaú, which is 3 of 184 by
    count and a fifth of the state by rows.

Nothing is repacked unless its own checks pass, so a half-finished download directory
produces a smaller package rather than a corrupt one.

Usage:
    uv run python models/world_wb_mides/code/stage_mg.py --source ~/Downloads
    uv run python models/world_wb_mides/code/stage_mg.py --source ~/Downloads --write
"""

from __future__ import annotations

import argparse
import collections
import re
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import INPUT_DIR, MG_MUNICIPALITIES

MG_INPUT = INPUT_DIR / "mg"

# `SICOM.<exercicio>.<ibge7>.<categoria>.zip` -- the same shape clean_mg.py's NESTED_RE
# matches, because these ARE the members of the bulk package.
PER_MUNICIPALITY = re.compile(
    r"^SICOM\.(?P<year>\d{4})\.(?P<ibge>\d{7})\.(?P<cat>[^.]+)\.zip$"
)
# Category -> the package label clean_mg.py expects, and the CSV phases it will look for.
CATEGORIES = {
    "empenho": ("empenhos", ("empenho", "rsp")),
    "despesa": ("despesas", ("liquidacao", "pagamento")),
}


def inspect(path: Path) -> tuple[bool, str, list[str]]:
    """Is this download whole, and does it hold what its name claims?"""
    match = PER_MUNICIPALITY.match(path.name)
    if not match:
        return False, "name off convention", []
    category = match.group("cat")
    if category not in CATEGORIES:
        return False, f"category {category!r} is not one MiDES reads", []
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.namelist()
    except zipfile.BadZipFile as exc:
        # The failure this whole module exists for. Say the size, because a truncated
        # file looks entirely normal in a directory listing.
        return (
            False,
            f"TRUNCATED -- {path.stat().st_size:,} bytes, no central directory ({exc})",
            [],
        )
    _, phases = CATEGORIES[category]
    missing = [
        phase
        for phase in phases
        if not any(m.endswith(f".{category}.{phase}.csv") for m in members)
    ]
    if missing:
        return False, f"no CSV member for {missing}", members
    return True, "ok", members


def main(source: Path, write: bool, years: set[int] | None) -> None:
    files = sorted(source.glob("SICOM.*.zip"))
    if not files:
        raise SystemExit(
            f"no SICOM.*.zip under {source}. Downloads are named "
            f"SICOM.<exercicio>.<ibge7>.<categoria>.zip; point --source at the "
            f"directory holding them."
        )

    good: dict[tuple[str, int], dict[str, Path]] = collections.defaultdict(
        dict
    )
    broken: list[tuple[Path, str]] = []
    for path in files:
        match = PER_MUNICIPALITY.match(path.name)
        if match is None:
            # Was an AttributeError three lines down. A stray file in the source
            # directory is a thing to report, not a crash.
            broken.append(
                (
                    path,
                    "filename is not SICOM.<exercicio>.<ibge7>.<categoria>.zip",
                )
            )
            continue
        if years and int(match.group("year")) not in years:
            continue
        ok, why, _ = inspect(path)
        if not ok:
            broken.append((path, why))
            continue
        good[(match.group("cat"), int(match.group("year")))][
            match.group("ibge")
        ] = path

    print(f"{len(files):,} files under {source}\n")
    if broken:
        print(f"REJECTED {len(broken)}:")
        for path, why in broken[:20]:
            print(f"  {path.name}: {why}")
        if len(broken) > 20:
            print(f"  ... and {len(broken) - 20} more")
        print("  Re-download these; they are incomplete, not merely odd.\n")

    print(f"{'package':22} {'municipalities':>18}  {'status'}")
    for (category, year), members in sorted(good.items()):
        label, _ = CATEGORIES[category]
        share = len(members) / MG_MUNICIPALITIES
        print(
            f"{label + '_' + str(year) + '.zip':22} "
            f"{len(members):>7,}/{MG_MUNICIPALITIES:<9} "
            f"{'COMPLETE' if len(members) == MG_MUNICIPALITIES else f'{share:.1%}'}"
        )

    if not write:
        print(
            "\nNothing written. Re-run with --write to build the packages, which is "
            "worth doing only once a package reads COMPLETE -- clean_mg.py has no way "
            "to tell a package short of 200 municipalities from a whole one."
        )
        return

    MG_INPUT.mkdir(parents=True, exist_ok=True)
    print()
    for (category, year), members in sorted(good.items()):
        label, _ = CATEGORIES[category]
        dest = MG_INPUT / f"{label}_{year}.zip"
        tmp = dest.with_suffix(".part")
        # ZIP_STORED, not DEFLATE: every member is itself a compressed zip, so deflating
        # again costs minutes of CPU per package and saves nothing. The bulk packages the
        # portal serves are built the same way.
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_STORED) as package:
            for ibge in sorted(members):
                package.write(members[ibge], arcname=members[ibge].name)
        tmp.replace(dest)
        print(
            f"  {dest.name}: {len(members):,} municipalities, "
            f"{dest.stat().st_size / 1e6:,.0f} MB -> {dest}"
        )
    print(
        f"\nNow run:  MIDES_DATA_DIR={INPUT_DIR.parent} python "
        f"models/world_wb_mides/code/clean_mg.py"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--source",
        type=Path,
        default=Path.home() / "Downloads",
        help="directory holding the hand-downloaded SICOM.*.zip files",
    )
    ap.add_argument(
        "--year", type=int, action="append", help="restrict to this exercise"
    )
    ap.add_argument(
        "--write",
        action="store_true",
        help="build the packages; without it, only report",
    )
    args = ap.parse_args()
    main(
        source=args.source.expanduser(),
        write=args.write,
        years=set(args.year) if args.year else None,
    )
