"""Download the Minas Gerais source files from dados.mg.gov.br.

Resumable and integrity-checked. Both matter here: the CKAN endpoint truncates large
transfers often enough that a plain "file exists and is non-empty" resume check will
happily keep a half-written .csv.gz forever, and the failure only surfaces much later as
`IO Error: Input is not a GZIP stream` in the middle of a long clean.
"""

from __future__ import annotations

import argparse
import gzip
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import BROWSER_UA, INPUT_DIR, MG_CKAN, MG_PACKAGES

MG_INPUT = INPUT_DIR / "mg"
CHUNK = 1 << 20

# Most fact and per-exercise dimension files carry the year in the name, in two
# shapes: `ft_despesa_2002.csv.gz` and `contratos2022.csv`. Files with no year --
# the dimension tables -- are whole-table exports and are always fetched.
YEAR_RE = re.compile(r"(?<!\d)(?P<year>20\d{2})(?!\d)")


def is_intact(path: Path) -> bool:
    """True when the file exists and is fully readable.

    A truncated gzip decompresses cleanly for most of its length and only raises at the
    end, so the whole member has to be walked -- checking the header is not enough.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    if path.suffix != ".gz":
        return True
    try:
        with gzip.open(path, "rb") as fh:
            while fh.read(CHUNK):
                pass
        return True
    except (OSError, EOFError, gzip.BadGzipFile):
        return False


def resource_urls(session: requests.Session, package: str) -> dict[str, str]:
    r = session.get(MG_CKAN, params={"id": package}, timeout=120)
    r.raise_for_status()
    out = {}
    for res in r.json()["result"]["resources"]:
        url = res["url"]
        name = url.rsplit("/", 1)[-1]
        if name.endswith((".csv.gz", ".csv")):
            out[name] = url
    return out


def download(
    session: requests.Session, name: str, url: str, dest_dir: Path
) -> bool:
    dest = dest_dir / name
    if is_intact(dest):
        return True
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
    tmp.replace(dest)
    if not is_intact(dest):
        dest.unlink(missing_ok=True)
        return False
    return True


def main(retries: int = 3, years: set[int] | None = None) -> None:
    MG_INPUT.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    # dados.mg.gov.br returns 403 to a bare requests/curl User-Agent.
    session.headers.update({"User-Agent": BROWSER_UA})

    # Six dimension tables (dm_acao, dm_programa, dm_unidade_orc, dm_item_desp,
    # dm_elemento_desp, dm_tempo_diario) are published by BOTH `despesa` and
    # `compras_contratos` under the same file name, with different contents: the despesa
    # copy is a superset, because compras only lists the classifications that appear in a
    # purchase (dm_acao 409,170 B vs 397,037 B). They share one surrogate-id space, so the
    # superset serves both models.
    #
    # First package wins, and `MG_PACKAGES` is ordered `despesa` first for exactly that
    # reason. Made explicit here rather than left to dict iteration order, which would
    # otherwise be silently load-bearing.
    wanted: dict[str, str] = {}
    for package in MG_PACKAGES:
        for name, url in resource_urls(session, package).items():
            if name in wanted:
                print(
                    f"  [skip] {name}: already taken from an earlier package"
                )
                continue
            wanted[name] = url
    print(f"{len(wanted)} resources across {len(MG_PACKAGES)} packages")

    # Year-scoped refresh. A recurring run only needs the open exercise, but it needs
    # EVERY dimension table, because those are whole-table exports that get rewritten
    # in place -- keeping last week's copy would leave new surrogate ids unresolved.
    if years is not None:
        scoped = {}
        for name, url in wanted.items():
            found = {int(m.group("year")) for m in YEAR_RE.finditer(name)}
            if not found or found & years:
                scoped[name] = url
        print(f"  year filter {sorted(years)}: {len(scoped)} of {len(wanted)}")
        wanted = scoped

    pending = dict(wanted)
    for attempt in range(1, retries + 1):
        failed = {}
        for name, url in sorted(pending.items()):
            ok = False
            try:
                ok = download(session, name, url, MG_INPUT)
            except requests.RequestException as exc:
                print(f"  {name}: {type(exc).__name__}")
            if ok:
                continue
            failed[name] = url
            print(f"  {name}: incomplete, will retry")
        if not failed:
            break
        pending = failed
        print(f"attempt {attempt}: {len(failed)} to retry")
    else:
        print(f"STILL FAILING after {retries} attempts: {sorted(pending)}")
        raise SystemExit(1)

    total = sum(p.stat().st_size for p in MG_INPUT.glob("*.csv*"))
    print(f"OK: {len(wanted)} files, {total / 1e9:.2f} GB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument(
        "--year",
        type=int,
        action="append",
        help="restrict per-exercise files to this year; repeatable. "
        "Dimension tables are always fetched.",
    )
    args = ap.parse_args()
    main(retries=args.retries, years=set(args.year) if args.year else None)
