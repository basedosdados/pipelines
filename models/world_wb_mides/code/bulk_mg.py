"""Fetch TCE-MG exercises as whole-state packages, verified, with a fallback.

WHY THIS IS THE FAST PATH
-------------------------
The gateway offers the same data two ways, and the cost difference is three
orders of magnitude:

  per municipality  `baixarArquivo/{seq}`      854 listing calls + 853 downloads
                                               per exercise/category = ~88,000
                                               requests for the full span, and
                                               the body is base64, so 26 GB of
                                               zip moves as ~35 GB.

  whole state       `baixarArquivoPct/{seqZip}`  ONE request per exercise/category
                                               = 52 for the full span, raw zip,
                                               26 GB on the wire.

The members of the bulk archive are byte-for-byte the same per-municipality zips
the other route serves, named `SICOM.<year>.<ibge>.<categoria>.zip`, so the two
routes produce an identical tree on disk.

THE CEILING THAT LIMITS THIS ROUTE TO SMALL PACKAGES
----------------------------------------------------
Bulk packages are built on the fly and served **chunked, with no Content-Length
and no Range support** (`Accept-Ranges` absent; a Range request returns 200 and
the whole body, not 206). So a transfer cannot be resumed.

Worse, **the gateway cuts the response at roughly 52-58 MB.** This is not a
random dropped connection -- it reproduces, and it reproduces at the same place
through every client:

    2026 empenho    (312 MB advertised)  cut at 58 MB
    2026 licitacao  (158 MB advertised)  cut at 55 MB
    2025 empenho    (583 MB, browser)    cut at 54.8 MB  -- 191 of 853
    2025 despesa    (1607 MB, browser)   cut at 52.4 MB  -- 165 of 853

The truncated file keeps a valid `PK` header and a plausible size, and the
portal's own UI reports it as a completed download. That is the bug this whole
module exists to route around, and it is why `2026 contrato` (35.7 MB) succeeds
on the first attempt while nothing above ~50 MB ever will -- retrying a 1.5 GB
package is not persistence, it is 55 MB of wasted transfer per attempt.

So bulk is used ONLY where the advertised size is under `BULK_MAX_MB`, which as
of 2026-09-22 is five of the fifty-two exercise/category pairs -- every one of
them `contrato`. The other forty-seven (26 GB) go to `harvest_mg.py`, one
municipality at a time. The saving is real but modest: ~6,800 of ~88,000
requests. Anything larger is handed straight to the fallback WITHOUT being
attempted.

WHAT COUNTS AS PROOF
--------------------
Two checks, and the second is the one that matters:

  1. the central directory parses -- it lives at the end of the archive and
     cannot be written until the whole stream has arrived, so this catches a
     short read.
  2. the member count equals the `qtdArquivos` the portal advertises for that
     exercise/category.

Check 1 alone is NOT sufficient. A zip cut at a member boundary can still expose
a readable directory for the members that did arrive, which is precisely the
"165 of 853 municipalities" failure. Only the count catches that.

Members are then extracted and each is itself opened as a zip before being
accepted, because a corrupt member inside a well-formed container would otherwise
surface much later, in the cleaner.

Usage:
    export MG_TOKEN_FILE=~/Downloads/world_wb_mides_data/.mg_token
    python bulk_mg.py                      # every exercise/category
    python bulk_mg.py --year 2025          # one exercise
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import BROWSER_UA, MG_API, MG_STATIC_BEARER

# pyrefly: ignore [missing-import]  # sibling module via sys.path
from download_mg import _fold

# pyrefly: ignore [missing-import]  # sibling module via sys.path
from harvest_mg import (
    CATEGORIES,
    FIRST_YEAR,
    LAST_YEAR,
    MG_INPUT,
    TokenExpiredError,
    TokenFile,
    ca_bundle,
    wait_for_fresh_token,
)

# The bulk listing spells categories as display labels; every other endpoint uses
# the unaccented singular slug. Neither spelling works in the other's place.
# Keyed on the folded label so a change of accent or case does not lose a
# category; `SLUG_TO_LABEL` keeps the display spelling for messages.
LABEL_TO_SLUG = {
    _fold("Despesas"): "despesa",
    _fold("Empenhos"): "empenho",
    _fold("Contratos"): "contrato",
    _fold("Licitações"): "licitacao",
}
SLUG_TO_LABEL = {
    "despesa": "Despesas",
    "empenho": "Empenhos",
    "contrato": "Contratos",
    "licitacao": "Licitações",
}

BULK_LEDGER = MG_INPUT / "_bulk_ledger.jsonl"
CHUNK = 1 << 20
MAX_ATTEMPTS = 3
WORKERS = 4
# The gateway truncates bulk responses at ~52-58 MB (see the module docstring).
# 45 leaves headroom under the lowest observed cut, because the exact boundary
# moves a little between runs and a package that squeaks past it once is not
# reliably fetchable.
BULK_MAX_MB = 45.0
_LEDGER_LOCK = threading.Lock()
# A stalled read is a block, not slowness: without this a dead connection holds a
# worker until the OS gives up, which can be many minutes of nothing.
READ_TIMEOUT = 180


def _size_mb(text: str | None) -> float:
    """Parse the portal's "1.57 GB" / "583.24 MB" into MB. 0 if unparseable."""
    try:
        value, unit = str(text).split()
        return float(value) * {"GB": 1024.0, "MB": 1.0, "KB": 1 / 1024}[unit]
    except Exception:
        return 0.0


def session_for(token: str) -> requests.Session:
    session = requests.Session()
    session.verify = ca_bundle()
    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "*/*",
            "Authorization": f"Bearer {MG_STATIC_BEARER}",
        }
    )
    return session


def categories_for(session, token_file, year: int) -> dict[str, dict]:
    response = session.get(
        MG_API + "/buscarCategoriaDownload",
        params={"exercicio": year, "origem": "SICOM"},
        headers={"AuthorizationProxy": f"token {token_file.get()}"},
        timeout=120,
    )
    if response.status_code == 401:
        raise TokenExpiredError("401 from buscarCategoriaDownload")
    response.raise_for_status()
    found = {}
    offered = []
    for entry in response.json():
        label = str(entry.get("categoria", ""))
        offered.append(label)
        # Folded, not exact: `download_mg.py` already records that these display
        # labels are not stable, and an exact lookup that misses is not a
        # harmless miss here -- `main()` would record the pair as "absent", and
        # `harvest_mg.py` treats "absent" as done, so the fallback would never
        # enumerate it. One accent or one capital would silently remove a whole
        # exercise-category from both phases.
        slug = LABEL_TO_SLUG.get(_fold(label))
        if slug:
            found[slug] = entry
    if offered and not found:
        print(
            f"  {year}: none of the offered categories matched -- {offered}",
            flush=True,
        )
    return found


def fetch_bulk(
    session, token_file, year, categoria, entry, stop
) -> Path | None:
    """Download one whole-state package to a temp file, verified. None if refused.

    Returns the path to a package whose central directory parses AND whose member
    count matches what the portal advertises. Anything else is deleted and
    retried, because a partial package that keeps its name is worse than no
    package -- it looks complete to everything downstream.
    """
    seq_zip = entry.get("seqZip")
    expected = int(entry.get("qtdArquivos") or 0)
    if seq_zip is None:
        print(f"  {year} {categoria}: no seqZip in {entry}", flush=True)
        return None
    temporary = MG_INPUT / f".bulk_{year}_{categoria}.part"
    temporary.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        if stop.is_set():
            return None
        written = 0
        started = time.time()
        try:
            with session.get(
                MG_API + f"/baixarArquivoPct/{seq_zip}",
                headers={"AuthorizationProxy": f"token {token_file.get()}"},
                stream=True,
                timeout=(30, READ_TIMEOUT),
            ) as response:
                if response.status_code == 401:
                    raise TokenExpiredError("401 mid-package")
                response.raise_for_status()
                with open(temporary, "wb") as handle:
                    for chunk in response.iter_content(CHUNK):
                        if stop.is_set():
                            return None
                        handle.write(chunk)
                        written += len(chunk)
            elapsed = max(time.time() - started, 1)
            with zipfile.ZipFile(temporary) as archive:
                members = archive.namelist()
            # The count check is the one that catches a package cut at a member
            # boundary, which still exposes a readable directory.
            if expected and len(members) != expected:
                raise zipfile.BadZipFile(
                    f"{len(members)} members, portal advertises {expected}"
                )
            print(
                f"  {year} {categoria:<10} {written / 1e6:>8.1f} MB in "
                f"{elapsed / 60:>4.1f} min ({written / elapsed / 1024:.0f} KB/s), "
                f"{len(members)} members OK",
                flush=True,
            )
            return temporary
        except TokenExpiredError:
            wait_for_fresh_token(token_file, stop)
        except (requests.RequestException, zipfile.BadZipFile, OSError) as exc:
            print(
                f"  {year} {categoria} attempt {attempt}/{MAX_ATTEMPTS} failed "
                f"after {written / 1e6:.0f} MB: {exc}",
                flush=True,
            )
            temporary.unlink(missing_ok=True)
            if attempt < MAX_ATTEMPTS:
                time.sleep(10 * attempt)
    return None


def extract(
    package: Path, year: int, categoria: str
) -> tuple[int, int, list[str]]:
    """Explode a package into the per-municipality tree, verifying each member.

    Each member is opened as a zip in its own right before being accepted: a
    corrupt member inside a well-formed container is otherwise invisible until
    the cleaner trips over it, long after the run that produced it.
    """
    target = MG_INPUT / str(year) / categoria
    target.mkdir(parents=True, exist_ok=True)
    written = skipped = 0
    bad: list[str] = []
    with zipfile.ZipFile(package) as archive:
        for name in archive.namelist():
            leaf = Path(name).name
            if not leaf.lower().endswith(".zip"):
                continue
            destination = target / leaf
            if destination.exists() and destination.stat().st_size > 0:
                skipped += 1
                continue
            temporary = target / f".{leaf}.part"
            with archive.open(name) as source, open(temporary, "wb") as handle:
                while True:
                    block = source.read(CHUNK)
                    if not block:
                        break
                    handle.write(block)
            try:
                with zipfile.ZipFile(temporary) as inner:
                    inner.namelist()
            except zipfile.BadZipFile as exc:
                temporary.unlink(missing_ok=True)
                bad.append(f"{leaf}: {exc}")
                continue
            temporary.replace(destination)
            written += 1
    return written, skipped, bad


def record(year: int, categoria: str, status: str, **payload) -> None:
    BULK_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with _LEDGER_LOCK, open(BULK_LEDGER, "a") as handle:
        handle.write(
            json.dumps(
                {
                    "year": year,
                    "categoria": categoria,
                    "status": status,
                    "at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
                    **payload,
                }
            )
            + "\n"
        )


def completed_pairs() -> set[tuple[int, str]]:
    done: set[tuple[int, str]] = set()
    if not BULK_LEDGER.exists():
        return done
    for line in BULK_LEDGER.read_text().splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("status") == "ok":
            done.add((entry["year"], entry["categoria"]))
    return done


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--categoria", action="append", choices=CATEGORIES)
    parser.add_argument(
        "--token-file", default=os.environ.get("MG_TOKEN_FILE")
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=WORKERS,
        help="packages in flight. Throughput is ~250 KB/s per connection and "
        "scales close to linearly, so this sets the wall-clock directly. Kept "
        "at or below what a browser opens per host.",
    )
    args = parser.parse_args()
    globals()["WORKERS"] = max(1, args.workers)
    if not args.token_file:
        raise SystemExit("--token-file or $MG_TOKEN_FILE required")

    stop = threading.Event()
    token_file = TokenFile(Path(args.token_file).expanduser())
    left = token_file.minutes_left()
    print(
        f"token valid for {left:.0f} min"
        if left
        else "token expiry unreadable"
    )

    years = (
        sorted(args.year)
        if args.year
        else list(range(FIRST_YEAR, LAST_YEAR + 1))
    )
    # Newest first: MiDES already holds MG through 2021, so recent exercises are
    # the actual gap and should land first if the run is cut short.
    years = sorted(years, reverse=True)
    categorias = tuple(args.categoria) if args.categoria else CATEGORIES

    done = completed_pairs()
    print(f"bulk ledger: {len(done)} pairs already complete", flush=True)
    oversized: list[tuple[int, str, float]] = []

    # Resolve every exercise's catalogue up front: 13 cheap requests, and it means
    # the worker pool never contends on the listing endpoint.
    listing = session_for(token_file)
    plan: list[tuple[int, str, dict]] = []
    for year in years:
        for _attempt in range(3):
            try:
                available = categories_for(listing, token_file, year)
                break
            except TokenExpiredError:
                wait_for_fresh_token(token_file, stop)
        else:
            print(f"  {year}: could not read catalogue", flush=True)
            continue
        for categoria in categorias:
            if (year, categoria) in done:
                continue
            entry = available.get(categoria)
            if not entry:
                # A category an exercise genuinely never published is a real
                # answer, not a failure -- do not send it to the fallback.
                print(f"  {year} {categoria}: not published", flush=True)
                record(year, categoria, "absent")
                continue
            size = _size_mb(entry.get("tamanho"))
            if size > BULK_MAX_MB:
                # Not attempted on purpose: the gateway would cut it at ~55 MB,
                # so trying costs 55 MB of transfer to learn what the advertised
                # size already told us.
                oversized.append((year, categoria, size))
                record(
                    year, categoria, "too_large", advertised_mb=round(size, 1)
                )
                continue
            plan.append((year, categoria, entry))

    if oversized:
        print(
            f"\n{len(oversized)} pairs exceed the {BULK_MAX_MB:.0f} MB bulk ceiling "
            f"and go to harvest_mg.py "
            f"({sum(m for _, _, m in oversized) / 1024:.1f} GB):",
            flush=True,
        )
        for year, categoria, size in sorted(oversized)[:6]:
            print(f"    {year} {categoria:<10} {size:>8.1f} MB", flush=True)
        if len(oversized) > 6:
            print(f"    ... and {len(oversized) - 6} more", flush=True)
    if not plan:
        print("\nno bulk-eligible package left to fetch")
        return
    advertised = sum(_size_mb(e.get("tamanho")) for _, _, e in plan)
    print(
        f"\n{len(plan)} packages to fetch, ~{advertised / 1024:.2f} GB, "
        f"{WORKERS} at a time\n",
        flush=True,
    )

    failed: list[tuple[int, str]] = []
    counters = {"files": 0}
    local = threading.local()

    def run_pair(job):
        year, categoria, entry = job
        if stop.is_set():
            return
        if not hasattr(local, "session"):
            local.session = session_for(token_file)
        package = fetch_bulk(
            local.session, token_file, year, categoria, entry, stop
        )
        if package is None:
            if stop.is_set():
                return
            print(
                f"  {year} {categoria}: BULK FAILED -- queued for "
                f"per-municipality fallback",
                flush=True,
            )
            record(year, categoria, "failed")
            failed.append((year, categoria))
            return
        written, skipped, bad = extract(package, year, categoria)
        package.unlink(missing_ok=True)
        counters["files"] += written
        print(
            f"    {year} {categoria}: extracted {written} new, "
            f"{skipped} already present"
            + (f", {len(bad)} CORRUPT" if bad else ""),
            flush=True,
        )
        for line in bad[:5]:
            print(f"      CORRUPT {line}", flush=True)
        record(
            year,
            categoria,
            "ok" if not bad else "partial",
            written=written,
            skipped=skipped,
            corrupt=len(bad),
        )
        if bad:
            failed.append((year, categoria))

    started = time.time()
    try:
        with ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(run_pair, plan))
    except KeyboardInterrupt:
        stop.set()
        raise

    print(
        f"\nBULK DONE: {counters['files']:,} files extracted in "
        f"{(time.time() - started) / 3600:.1f} h",
        flush=True,
    )
    if failed:
        # A bulk-eligible package that still would not verify. Rare, and the
        # fallback picks it up like any oversized pair.
        print(
            "\nUnder the ceiling but still unverified -- handing to fallback:"
        )
        for year, categoria in sorted(set(failed)):
            print(f"  {year} {categoria}")
    print("bulk phase done; harvest_mg.py covers everything else")


if __name__ == "__main__":
    main()
