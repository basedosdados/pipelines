"""Download the Météo-France *Données climatologiques de base* archives.

Daily (QUOT) and monthly (MENS) series, published per département and per period
on the OVH object store. The département list and the exact file names come from
the data.gouv.fr resource listing rather than being hard-coded, because the
period split changes over time (``latest-2025-2026`` rolls forward each year).

    uv run python models/fr_meteofrance/code/clim_download.py [--only quot|mens]

Files land in ``$MFC_INPUT`` (default ``~/Downloads/fr_meteofrance_clim/input``).
"""

import argparse
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DATASETS = {
    "quot": "6569b51ae64326786e4e8e1a",  # Données climatologiques de base - quotidiennes
    "mens": "6569b3d7d193b4daf2b43edc",  # Données climatologiques de base - mensuelles
}

INPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_INPUT", "~/Downloads/fr_meteofrance_clim/input")
    )
)


def resource_urls(dataset_id: str) -> list[tuple[str, str, int]]:
    """Return ``(url, filename, size)`` for every ``csv.gz`` resource."""
    url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    with urllib.request.urlopen(url, timeout=120) as fh:
        payload = json.load(fh)
    out = []
    for r in payload.get("resources", []):
        if (r.get("format") or "") != "csv.gz":
            continue
        u = r["url"]
        out.append((u, u.rsplit("/", 1)[-1], r.get("filesize") or 0))
    return out


def fetch(args, attempts: int = 5) -> tuple[str, bool]:
    """Fetch one file, resuming the run by skipping anything already on disk.

    Retries with a linear backoff: pulling ~940 files in parallel reliably trips
    transient DNS and connection-reset failures against the OVH store, and a
    single failure would otherwise abandon the whole download.
    """
    url, dest = args
    if dest.exists() and dest.stat().st_size > 0:
        return dest.name, False
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, attempts + 1):
        try:
            with (
                urllib.request.urlopen(url, timeout=900) as r,
                tmp.open("wb") as fh,
            ):
                while chunk := r.read(1 << 20):
                    fh.write(chunk)
            tmp.rename(dest)
            return dest.name, True
        except Exception as exc:  # retry every transport error
            if attempt == attempts:
                raise RuntimeError(
                    f"{dest.name}: giving up after {attempts} tries"
                ) from exc
            time.sleep(2 * attempt)
    raise AssertionError("unreachable")


def download(kind: str, workers: int = 4) -> None:
    out = INPUT / kind
    out.mkdir(parents=True, exist_ok=True)
    resources = resource_urls(DATASETS[kind])
    total = sum(s for _u, _n, s in resources)
    print(f"{kind}: {len(resources)} files, {total / 1e9:.2f} GB compressed")

    jobs = [(u, out / n) for u, n, _s in resources]
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for name, fetched in pool.map(fetch, jobs):
            done += 1
            if done % 50 == 0 or not fetched:
                print(
                    f"  {done}/{len(jobs)} {name}{'' if fetched else ' (cached)'}"
                )
    print(f"{kind}: done, {len(list(out.glob('*.csv.gz')))} files on disk")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only", choices=["quot", "mens"], help="download one series"
    )
    args = parser.parse_args()
    print(f"input {INPUT}")
    for kind in ("mens", "quot"):
        if args.only in (None, kind):
            download(kind)


if __name__ == "__main__":
    main()
