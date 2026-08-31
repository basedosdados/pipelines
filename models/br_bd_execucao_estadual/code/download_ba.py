"""Download the Bahia source archives from dados.ba.gov.br.

BA publishes a handful of large ZIPs off FIPLAN (expenditure) and SIMPAS/SAEB
(procurement), refreshed daily with D-1 data. Each ZIP holds one CSV per database view.

The archives are big -- Despesas.zip is 222 MB compressed and 2.65 GB extracted -- so the
download is resumable and the ZIP central directory is verified before a file is accepted.
A truncated ZIP is the same trap as a truncated gzip: it opens fine and only fails deep
into extraction, long after the download step has reported success.
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import BA_CKAN, BA_PACKAGES, BROWSER_UA, INPUT_DIR

BA_INPUT = INPUT_DIR / "ba"
CHUNK = 1 << 20


def is_intact(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    if path.suffix.lower() != ".zip":
        return True
    try:
        with zipfile.ZipFile(path) as zf:
            # testzip() walks every member's CRC. Slower than reading the central
            # directory, and worth it: a truncated archive still lists its members.
            return zf.testzip() is None
    except (zipfile.BadZipFile, OSError):
        return False


def plan(session: requests.Session) -> dict[str, str]:
    wanted: dict[str, str] = {}
    for package in BA_PACKAGES:
        r = session.get(BA_CKAN, params={"id": package}, timeout=180)
        r.raise_for_status()
        for res in r.json()["result"]["resources"]:
            url = res["url"]
            name = url.rsplit("/", 1)[-1]
            if name.lower().endswith((".zip", ".csv")):
                wanted[name] = url
    return wanted


def download(session: requests.Session, name: str, url: str) -> int:
    dest = BA_INPUT / name
    if is_intact(dest):
        return dest.stat().st_size
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, stream=True, timeout=3600) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(CHUNK):
                fh.write(chunk)
    tmp.replace(dest)
    if not is_intact(dest):
        dest.unlink(missing_ok=True)
        raise OSError(f"{name}: archive failed its integrity check")
    return dest.stat().st_size


def main(retries: int = 3) -> None:
    BA_INPUT.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})

    pending = plan(session)
    print(f"{len(pending)} files")
    for attempt in range(1, retries + 1):
        failed = {}
        for name, url in sorted(pending.items()):
            try:
                size = download(session, name, url)
                print(f"  {name}: {size / 1e6:.0f} MB", flush=True)
            except (requests.RequestException, OSError) as exc:
                print(f"  {name}: {exc}")
                failed[name] = url
        if not failed:
            break
        pending = failed
        print(f"attempt {attempt}: {len(failed)} to retry")
    else:
        print(f"STILL FAILING: {sorted(pending)}")
        raise SystemExit(1)
    print("BA download complete")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--retries", type=int, default=3)
    main(**vars(ap.parse_args()))
