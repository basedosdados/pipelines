"""Download Ceará execution documents from the Ceará Transparente portal.

CE publishes SIAFE-CE's three documents as file attachments on a catalogue whose HTML
is behind a JS anti-bot challenge. The files themselves are not challenged, so this
downloader needs no browser and no cookie -- it needs two things the portal does not
advertise, and both are cheap to get wrong:

* **`?force_download=true` on every URL.** Without it the server returns HTTP 403 with
  a 9-byte `forbidden` body, for a URL that serves the file perfectly with it. That 403
  is indistinguishable from an IP block by status code, and reading it as one is how a
  previous run concluded CE had blocked it twice. See `constants.CE_FORCE_DOWNLOAD`.
* **A Brazilian IP.** The same URL is 403 from outside Brazil, and that one really is a
  block. `--check-egress` probes it before starting.

The URL list comes from `ce_manifest.txt`, captured by enumerating the catalogue inside
a browser (the list page is AJAX-rendered and the detail pages are challenged, so
`requests` cannot do it). Attachment URLs are content-addressed and rotate on republish,
so the manifest is a seed: any entry that 404s is reported at the end for
re-enumeration rather than silently skipped.

Pacing is serial with a ~1.2 s delay and escalating backoff. This is not superstition:
four concurrent workers over these 216 URLs produced a real IP-wide block lasting over
25 minutes. There is nothing to gain from concurrency here -- the whole set is well
under an hour serially.

Files are stored as the raw bytes the server sends, named
`<dataset id>__<sha1 prefix>__<file name>` with a `.meta.json` sidecar. The name carries
the content hash because **dataset 170 publishes `NPD+4BI.csv` twice with different
hashes**, and a name-keyed destination silently keeps one of them.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from constants import (
    BROWSER_UA,
    CE_ATTACHMENTS,
    CE_BASE,
    CE_FORBIDDEN_MAGIC,
    CE_FORCE_DOWNLOAD,
    CE_MANIFEST,
    CE_MIN_BODY_BYTES,
    CE_TITLE_PHASE,
    INPUT_DIR,
)

CE_INPUT = INPUT_DIR / "ce"

HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "*/*",
    "Referer": f"{CE_BASE}/portal-da-transparencia/dados-abertos/conjuntos-de-dados",
}


class ManifestStaleError(Exception):
    """A manifest entry no longer resolves; the file was republished."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def phase_of(title: str) -> str | None:
    """Phase from the dataset TITLE, which is stable across the series.

    The file NAME is not: one phase appears as `NLD_Ano2022_1Bimestre.csv`,
    `NLD+4+Bimestre.csv`, `NLD+5+BI.xls` and `1+BI+NLD.xls` inside three exercises.
    """
    low = title.lower()
    for needle, phase in CE_TITLE_PHASE:
        if needle in low:
            return phase
    return None


def slot_year(title: str) -> int | None:
    """The exercise the CATALOGUE files a dataset under.

    Used only to organise the download and to scope `--year`. It is **not** the year the
    rows belong to -- dataset 135 is titled 2020 and holds `NLD_Ano2019_*` files -- so
    the cleaner partitions on the exercise column instead.
    """
    m = re.search(r"(20\d{2})", title)
    return int(m.group(1)) if m else None


def local_name(dataset_id: str, rel: str) -> str:
    """Destination file name: dataset id, content-hash prefix, and the source name.

    The hash prefix is load-bearing. Dataset 170 lists `NPD+4BI.csv` twice with
    different `<sha1>/store/<sha256>/` paths; keyed on the name alone the second
    download overwrites the first and one of the two files is lost without a trace.
    """
    sha1 = rel.split("/", 1)[0]
    raw = unquote(rel.rsplit("/", 1)[-1])
    safe = re.sub(r"[^A-Za-z0-9._+-]+", "_", raw)
    return f"{dataset_id}__{sha1[:10]}__{safe}"


def read_manifest(path: Path) -> list[dict]:
    entries: list[dict] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        dataset_id, title, rel = line.split("|", 2)
        entries.append(
            {
                "dataset_id": dataset_id,
                "title": title,
                "rel": rel,
                "phase": phase_of(title),
                "slot_year": slot_year(title),
                "name": local_name(dataset_id, rel),
            }
        )
    return entries


def check_egress(session: requests.Session, entries: list[dict]) -> None:
    """Prove the downloads are reachable before spending an hour finding out they are not.

    The probe is a 256-byte range request against a real attachment, because that is the
    thing that is geo-fenced. The portal homepage answers from anywhere and proves
    nothing, and an IP-echo service reports the tunnel's country rather than the route
    actually taken to this host.
    """
    rel = entries[0]["rel"]
    r = session.get(
        f"{CE_ATTACHMENTS}/{rel}?{CE_FORCE_DOWNLOAD}",
        headers={"Range": "bytes=0-255"},
        timeout=60,
    )
    if r.status_code in (200, 206) and not r.content.startswith(
        CE_FORBIDDEN_MAGIC
    ):
        print(
            f"egress OK: {r.status_code}, {len(r.content)} bytes", flush=True
        )
        return
    raise SystemExit(
        f"egress check failed ({r.status_code}, body starts {r.content[:16]!r}). "
        f"Either this host is not being reached from a Brazilian IP, or the URL was "
        f"built without ?{CE_FORCE_DOWNLOAD} -- check the parameter before assuming a "
        f"block, because the two are identical from here."
    )


def fetch(
    session: requests.Session, entry: dict, retries: int = 5
) -> tuple[str, int]:
    dest = CE_INPUT / entry["phase"] / entry["name"]
    meta = dest.with_suffix(dest.suffix + ".meta.json")
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and meta.exists():
        recorded = json.loads(meta.read_text())
        if recorded.get("bytes") == dest.stat().st_size:
            return "skip", recorded["bytes"]

    url = f"{CE_ATTACHMENTS}/{entry['rel']}?{CE_FORCE_DOWNLOAD}"
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=900, stream=True)
            if r.status_code == 404:
                r.close()
                raise ManifestStaleError(entry["name"])
            if r.status_code != 200:
                r.close()
                # Escalating backoff. A 403 here is most likely the missing parameter
                # (impossible on this path) or a real block from too much traffic.
                wait = 60 * (attempt + 1)
                print(
                    f"    HTTP {r.status_code} on {entry['name'][:52]} "
                    f"-- sleeping {wait}s",
                    flush=True,
                )
                time.sleep(wait)
                continue

            tmp = dest.with_suffix(dest.suffix + ".part")
            size = 0
            with tmp.open("wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
                    size += len(chunk)
            r.close()

            head = tmp.open("rb").read(16)
            # A refused body is a 200-looking stream. `forb` is ASCII 666f7262.
            if head.startswith(CE_FORBIDDEN_MAGIC) or size < CE_MIN_BODY_BYTES:
                tmp.unlink(missing_ok=True)
                print(
                    f"    refused body ({size} B, {head[:8]!r}) on "
                    f"{entry['name'][:52]}",
                    flush=True,
                )
                time.sleep(60 * (attempt + 1))
                continue

            tmp.replace(dest)
            meta.write_text(
                json.dumps(
                    {
                        "dataset_id": entry["dataset_id"],
                        "title": entry["title"],
                        "phase": entry["phase"],
                        "slot_year": entry["slot_year"],
                        "url": url,
                        "bytes": size,
                    },
                    ensure_ascii=False,
                )
            )
            return "ok", size
        except ManifestStaleError:
            raise
        except Exception as exc:
            print(
                f"    {type(exc).__name__} on {entry['name'][:52]} "
                f"(attempt {attempt + 1})",
                flush=True,
            )
            time.sleep(20 * (attempt + 1))
    return "FAIL", 0


def main(
    phases: tuple[str, ...] | None = None,
    years: set[int] | None = None,
    pause: float = 1.2,
    manifest: Path = CE_MANIFEST,
) -> None:
    entries = read_manifest(manifest)
    unknown = [e for e in entries if e["phase"] is None]
    if unknown:
        raise SystemExit(
            f"{len(unknown)} manifest entries have a title no phase rule matches, "
            f"e.g. {unknown[0]['title']!r}. Add the rule rather than dropping the rows."
        )
    if phases:
        entries = [e for e in entries if e["phase"] in phases]
    if years:
        entries = [e for e in entries if e["slot_year"] in years]

    session = _session()
    check_egress(session, entries)

    stale: list[str] = []
    failures: list[str] = []
    counts: dict[str, list[int]] = {}

    for i, entry in enumerate(entries, 1):
        try:
            status, size = fetch(session, entry)
        except ManifestStaleError:
            stale.append(
                f"{entry['dataset_id']}|{entry['title']}|{entry['rel']}"
            )
            print(
                f"  [{i}/{len(entries)}] STALE {entry['name'][:56]}",
                flush=True,
            )
            time.sleep(pause)
            continue
        if status == "FAIL":
            failures.append(entry["name"])
        bucket = counts.setdefault(entry["phase"], [0, 0])
        bucket[0] += 1
        bucket[1] += size
        print(
            f"  [{i}/{len(entries)}] {status:<4} {entry['name'][:56]:<56} "
            f"{size / 1e6:8.2f} MB",
            flush=True,
        )
        time.sleep(pause)

    for phase, (n, size) in sorted(counts.items()):
        print(f"{phase:<12} {n:>4} file(s) {size / 1e6:>10.1f} MB")

    if stale:
        print(
            f"\n{len(stale)} manifest entr(ies) returned 404. Attachment URLs are "
            f"content-addressed and rotate on republish -- re-enumerate these dataset "
            f"ids in the browser and refresh ce_manifest.txt:",
            flush=True,
        )
        for line in stale:
            print(f"  {line}", flush=True)
    if failures:
        raise SystemExit(
            f"{len(failures)} file(s) could not be downloaded: "
            f"{', '.join(failures[:20])}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=("empenho", "liquidacao", "pagamento"),
        action="append",
    )
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--manifest", type=Path, default=CE_MANIFEST)
    parser.add_argument("--pause", type=float, default=1.2)
    args = parser.parse_args()
    main(
        phases=tuple(args.phase) if args.phase else None,
        years=set(args.year) if args.year else None,
        pause=args.pause,
        manifest=args.manifest,
    )
