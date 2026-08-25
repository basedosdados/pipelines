"""Download the PIAAC Public Use Files and supporting documents.

Usage:
    uv run python models/world_oecd_piaac/code/download.py [--docs-only] [--pufs-only]

Resumable: a file whose local size already matches the server's Content-Length is
skipped, so the script can be re-run after an interruption. webfs.oecd.org is slow
(a 45 MB file can take minutes) and rejects non-browser user agents.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# webfs.oecd.org serves a single connection at roughly 200 KB/s, so the full 2.2 GB
# takes about three hours serially. A handful of parallel connections cuts that to
# under half an hour; the server has shown no sign of throttling at this width.
DEFAULT_WORKERS = 6

_spec = importlib.util.spec_from_file_location(
    "piaac_constants", Path(__file__).with_name("constants.py")
)
C = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(C)


def _request(url: str) -> urllib.request.Request:
    # A bare User-Agent is enough for webfs.oecd.org, but www.oecd.org's edge also
    # wants a browser-shaped Accept and Referer. It rejects .zip for every scripted
    # client regardless -- see download_docs().
    return urllib.request.Request(
        url,
        headers={
            "User-Agent": C.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.oecd.org/en/about/programmes/piaac/piaac-data.html",
        },
    )


def remote_size(url: str) -> int | None:
    request = _request(url)
    request.get_method = lambda: "HEAD"
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            length = response.headers.get("Content-Length")
            return int(length) if length else None
    except Exception:
        return None


def fetch(url: str, destination: Path) -> str:
    """Download unless the local file already matches the server's size."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    expected = remote_size(url)
    if (
        destination.exists()
        and expected is not None
        and destination.stat().st_size == expected
    ):
        return f"skip ({destination.stat().st_size:,} B)"

    partial = destination.with_suffix(destination.suffix + ".part")
    with (
        urllib.request.urlopen(_request(url), timeout=900) as response,
        partial.open("wb") as out,
    ):
        shutil.copyfileobj(response, out, length=1 << 20)

    size = partial.stat().st_size
    if expected is not None and size != expected:
        partial.unlink()
        raise OSError(f"{url}: got {size:,} B, expected {expected:,} B")
    partial.replace(destination)
    return f"ok ({size:,} B)"


def unpack(archive: Path) -> Path:
    """Cycle 2 Israel and Korea ship as zips; extract the single CSV alongside."""
    target = archive.with_suffix(".csv")
    with zipfile.ZipFile(archive) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(
                f"{archive.name}: expected 1 CSV, found {members}"
            )
        with zf.open(members[0]) as src, target.open("wb") as out:
            shutil.copyfileobj(src, out, length=1 << 20)
        # The readme lists which variables the re-release corrected; keep it.
        for name in zf.namelist():
            if name.lower().startswith("readme"):
                readme = archive.parent / f"{archive.stem}_{Path(name).name}"
                readme.write_bytes(zf.read(name))
    return target


def download_docs() -> None:
    print("=== documents ===", flush=True)
    for cycle, documents in C.DOCS.items():
        for name, url in documents:
            destination = C.DOCS_ROOT / f"cycle_{cycle}" / name
            try:
                print(
                    f"  cy{cycle} {name:<45} {fetch(url, destination)}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"  cy{cycle} {name:<45} FAILED {type(exc).__name__}: {exc}",
                    flush=True,
                )


def _download_one(entry: tuple) -> tuple[str, bool]:
    iso3, cycle, round_, remote, _delim, zipped, _national = entry
    destination = C.local_puf_path(iso3, cycle, round_, remote)
    label = f"cy{cycle} r{round_} {iso3}"
    try:
        status = fetch(C.puf_url(cycle, remote), destination)
        if zipped:
            status += f" -> {unpack(destination).name}"
        return f"  {label:<16} {status}", True
    except Exception as exc:
        return f"  {label:<16} FAILED {type(exc).__name__}: {exc}", False


def download_pufs(workers: int = DEFAULT_WORKERS) -> None:
    print(
        f"=== public use files ({len(C.PUF_FILES)} files, {workers} workers) ===",
        flush=True,
    )
    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for done, (line, ok) in enumerate(
            pool.map(_download_one, C.PUF_FILES), start=1
        ):
            print(f"[{done:>2}/{len(C.PUF_FILES)}]{line}", flush=True)
            failures += not ok
    print(f"=== done. failures: {failures} ===", flush=True)


def main() -> None:
    argv = sys.argv[1:]
    workers = DEFAULT_WORKERS
    if "--workers" in argv:
        workers = int(argv[argv.index("--workers") + 1])
    if "--pufs-only" not in argv:
        download_docs()
    if "--docs-only" not in argv:
        download_pufs(workers)


if __name__ == "__main__":
    main()
