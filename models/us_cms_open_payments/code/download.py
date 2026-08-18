"""Fetch one program year of source data into the scratch directory.

Current program years ship as loose CSVs; archived ones ship as a single ZIP
per year whose members are extracted and the ZIP then deleted. Nothing lands
in the repo or in Dropbox -- see constants.DATA_ROOT.

    uv run python download.py detail 2024
    uv run python download.py profile
    uv run python download.py summary
"""

import shutil
import subprocess
import sys
import zipfile

import constants as c

# A single stream from download.cms.gov runs at about 2 MB/s; eight parallel
# byte ranges reach about 15 MB/s, which is the difference between seven hours
# and one for the 60 GB of detail files.
PARALLEL_CHUNKS = 8
PARALLEL_MIN_BYTES = 200 << 20


def remote_size(url: str) -> int | None:
    """Total size from a one-byte range request; None when unavailable."""
    out = subprocess.run(
        [
            "curl",
            "-s",
            "-m",
            "60",
            "-r",
            "0-0",
            "-D",
            "-",
            "-o",
            "/dev/null",
            url,
        ],
        capture_output=True,
        text=True,
    ).stdout
    for line in out.splitlines():
        if line.lower().startswith("content-range") and "/" in line:
            total = line.rsplit("/", 1)[1].strip()
            if total.isdigit():
                return int(total)
    return None


def _fetch_serial(url: str, tmp) -> None:
    subprocess.run(
        [
            "curl",
            "-fsSL",
            "--retry",
            "3",
            "--retry-delay",
            "5",
            url,
            "-o",
            str(tmp),
        ],
        check=True,
    )


def _fetch_parallel(url: str, tmp, size: int) -> None:
    span = -(-size // PARALLEL_CHUNKS)
    parts = [
        tmp.with_suffix(f"{tmp.suffix}.{i:02d}")
        for i in range(PARALLEL_CHUNKS)
    ]
    running = []
    for index, part in enumerate(parts):
        start = index * span
        end = min(start + span, size) - 1
        if start > end:
            part.write_bytes(b"")
            continue
        running.append(
            subprocess.Popen(
                [
                    "curl",
                    "-fsS",
                    "--retry",
                    "3",
                    "--retry-delay",
                    "5",
                    "-r",
                    f"{start}-{end}",
                    url,
                    "-o",
                    str(part),
                ]
            )
        )
    for process in running:
        if process.wait() != 0:
            raise RuntimeError(f"chunk download failed for {url}")

    with open(tmp, "wb") as out:
        for part in parts:
            if not part.exists():
                continue
            with open(part, "rb") as chunk:
                shutil.copyfileobj(chunk, out, length=8 << 20)
            part.unlink()
    got = tmp.stat().st_size
    if got != size:
        raise RuntimeError(f"{url}: assembled {got} bytes, expected {size}")


def _fetch(url: str, dest) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  have {dest.name}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    size = remote_size(url)
    if size and size >= PARALLEL_MIN_BYTES:
        print(
            f"  get {dest.name} ({size / 1e9:.2f} GB, {PARALLEL_CHUNKS} streams)"
        )
        _fetch_parallel(url, tmp, size)
    else:
        print(f"  get {dest.name}")
        _fetch_serial(url, tmp)
    tmp.rename(dest)


def detail(year: int) -> None:
    """Land general/research/ownership CSVs for one program year."""
    targets = {
        kind: c.INPUT_DIR / f"{kind}_{year}.csv" for kind in c.DETAIL_KINDS
    }
    if all(p.exists() and p.stat().st_size > 0 for p in targets.values()):
        print(f"  program year {year} already downloaded")
        return

    if year in c.CURRENT_YEARS:
        for kind, dest in targets.items():
            _fetch(c.detail_url(year, kind), dest)
        return

    archive = c.INPUT_DIR / c.ARCHIVE_ZIPS[year]
    _fetch(c.archive_url(year), archive)
    with zipfile.ZipFile(archive) as zf:
        for kind, stem in c.DETAIL_KINDS.items():
            member = next(n for n in zf.namelist() if f"OP_DTL_{stem}_" in n)
            print(f"  extract {member}")
            with zf.open(member) as src, open(targets[kind], "wb") as out:
                shutil.copyfileobj(src, out, length=8 << 20)
    archive.unlink()
    print(f"  removed {archive.name}")


def profiles() -> None:
    for name, url in c.PROFILE_FILES.items():
        _fetch(url, c.INPUT_DIR / f"{name}.csv")


def summaries() -> None:
    for name, stem in c.SUMMARY_PER_YEAR.items():
        for year in c.SUMMARY_YEARS:
            _fetch(
                c.summary_url(stem.format(year=year)),
                c.INPUT_DIR / f"{name}_{year}.csv",
            )
    for name, (stem, joined) in c.SUMMARY_ALL_YEARS.items():
        _fetch(c.summary_url(stem, joined), c.INPUT_DIR / f"{name}.csv")


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "detail":
        detail(int(sys.argv[2]))
    elif what == "profile":
        profiles()
    elif what == "summary":
        summaries()
    else:
        raise SystemExit(f"unknown target: {what}")
