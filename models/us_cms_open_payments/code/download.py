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


def _fetch(url: str, dest) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  have {dest.name}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    print(f"  get {dest.name}")
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
