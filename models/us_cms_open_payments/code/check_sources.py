"""Confirm every catalogued source URL resolves before any download starts."""

import concurrent.futures as cf
import subprocess

import constants as c


def status(url: str) -> str:
    out = subprocess.run(
        [
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-m",
            "60",
            "-r",
            "0-0",
            url,
        ],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return out


def catalogue() -> list[tuple[str, str]]:
    items = []
    for year in c.CURRENT_YEARS:
        for kind in c.DETAIL_KINDS:
            items.append((f"detail {kind} {year}", c.detail_url(year, kind)))
    for year in c.ARCHIVE_ZIPS:
        items.append((f"archive {year}", c.archive_url(year)))
    for name, url in c.PROFILE_FILES.items():
        items.append((f"profile {name}", url))
    for name, stem in c.SUMMARY_PER_YEAR.items():
        for year in c.SUMMARY_YEARS:
            items.append(
                (
                    f"summary {name} {year}",
                    c.summary_url(stem.format(year=year)),
                )
            )
    for name, (stem, joined) in c.SUMMARY_ALL_YEARS.items():
        items.append((f"summary {name}", c.summary_url(stem, joined)))
    return items


if __name__ == "__main__":
    items = catalogue()
    with cf.ThreadPoolExecutor(max_workers=12) as pool:
        codes = list(pool.map(lambda it: status(it[1]), items))
    bad = [
        (label, url, code)
        for (label, url), code in zip(items, codes, strict=True)
        if code not in ("200", "206")
    ]
    print(f"{len(items)} URLs checked, {len(bad)} unreachable")
    for label, url, code in bad:
        print(f"  {code}  {label}\n       {url}")
