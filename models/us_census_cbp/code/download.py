"""Download U.S. Census County Business Patterns (CBP) raw files.

Years 1998-2023, three geographic levels each: US, State, County.
Saves to models/us_census_cbp/data/input/ as cbp<YY>{us,st,co}.txt.

- US file: try cbp<YY>us.zip first, fall back to cbp<YY>us.txt on 404.
- State/County: cbp<YY>{st,co}.zip, fall back to .txt on 404.
- Unzip archives, normalize inner name to lowercase, remove the .zip.
- Gentle: sequential, short pause between files; retry timeouts up to 3x.
"""

import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://www2.census.gov/programs-surveys/cbp/datasets"
YEARS = list(range(1998, 2024))
LEVELS = ["us", "st", "co"]
PAUSE = 1.5  # seconds between files
TIMEOUT = 120  # per-request timeout (slow server)
RETRIES = 3  # retries on timeout / transient error

results = {}  # (year, level) -> dict(status, detail, rows)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fetch(url: str, dest: Path) -> str:
    """Download url to dest. Return 'ok', 'http:<code>' on 404-type miss,
    or raise after exhausting retries on timeout/transient errors."""
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            with requests.get(url, timeout=TIMEOUT, stream=True) as r:
                if r.status_code == 404:
                    return "http:404"
                if r.status_code != 200:
                    # treat other non-200 as retryable a couple times
                    if attempt < RETRIES:
                        time.sleep(2**attempt)
                        continue
                    return f"http:{r.status_code}"
                tmp = dest.with_suffix(dest.suffix + ".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        if chunk:
                            f.write(chunk)
                tmp.rename(dest)
                return "ok"
        except requests.RequestException as e:
            last_exc = e
            log(f"  attempt {attempt}/{RETRIES} failed for {url}: {e}")
            if attempt < RETRIES:
                time.sleep(2**attempt)
    raise RuntimeError(
        f"timeout/transient after {RETRIES} attempts: {last_exc}"
    )


def unzip_normalize(zip_path: Path, want_txt: Path) -> bool:
    """Extract the single .txt member of zip_path to want_txt (lowercased).
    Remove the zip afterwards. Return True on success."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".txt")]
        if not members:
            members = zf.namelist()
        member = members[0]
        with zf.open(member) as src, open(want_txt, "wb") as out:
            out.write(src.read())
    zip_path.unlink()
    return want_txt.exists() and want_txt.stat().st_size > 0


def row_count(path: Path) -> int:
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n


def handle(year: int, level: str) -> None:
    yy = f"{year % 100:02d}"
    key = (year, level)
    txt_final = INPUT_DIR / f"cbp{yy}{level}.txt"

    if txt_final.exists() and txt_final.stat().st_size > 0:
        rows = row_count(txt_final)
        results[key] = {"status": "OK", "detail": "cached", "rows": rows}
        log(f"cbp{yy}{level}: already present ({rows} rows) - skip")
        return

    zip_url = f"{BASE}/{year}/cbp{yy}{level}.zip"
    txt_url = f"{BASE}/{year}/cbp{yy}{level}.txt"
    zip_dest = INPUT_DIR / f"cbp{yy}{level}.zip"

    # 1) try zip
    try:
        st = fetch(zip_url, zip_dest)
    except RuntimeError as e:
        results[key] = {"status": "FAILED", "detail": f"zip {e}", "rows": 0}
        log(f"cbp{yy}{level}: FAILED zip - {e}")
        return

    if st == "ok":
        try:
            ok = unzip_normalize(zip_dest, txt_final)
        except zipfile.BadZipFile as e:
            results[key] = {
                "status": "FAILED",
                "detail": f"bad zip: {e}",
                "rows": 0,
            }
            log(f"cbp{yy}{level}: FAILED bad zip - {e}")
            if zip_dest.exists():
                zip_dest.unlink()
            return
        if ok:
            rows = row_count(txt_final)
            results[key] = {"status": "OK", "detail": "zip", "rows": rows}
            log(f"cbp{yy}{level}: OK from zip ({rows} rows)")
        else:
            results[key] = {
                "status": "FAILED",
                "detail": "empty after unzip",
                "rows": 0,
            }
            log(f"cbp{yy}{level}: FAILED empty after unzip")
        return

    # 2) fall back to .txt (404 on zip)
    log(f"cbp{yy}{level}: zip {st}, trying .txt fallback")
    try:
        st2 = fetch(txt_url, txt_final)
    except RuntimeError as e:
        results[key] = {"status": "FAILED", "detail": f"txt {e}", "rows": 0}
        log(f"cbp{yy}{level}: FAILED txt - {e}")
        return

    if st2 == "ok":
        rows = row_count(txt_final)
        results[key] = {"status": "OK", "detail": "txt", "rows": rows}
        log(f"cbp{yy}{level}: OK from txt ({rows} rows)")
    else:
        results[key] = {
            "status": "FAILED",
            "detail": f"zip {st}, txt {st2}",
            "rows": 0,
        }
        log(f"cbp{yy}{level}: FAILED - zip {st}, txt {st2}")


def main() -> None:
    for year in YEARS:
        for level in LEVELS:
            handle(year, level)
            time.sleep(PAUSE)

    # summary
    print("\n===== SUMMARY =====", flush=True)
    ok = failed = 0
    for year in YEARS:
        row = [f"{year}"]
        for level in LEVELS:
            r = results[(year, level)]
            if r["status"] == "OK":
                ok += 1
                row.append(f"{level}=OK({r['rows']})")
            else:
                failed += 1
                row.append(f"{level}=FAILED[{r['detail']}]")
        print("  " + "  ".join(row), flush=True)
    print(f"\nOK={ok}  FAILED={failed}", flush=True)


if __name__ == "__main__":
    main()
