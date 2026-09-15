"""Download every raw source for us_ffiec_bank_reporting.

Four sources, three different access mechanisms:

  call    FFIEC CDR bulk data -- an ASP.NET WebForms page. Needs a two-step
          postback (select product -> the period dropdown is populated -> submit)
          carrying __VIEWSTATE through a session cookie jar.
  mdrm    Federal Reserve MDRM data dictionary -- a plain zip.
  bhc     FR Y-9C consolidated financials -- Chicago Fed CSV (1986-1999) then
          FFIEC NPW zips (2000+).
  cra     CRA aggregate & disclosure flat files -- behind the ffiec.gov WAF, so
          fetched with curl_cffi impersonating Chrome.

Resumable: a file already on disk that passes its integrity check is skipped.
A download that lands short or malformed is deleted, never left behind to be
mistaken for a complete one on the next run.

Usage:
    python download.py [call|mdrm|bhc|cra|all]
"""

from __future__ import annotations

import io
import re
import sys
import time
import zipfile
from pathlib import Path

import requests
from curl_cffi import requests as cffi_requests

from pipelines.datasets.us_ffiec_bank_reporting.common import (
    BHC_CHICAGOFED_LAST_YEAR,
    BHC_FIRST,
    BHC_LAST,
    BROWSER_UA,
    CALL_FIRST,
    CALL_LAST,
    CDR_BULK_URL,
    CDR_PRODUCT,
    CHICAGOFED_BHCF_URL,
    CRA_FIRST_YEAR,
    CRA_FLAT_URL,
    CRA_LAST_YEAR,
    IMPERSONATE,
    INPUT_DIR,
    MDRM_ZIP_URL,
    NPW_BHCF_URL,
    NPW_FIN_PAGE,
    ensure_dirs,
    mmddyyyy,
    quarters,
    yyyymmdd,
)

HIDDEN_RE = re.compile(
    r'<input type="hidden" name="([^"]+)"[^>]*value="([^"]*)"'
)


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _good_zip(path: Path, min_bytes: int = 1024) -> bool:
    if not path.exists() or path.stat().st_size < min_bytes:
        return False
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None and len(z.namelist()) > 0
    except zipfile.BadZipFile:
        return False


def _good_text(path: Path, min_bytes: int = 1024) -> bool:
    return path.exists() and path.stat().st_size >= min_bytes


def _is_bhcf(content: bytes) -> bool:
    """Does this look like a BHCF extract rather than an error page?

    A size check alone is not enough: an HTTP 200 carrying an HTML error page is
    comfortably over 1 KB, and once written it satisfies `_good_text` and is
    skipped forever on later runs -- the cached-error-page trap. Every BHCF
    file, comma-era and caret-era alike, names RSSD9001 in its header row.
    """
    head = content[:4096].upper()
    return b"RSSD9001" in head


# --------------------------------------------------------------------------
# FFIEC CDR -- Call Report bulk data
# --------------------------------------------------------------------------


def _cdr_session() -> tuple[requests.Session, dict, dict]:
    """Open a CDR session and return (session, hidden fields, period -> value)."""
    s = requests.Session()
    s.headers.update({"User-Agent": BROWSER_UA})
    first = s.get(CDR_BULK_URL, timeout=120)
    first.raise_for_status()
    fields = dict(HIDDEN_RE.findall(first.text))
    fields.update(
        {
            "__EVENTTARGET": "ctl00$MainContentHolder$ListBox1",
            "__EVENTARGUMENT": "",
            "ctl00$MainContentHolder$ListBox1": CDR_PRODUCT,
        }
    )
    second = s.post(
        CDR_BULK_URL,
        data=fields,
        timeout=180,
        headers={"Referer": CDR_BULK_URL},
    )
    second.raise_for_status()
    base = dict(HIDDEN_RE.findall(second.text))
    block = re.search(r"DatesDropDownList.*?</select>", second.text, re.S)
    if not block:
        raise RuntimeError("CDR period dropdown not found after postback")
    periods = {
        label: value
        for value, label in re.findall(
            r'value="(\d+)"[^>]*>([^<]*)</option>', block.group(0)
        )
    }
    return s, base, periods


def latest_call_period() -> tuple[int, int]:
    """Newest Call Report period the CDR actually offers, as (year, quarter).

    Read from the period dropdown rather than assumed from the calendar: a
    quarter appears there only once the FFIEC publishes it, roughly 30-45 days
    after the quarter ends, and the recurring flow must not ask for a period
    that does not exist yet.
    """
    _, _, periods = _cdr_session()
    best = max(
        (int(lbl[6:10]), (int(lbl[0:2]) - 1) // 3 + 1)
        for lbl in periods
        if len(lbl) == 10 and lbl[2] == "/" and lbl[5] == "/"
    )
    return best


def download_call(
    first: tuple[int, int] | None = None, last: tuple[int, int] | None = None
) -> None:
    """Fetch Call Report bulk zips for a period range.

    The recurring flow passes a trailing window; the one-shot onboarding passes
    nothing and gets the full CALL_FIRST..CALL_LAST span.
    """
    dest_dir = INPUT_DIR / "call"
    dest_dir.mkdir(parents=True, exist_ok=True)
    wanted = quarters(first or CALL_FIRST, last or CALL_LAST)
    todo = [
        (y, q)
        for y, q in wanted
        if not _good_zip(dest_dir / f"call_{y}Q{q}.zip")
    ]
    _log(f"call: {len(wanted)} quarters wanted, {len(todo)} to fetch")
    if not todo:
        return
    session, base, periods = _cdr_session()
    for i, (y, q) in enumerate(todo, 1):
        label = mmddyyyy(y, q)
        if label not in periods:
            _log(f"call {y}Q{q}: period {label} not offered by CDR -- SKIPPED")
            continue
        payload = dict(base)
        payload.update(
            {
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "ctl00$MainContentHolder$ListBox1": CDR_PRODUCT,
                "ctl00$MainContentHolder$DatesDropDownList": periods[label],
                "ctl00$MainContentHolder$FormatType": "TSVRadioButton",
                "ctl00$MainContentHolder$TabStrip1$Download_0": "Download",
            }
        )
        out = dest_dir / f"call_{y}Q{q}.zip"
        tmp = out.with_suffix(".part")
        r = session.post(
            CDR_BULK_URL,
            data=payload,
            timeout=1800,
            headers={"Referer": CDR_BULK_URL},
            stream=True,
        )
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
        if not _good_zip(tmp):
            size = tmp.stat().st_size if tmp.exists() else 0
            tmp.unlink(missing_ok=True)
            raise RuntimeError(
                f"call {y}Q{q}: CDR returned {size} bytes, not a usable zip. "
                "Expected non-empty for this period -- check the coverage table."
            )
        tmp.rename(out)
        _log(
            f"call {y}Q{q}: {out.stat().st_size / 1e6:.1f} MB  ({i}/{len(todo)})"
        )
        # Re-open the session periodically; the viewstate is session-scoped.
        if i % 25 == 0:
            session, base, periods = _cdr_session()


# --------------------------------------------------------------------------
# MDRM data dictionary
# --------------------------------------------------------------------------


def download_mdrm() -> None:
    out = INPUT_DIR / "mdrm" / "MDRM.zip"
    out.parent.mkdir(parents=True, exist_ok=True)
    if _good_zip(out):
        _log("mdrm: already present")
        return
    r = requests.get(
        MDRM_ZIP_URL, timeout=600, headers={"User-Agent": BROWSER_UA}
    )
    r.raise_for_status()
    out.write_bytes(r.content)
    if not _good_zip(out):
        out.unlink(missing_ok=True)
        raise RuntimeError("mdrm: downloaded file is not a usable zip")
    _log(f"mdrm: {out.stat().st_size / 1e6:.1f} MB")


# --------------------------------------------------------------------------
# FR Y-9C -- holding company consolidated financials
# --------------------------------------------------------------------------


def download_bhc(
    first: tuple[int, int] | None = None, last: tuple[int, int] | None = None
) -> None:
    """Fetch FR Y-9C/Y-9SP extracts for a period range (see download_call)."""
    dest_dir = INPUT_DIR / "bhc"
    dest_dir.mkdir(parents=True, exist_ok=True)
    wanted = quarters(first or BHC_FIRST, last or BHC_LAST)
    npw = cffi_requests.Session(impersonate=IMPERSONATE)
    warmed: set[int] = set()
    fetched = 0
    for y, q in wanted:
        out = dest_dir / f"bhcf_{y}Q{q}.txt"
        if _good_text(out):
            continue
        if y <= BHC_CHICAGOFED_LAST_YEAR:
            url = CHICAGOFED_BHCF_URL.format(yymm=f"{y % 100:02d}{3 * q:02d}")
            r = requests.get(
                url, timeout=900, headers={"User-Agent": BROWSER_UA}
            )
            if (
                r.status_code != 200
                or len(r.content) < 1024
                or not _is_bhcf(r.content)
            ):
                _log(
                    f"bhc {y}Q{q}: Chicago Fed returned {r.status_code}/"
                    f"{len(r.content)}B without an RSSD9001 header -- "
                    "not published, skipped"
                )
                continue
            out.write_bytes(r.content)
        else:
            if y not in warmed:
                npw.get(NPW_FIN_PAGE.format(year=y), timeout=180)
                warmed.add(y)
            name = f"BHCF{yyyymmdd(y, q)}.ZIP"
            r = npw.get(NPW_BHCF_URL.format(name=name), timeout=900)
            if r.status_code != 200 or r.content[:2] != b"PK":
                _log(
                    f"bhc {y}Q{q}: NPW returned {r.status_code}/"
                    f"{len(r.content)}B -- not published, skipped"
                )
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                inner = [n for n in z.namelist() if n.lower().endswith(".txt")]
                if not inner:
                    _log(f"bhc {y}Q{q}: NPW zip has no .txt member -- skipped")
                    continue
                payload = z.read(inner[0])
                if not _is_bhcf(payload):
                    _log(
                        f"bhc {y}Q{q}: NPW .txt member has no RSSD9001 "
                        "header -- skipped"
                    )
                    continue
                out.write_bytes(payload)
        fetched += 1
        _log(f"bhc {y}Q{q}: {out.stat().st_size / 1e6:.1f} MB")
    _log(
        f"bhc: {fetched} new files, {len(list(dest_dir.glob('*.txt')))} total"
    )


# --------------------------------------------------------------------------
# CRA aggregate & disclosure flat files
# --------------------------------------------------------------------------

CRA_KINDS = ("discl", "aggr", "trans")


def download_cra(
    first_year: int | None = None, last_year: int | None = None
) -> None:
    """Fetch every CRA flat file, retrying the ones the server throttles.

    www.ffiec.gov serves these fine one at a time but starts returning a 404
    HTML page (and occasionally the 403 CAPTCHA) partway through a sweep. The
    404 is not real: every URL that failed during one run returned a full zip
    on a later attempt. So a failure is retried with backoff rather than
    treated as "not published", and a file still missing at the end raises --
    a silently skipped year would otherwise become a permanent hole that looks
    like complete data on the next run.
    """
    dest_dir = INPUT_DIR / "cra"
    dest_dir.mkdir(parents=True, exist_ok=True)
    session = cffi_requests.Session(impersonate=IMPERSONATE)
    missing: list[str] = []
    for year in range(
        first_year or CRA_FIRST_YEAR, (last_year or CRA_LAST_YEAR) + 1
    ):
        for kind in CRA_KINDS:
            out = dest_dir / f"cra_{year}_{kind}.zip"
            if _good_zip(out):
                continue
            name = f"{year % 100:02d}exp_{kind}.zip"
            for attempt in range(5):
                if attempt:
                    time.sleep(5 * 2 ** (attempt - 1))
                try:
                    r = session.get(
                        CRA_FLAT_URL.format(name=name), timeout=1800
                    )
                except Exception as exc:  # transport error, same treatment
                    _log(f"cra {year} {kind}: {type(exc).__name__}, retrying")
                    continue
                if r.status_code == 200 and r.content[:2] == b"PK":
                    out.write_bytes(r.content)
                    if _good_zip(out):
                        _log(
                            f"cra {year} {kind}: {out.stat().st_size / 1e6:.1f} MB"
                        )
                        break
                    out.unlink(missing_ok=True)
                _log(
                    f"cra {year} {kind}: HTTP {r.status_code} / "
                    f"{len(r.content)}B, attempt {attempt + 1}/5"
                )
            else:
                missing.append(f"{year}/{kind}")
            time.sleep(1.5)
    if missing:
        raise RuntimeError(
            f"cra: {len(missing)} files still missing after retries: {missing}. "
            "Re-run `python download.py cra` -- the server throttles sweeps and "
            "usually serves them on a later pass."
        )


def main() -> None:
    ensure_dirs()
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    steps = {
        "mdrm": download_mdrm,
        "call": download_call,
        "bhc": download_bhc,
        "cra": download_cra,
    }
    for name in steps if what == "all" else [what]:
        _log(f"=== {name} ===")
        steps[name]()
    _log("done")


if __name__ == "__main__":
    main()
