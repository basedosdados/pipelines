"""Download raw MEDSL election-returns files for us_medsl_elections.

Two corpora are downloaded from the MIT Election Data and Science Lab (MEDSL)
on Harvard Dataverse:

1. AGGREGATE national files (six long-panel tables) written to input/.
2. PRECINCT-level returns (~210 files, ~11 GB) written to
   input/precinct/<year>/. These come from four "by Individual State"
   datasets (2018, 2020, 2022, 2024; one data file per state) plus five
   per-office 2016 national files (fileIds known).

Download mechanism (works for guestbook-locked and open files alike):
Dataverse's "General guestbook" blocks the plain access API with

    {"status":"ERROR","message":"You may not download this file without the
     required Guestbook response for guestbookID <n>."}

Bypass: POST an anonymous guestbook response to
    /api/access/datafile/<fileId>?format=original
which returns a short-lived signedUrl; then GET that signedUrl. Requesting
?format=original yields the ORIGINAL CSV (preserving leading zeros in FIPS /
precinct codes) for Dataverse-ingested .tab files, and is a harmless no-op
for native (non-ingested) .csv files. The POST returns a valid signedUrl
even when no guestbook is attached, so the same call path serves both.

Files are idempotent on re-run (existing non-empty, header-valid files are
skipped). A manifest is written to input/precinct/manifest.json.
"""

import json
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

INPUT_DIR = Path(__file__).resolve().parents[1] / "input"
PRECINCT_DIR = INPUT_DIR / "precinct"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

API = "https://dataverse.harvard.edu/api/access/datafile"
DATASET_API = "https://dataverse.harvard.edu/api/datasets/:persistentId/"
UA = "Mozilla/5.0 (Data Basis onboarding pipeline)"

# Anonymous guestbook response (MEDSL guestbooks carry no custom questions).
GUESTBOOK = {
    "guestbookResponse": {
        "name": "Data Basis",
        "email": "contato@basedosdados.org",
        "institution": "Data Basis",
        "position": "Researcher",
    }
}

# ---------------------------------------------------------------------------
# Corpus 1: aggregate national long-panel files (written to input/).
# filename -> (fileId, use_format_original, github_raw_fallback)
# ---------------------------------------------------------------------------
AGGREGATE_FILES = {
    "president_1976_2024.csv": (13887042, False, None),
    "senate_state_1976_2024.tab": (13887039, True, None),
    "house_1976_2024.tab": (13592823, True, None),
    "countypres_2000_2024.tab": (13573089, True, None),
    "senate_county_2022.tab": (7412054, True, None),
    "state_offices_2016.tab": (
        3826178,
        True,
        "https://raw.githubusercontent.com/MEDSL/state-returns/master/stateoffices2016.csv",
    ),
}

# ---------------------------------------------------------------------------
# Corpus 2: precinct-level returns.
# ---------------------------------------------------------------------------
STATE_DATASETS = {
    2018: "doi:10.7910/DVN/NVQYMG",
    2020: "doi:10.7910/DVN/NT66Z3",
    2022: "doi:10.7910/DVN/UYQIEP",
    2024: "doi:10.7910/DVN/NYTPDU",
}

NATIONAL_2016 = {
    "president": 3345331,
    "senate": 3345325,  # guestbook-locked
    "house": 6412765,
    "state": 3345337,
    "local": 3345356,
}

# 50 states + DC. Postal codes and full names (for the non-standard 2022 files).
POSTAL = {
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "dc",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
}
FULL_NAME = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "district of columbia": "dc",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}

# Header tokens common to MEDSL precinct / office returns; used to validate
# that a download is real tabular data rather than an error JSON or HTML.
EXPECTED_TOKENS = (
    "precinct",
    "office",
    "votes",
    "candidate",
    "county_fips",
    "party",
)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
# pyrefly: ignore [bad-return]
def _http_get_bytes(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=180) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(f"GET failed for {url}: {e}") from e


def _post_guestbook(file_id: int, fmt_original: bool) -> str:
    """Submit the guestbook response; return the signed download URL."""
    url = f"{API}/{file_id}" + ("?format=original" if fmt_original else "")
    body = json.dumps(GUESTBOOK).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        payload = json.load(r)
    if payload.get("status") != "OK":
        raise RuntimeError(f"guestbook POST error: {payload}")
    return payload["data"]["signedUrl"]


def _stream_to_file(signed: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(signed, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
    tmp.replace(dest)


def _header_line(path: Path) -> str:
    with open(path, "rb") as f:
        return f.readline(1 << 16).decode("utf-8", errors="replace").strip()


def _is_valid_data(path: Path) -> bool:
    """True if the file opens as CSV with a plausible MEDSL header."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    head = _header_line(path).lower()
    if not head or head.startswith("{") or head.startswith("<"):
        return False
    if "," not in head:
        return False
    hits = sum(1 for t in EXPECTED_TOKENS if t in head)
    return hits >= 2


def _count_rows(path: Path) -> int:
    cnt = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            cnt += b.count(b"\n")
    return max(cnt - 1, 0)  # minus header


# ---------------------------------------------------------------------------
# State-code inference from filename
# ---------------------------------------------------------------------------
def filename_to_state(fname: str):
    """Map a MEDSL data filename to a 2-letter uppercase state code, or None."""
    base = fname.rsplit(".", 1)[0].lower()
    spaced = " " + re.sub(r"[^a-z]+", " ", base).strip() + " "
    # Full state names first (longest first to avoid virginia/west virginia).
    for name in sorted(FULL_NAME, key=len, reverse=True):
        if f" {name} " in spaced:
            return FULL_NAME[name].upper()
    # First alphabetic token that is exactly a valid postal code.
    for tok in re.findall(r"[a-z]+", base):
        if tok in POSTAL:
            return tok.upper()
    return None


# ---------------------------------------------------------------------------
# Generic download-one-file (guestbook POST -> signedUrl -> stream)
# ---------------------------------------------------------------------------
def download_datafile(
    file_id: int,
    dest: Path,
    fmt_original: bool = True,
    retries: int = 3,
    gh_fallback: str | None = None,
) -> str:
    """Download one Dataverse datafile to dest. Returns a status string."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if _is_valid_data(dest):
        return (
            f"SKIP  {dest.name} ({dest.stat().st_size / 1e6:.1f} MB, exists)"
        )

    last_err = None
    for attempt in range(retries):
        try:
            signed = _post_guestbook(file_id, fmt_original)
            _stream_to_file(signed, dest)
            if not _is_valid_data(dest):
                raise RuntimeError("downloaded file failed header validation")
            return (
                f"OK    {dest.name}  {dest.stat().st_size / 1e6:.2f} MB  "
                f"[{time.strftime('%H:%M:%S')}]"
            )
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(2**attempt)

    if gh_fallback:
        try:
            data = _http_get_bytes(gh_fallback)
            dest.write_bytes(data)
            if _is_valid_data(dest):
                return f"OK    {dest.name} via GitHub mirror"
        except Exception as e:
            last_err = e
    raise RuntimeError(f"FAILED {dest.name} (fileId {file_id}): {last_err}")


# ---------------------------------------------------------------------------
# Planning: enumerate precinct files from the four state datasets
# ---------------------------------------------------------------------------
def build_precinct_plan() -> list[dict]:
    """Return a list of download tasks for the precinct corpus."""
    plan: list[dict] = []

    for year, doi in STATE_DATASETS.items():
        url = f"{DATASET_API}?persistentId={doi}"
        meta = json.loads(_http_get_bytes(url).decode())
        files = meta["data"]["latestVersion"]["files"]
        for f in files:
            df = f["dataFile"]
            fname = df["filename"]
            ctype = df.get("contentType", "")
            # Skip README / codebook markdown files.
            if fname.lower().endswith(".md") or ctype == "text/markdown":
                continue
            # 2020 North Carolina: skip the redundant "-sorted" duplicate.
            if "-sorted" in fname.lower():
                continue
            state = filename_to_state(fname)
            if state is None:
                key = re.sub(r"[^A-Za-z0-9]+", "_", fname.rsplit(".", 1)[0])
                dest = PRECINCT_DIR / str(year) / f"{key}.csv"
                plan.append(
                    {
                        "year": year,
                        "key": key,
                        "file_id": df["id"],
                        "dest": dest,
                        "src_filename": fname,
                        "unmapped": True,
                    }
                )
            else:
                dest = PRECINCT_DIR / str(year) / f"{state}.csv"
                plan.append(
                    {
                        "year": year,
                        "key": state,
                        "file_id": df["id"],
                        "dest": dest,
                        "src_filename": fname,
                        "unmapped": False,
                    }
                )

    # 2016 national per-office files.
    for office, fid in NATIONAL_2016.items():
        dest = PRECINCT_DIR / "2016" / f"{office}.csv"
        plan.append(
            {
                "year": 2016,
                "key": office,
                "file_id": fid,
                "dest": dest,
                "src_filename": f"2016 {office} (fileId {fid})",
                "unmapped": False,
            }
        )
    return plan


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------
def run_aggregate() -> None:
    print("=== Aggregate national files -> input/ ===")
    for name, (fid, fmt, gh) in AGGREGATE_FILES.items():
        dest = INPUT_DIR / name
        try:
            print(
                download_datafile(fid, dest, fmt_original=fmt, gh_fallback=gh)
            )
        except Exception as e:
            print(f"ERROR {name}: {e}")


def run_precinct(max_workers: int = 4) -> None:
    print("\n=== Precinct corpus -> input/precinct/<year>/ ===")
    plan = build_precinct_plan()
    print(
        f"Planned {len(plan)} precinct files across "
        f"{sorted({t['year'] for t in plan})}"
    )

    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(
                download_datafile,
                t["file_id"],
                t["dest"],
                True,
                3,
                None,
            ): t
            for t in plan
        }
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                print(f"[{t['year']} {t['key']}] {fut.result()}")
            except Exception as e:
                msg = f"[{t['year']} {t['key']}] {e}"
                print(msg)
                errors.append(msg)

    write_manifest(plan)

    if errors:
        print(f"\n{len(errors)} file(s) FAILED:")
        for m in errors:
            print("  " + m)
    unmapped = [t for t in plan if t["unmapped"]]
    if unmapped:
        print(f"\n{len(unmapped)} UNMAPPED file(s) (saved under raw name):")
        for t in unmapped:
            print(f"  {t['year']}: {t['src_filename']} -> {t['dest'].name}")


def write_manifest(plan: list[dict]) -> None:
    manifest: dict[str, dict] = {}
    for t in plan:
        dest: Path = t["dest"]
        year = str(t["year"])
        entry = {
            "filename": dest.name,
            "src_filename": t["src_filename"],
            "fileId": t["file_id"],
            "rows": _count_rows(dest) if dest.exists() else None,
            "bytes": dest.stat().st_size if dest.exists() else None,
        }
        manifest.setdefault(year, {})[t["key"]] = entry
    out = PRECINCT_DIR / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"\nManifest written: {out}")


def main() -> None:
    run_aggregate()
    run_precinct()


if __name__ == "__main__":
    main()
