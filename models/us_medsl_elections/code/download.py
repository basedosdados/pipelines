"""Download raw MEDSL election-returns files for us_medsl_elections.

Six files come from the MIT Election Data and Science Lab (MEDSL) on Harvard
Dataverse. Four sit behind Dataverse "General guestbook" (guestbookID 458).
That guestbook has no custom questions; it only requires name/email/
institution/position. The plain access API returns:

    {"status":"ERROR","message":"You may not download this file without the
     required Guestbook response for guestbookID 458."}

Bypass: POST the guestbook response to
    /api/access/datafile/<fileId>[?format=original]
which returns a short-lived signedUrl; then GET that signedUrl. Requesting
?format=original yields the original CSV (preserves leading zeros in FIPS
codes) for ingested .tab files.

One file (senate_state) has no guestbook and downloads directly. The 2016
state-offices file is fetched from the MEDSL GitHub mirror as a fallback
(identical to the Dataverse original).

Files are written to models/us_medsl_elections/input/. Re-runs skip files
already present (idempotent).
"""

import json
import time
import urllib.error
import urllib.request
from pathlib import Path

INPUT_DIR = Path(__file__).resolve().parents[1] / "input"
INPUT_DIR.mkdir(parents=True, exist_ok=True)

API = "https://dataverse.harvard.edu/api/access/datafile"

# Anonymous guestbook response (no custom questions on guestbook 458).
GUESTBOOK = {
    "guestbookResponse": {
        "name": "Data Basis",
        "email": "contato@basedosdados.org",
        "institution": "Data Basis",
        "position": "Researcher",
    }
}

# filename -> (fileId, needs_guestbook, use_format_original, github_raw_fallback)
FILES = {
    "president_1976_2024.csv": (13887042, True, False, None),
    "senate_state_1976_2024.tab": (13887039, False, True, None),
    "house_1976_2024.tab": (13592823, True, True, None),
    "countypres_2000_2024.tab": (13573089, True, True, None),
    "senate_county_2022.tab": (7412054, True, True, None),
    "state_offices_2016.tab": (
        3826178,
        True,
        True,
        "https://raw.githubusercontent.com/MEDSL/state-returns/master/stateoffices2016.csv",
    ),
}

UA = "Mozilla/5.0 (Data Basis onboarding pipeline)"


def _get(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(f"GET failed for {url}: {e}") from e


def _post_guestbook(file_id: int, fmt_original: bool, retries: int = 3) -> str:
    """Submit the guestbook response; return the signed download URL."""
    url = f"{API}/{file_id}"
    if fmt_original:
        url += "?format=original"
    body = json.dumps(GUESTBOOK).encode()
    for attempt in range(retries):
        try:
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
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(
                    f"guestbook POST failed for {file_id}: {e}"
                ) from e


def _looks_like_error(data: bytes) -> bool:
    head = data[:64].lstrip()
    return (
        head.startswith(b'{"status":"ERROR"')
        or head.startswith(b"<!DOCTYPE")
        or head.startswith(b"<html")
    )


def download(name: str, spec) -> None:
    file_id, needs_gb, fmt_original, gh_fallback = spec
    dest = INPUT_DIR / name
    if dest.exists() and dest.stat().st_size > 0:
        print(f"SKIP  {name} (exists, {dest.stat().st_size / 1e6:.1f} MB)")
        return

    data = None
    method = None
    try:
        if needs_gb:
            signed = _post_guestbook(file_id, fmt_original)
            data = _get(signed)
            method = f"guestbook-POST -> signedUrl ({'format=original' if fmt_original else 'default'})"
        else:
            url = f"{API}/{file_id}" + (
                "?format=original" if fmt_original else ""
            )
            data = _get(url)
            method = f"direct GET ({'format=original' if fmt_original else 'default'})"
        if _looks_like_error(data):
            raise RuntimeError("response looks like an error / HTML page")
    except Exception as e:
        if gh_fallback:
            print(
                f"WARN  {name}: Dataverse failed ({e}); trying GitHub mirror"
            )
            data = _get(gh_fallback)
            method = f"GitHub mirror {gh_fallback}"
        else:
            raise

    dest.write_bytes(data)
    print(
        f"OK    {name}  {dest.stat().st_size / 1e6:.2f} MB  via {method}  "
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]"
    )


def main() -> None:
    for name, spec in FILES.items():
        download(name, spec)


if __name__ == "__main__":
    main()
