"""Download the Minas Gerais source packages from the TCE-MG open data gateway.

TCE-MG publishes SICOM municipal budget execution through a WSO2 API gateway. There is
no CKAN, no static file tree and no per-file URL: every byte moves through
`baixarArquivoPct/<seqZip>`, which returns ONE zip holding all 853 municipalities for
one exercise and one category. Two categories carry what MiDES needs -- "Empenhos"
(empenho + restos a pagar) and "Despesas" (liquidação + pagamento) -- so a full
2014-2026 backfill is 26 package requests, not 22,000 per-file ones.

THE TOKEN IS AN INPUT TO THIS SCRIPT. IT IS NOT ACQUIRED HERE.

Every endpoint requires two headers together:

    Authorization:      Bearer <static, published in the SPA's own JS bundle>
    AuthorizationProxy: token <JWT, 120-minute TTL, per session>

The JWT is issued only by `login/captcha.jsf`, a page protected by reCAPTCHA v3. This
module does not touch that page, does not drive a browser, does not call a solving
service and does not replay a captured token on the user's behalf. Automating that gate
is bot-detection circumvention, and TCE-MG has not consented to it. The token is read
from `MG_API_TOKEN` or `--token`, a human obtains it in their own browser, and the
proper fix is a service credential -- see TCE_MG_CREDENTIAL_REQUEST.md beside this file.

Two further traps, both cheap and both fatal if missed:

*   **The TLS chain is incomplete.** Both TCE-MG hosts send the leaf certificate without
    the Sectigo OV R36 intermediate. macOS `curl` succeeds because the system keychain
    supplies the missing link; Python with certifi fails every request with
    CERTIFICATE_VERIFY_FAILED. `_ca_bundle()` concatenates certifi with the pinned
    intermediate in `certs/`. Verified from this repo: bare certifi raises SSLError,
    the concatenated bundle returns the expected unauthenticated 401.
*   **`seqZip` is not stable across exercises.** It is a database sequence, not a
    content address; 2026's "Empenhos" was 109824 and 2025's was 109814. It is resolved
    from `buscarCategoriaDownload` on every run and is never cached or hardcoded.

Usage:
    export MG_API_TOKEN='<jwt>'
    uv run python models/world_wb_mides/code/download_mg.py --year 2022 --year 2023
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import time
import unicodedata
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import certifi
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    MG_CA_BUNDLE,
    MG_CATEGORIES_URL,
    MG_CATEGORY_PHASES,
    MG_FIRST_YEAR,
    MG_MUNICIPALITIES,
    MG_PACKAGE,
    MG_STATIC_BEARER,
    MG_STATS_URL,
    REQUEST_INTERVAL_SECONDS,
)

MG_INPUT = INPUT_DIR / "mg"
CHUNK = 1 << 20

HOW_TO_GET_A_TOKEN = """
HOW TO OBTAIN A TCE-MG TOKEN.

This script will not obtain one for you: the only issuer is a reCAPTCHA-protected page,
and automating it is circumvention. A human does this, in a browser, in about a minute:

  1. Open https://dadosabertos.tce.mg.gov.br/ in a normal browser.
  2. Let the page finish loading. reCAPTCHA v3 is invisible and scores the session
     without presenting a challenge, so there is nothing to click.
  3. Open the developer console and read the issued JWT:
         localStorage.getItem('tokenAuthorizationProxy')
     (Equivalently: Application -> Local Storage -> the same key.)
  4. Hand it to this script, WITHOUT the leading "token " prefix:
         export MG_API_TOKEN='eyJ...'
         uv run python models/world_wb_mides/code/download_mg.py

The token lives 120 minutes. A full backfill can outlast it; the script says exactly
which packages are already on disk when it expires, and a re-run with a fresh token
resumes rather than restarting.

This is the interim path, not the intended one. The fix is a service credential from
TCE-MG -- the ask is drafted in TCE_MG_CREDENTIAL_REQUEST.md beside this file.
""".strip()


class TokenError(RuntimeError):
    """The proxy token is missing, malformed, rejected or expired."""


def _ca_bundle() -> str:
    """certifi plus the intermediate TCE-MG omits from its chain, as one file.

    Written once per process into the temp dir. Returning a path (rather than setting
    REQUESTS_CA_BUNDLE) keeps the override scoped to this session and leaves every other
    TLS client in the process alone.
    """
    if not MG_CA_BUNDLE.exists():
        raise FileNotFoundError(
            f"missing pinned intermediate {MG_CA_BUNDLE}. Without it every request to "
            "TCE-MG fails with CERTIFICATE_VERIFY_FAILED, because the server does not "
            "send it."
        )
    descriptor, path = tempfile.mkstemp(prefix="tce_mg_ca_", suffix=".pem")
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(Path(certifi.where()).read_bytes())
        handle.write(b"\n")
        handle.write(MG_CA_BUNDLE.read_bytes())
    return path


def _token_minutes_left(token: str) -> float | None:
    """Minutes until `exp`, read out of the JWT payload. None if unreadable.

    The payload is decoded, not verified -- there is no public key here and no need for
    one. The point is a loud warning before a four-hour backfill is started on a token
    with eleven minutes left on it.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload))["exp"]
    except Exception:
        return None
    return (
        datetime.fromtimestamp(exp, tz=UTC) - datetime.now(tz=UTC)
    ).total_seconds() / 60


def resolve_token(token: str | None) -> str:
    """`--token` beats `MG_API_TOKEN`; absent either, fail with instructions."""
    token = (token or os.environ.get("MG_API_TOKEN") or "").strip()
    if not token:
        raise TokenError(
            f"No token supplied in --token or $MG_API_TOKEN.\n\n{HOW_TO_GET_A_TOKEN}"
        )
    if token.lower().startswith("token "):
        # The header value is "token <jwt>"; people copy the whole thing out of the
        # network tab. Accept it rather than sending "token token <jwt>".
        token = token[len("token ") :].strip()
    if token.count(".") != 2:
        raise TokenError(
            f"MG_API_TOKEN does not look like a JWT (expected 2 dots, got "
            f"{token.count('.')}).\n\n{HOW_TO_GET_A_TOKEN}"
        )
    return token


def session_for(token: str) -> requests.Session:
    session = requests.Session()
    session.verify = _ca_bundle()
    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "*/*",
            "Authorization": f"Bearer {MG_STATIC_BEARER}",
            "AuthorizationProxy": f"token {token}",
        }
    )
    return session


def _get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """One paced request, with 401 translated into an actionable token error.

    A 401 here is never a bug in the URL: an unauthenticated call to a valid path
    returns 401 with a zero-length body, and a wrong path with valid headers returns 403
    with WSO2 code 900906. So 401 means the token, and only the token.
    """
    time.sleep(REQUEST_INTERVAL_SECONDS)
    response = session.get(url, timeout=kwargs.pop("timeout", 300), **kwargs)
    if response.status_code == 401:
        raise TokenError(
            f"401 from {url} -- the proxy token was rejected. A TCE-MG token lives 120 "
            f"minutes; if this run had already downloaded packages, they are on disk in "
            f"{MG_INPUT} and a re-run with a fresh token will skip them.\n\n"
            f"{HOW_TO_GET_A_TOKEN}"
        )
    response.raise_for_status()
    return response


def _fold(text: str) -> str:
    """Accent- and case-insensitive key for matching the category display labels."""
    stripped = unicodedata.normalize("NFKD", text)
    return (
        "".join(c for c in stripped if not unicodedata.combining(c))
        .strip()
        .lower()
    )


def categories(session: requests.Session, year: int) -> dict[str, dict]:
    """The packages published for one exercise, keyed by the labels we consume.

    Returns only the two MiDES needs. The label is matched folded, because the portal's
    own copy is not stable in case or accent and an exact-string match would silently
    return nothing -- which reads identically to "this exercise has no data".
    """
    response = _get(
        session,
        MG_CATEGORIES_URL,
        params={"exercicio": year, "origem": "SICOM"},
    )
    try:
        listed = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{year}: buscarCategoriaDownload returned {len(response.content)} bytes "
            f"that are not JSON. First 200: {response.content[:200]!r}"
        ) from exc
    wanted = {_fold(label): label for label in MG_CATEGORY_PHASES}
    found: dict[str, dict] = {}
    for entry in listed:
        label = wanted.get(_fold(str(entry.get("categoria", ""))))
        if label:
            found[label] = entry
    missing = set(MG_CATEGORY_PHASES) - set(found)
    if missing:
        seen = sorted({str(e.get("categoria")) for e in listed})
        raise RuntimeError(
            f"{year}: no package for {sorted(missing)}. Categories offered: {seen}. "
            "An exercise that genuinely has no data returns an empty list; a renamed "
            "label returns the others, so check the list above before assuming a gap."
        )
    return found


def download_package(
    session: requests.Session, year: int, label: str, entry: dict
) -> Path | None:
    """Fetch one whole-state package to disk. Returns None if already present.

    The count and size the portal advertises for the package are checked against what
    arrives, because the failure mode that matters is a short read: a truncated zip
    still opens (the central directory is at the end, so usually it does not -- but a
    zip truncated at a member boundary does) and a quietly incomplete exercise looks
    exactly like a real one with fewer municipalities.
    """
    seq_zip = entry.get("seqZip")
    if seq_zip is None:
        raise RuntimeError(
            f"{year} {label}: category entry carries no seqZip: {entry}"
        )
    dest = MG_INPUT / f"{_fold(label)}_{year}.zip"
    if dest.exists() and dest.stat().st_size > 0:
        print(
            f"  {dest.name}: already on disk ({dest.stat().st_size / 1e6:.0f} MB)"
        )
        return None

    advertised = entry.get("qtdArquivos")
    if advertised is not None and int(advertised) != MG_MUNICIPALITIES:
        print(
            f"  WARNING {year} {label}: portal advertises {advertised} files, expected "
            f"{MG_MUNICIPALITIES} municipalities"
        )

    url = MG_PACKAGE.format(seq_zip=seq_zip)
    tmp = dest.with_suffix(".part")
    written = 0
    time.sleep(REQUEST_INTERVAL_SECONDS)
    with session.get(url, stream=True, timeout=3600) as response:
        if response.status_code == 401:
            raise TokenError(
                f"401 while downloading {year} {label}. The token expired mid-flight "
                f"(they live 120 minutes). Completed packages are in {MG_INPUT} and a "
                f"re-run with a fresh token resumes.\n\n{HOW_TO_GET_A_TOKEN}"
            )
        response.raise_for_status()
        expected = int(response.headers.get("Content-Length") or 0)
        head = b""
        with open(tmp, "wb") as handle:
            for chunk in response.iter_content(CHUNK):
                if not head:
                    head = chunk[:4]
                handle.write(chunk)
                written += len(chunk)
    # An expired session or a gateway error can arrive as HTTP 200 carrying an HTML
    # page or a JSON fault. Writing that to `empenhos_2023.zip` and letting the cleaner
    # discover it later turns a five-second failure into a confusing one.
    if head[:2] != b"PK":
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"{year} {label}: response is not a zip (magic {head[:4]!r}). The gateway "
            "returns 200 with an error body when a session goes bad."
        )
    if expected and written != expected:
        tmp.unlink(missing_ok=True)
        raise OSError(
            f"{year} {label}: short read, got {written} bytes, expected {expected}"
        )
    # The check above is necessary and NOT sufficient: these packages are built on the
    # fly, so the gateway answers with chunked transfer encoding and sends no
    # Content-Length at all. `expected` is then 0, the comparison is skipped, and a
    # connection dropped mid-stream produces a file with a valid PK header, a plausible
    # size, and no central directory.
    #
    # That is not hypothetical. Fetching these same packages through the portal's own
    # browser UI on 2026-09-21 produced `2025_EMPENHOS.zip` at 54.8 MB of 583 MB (191 of
    # 853 municipalities) and `2025_DESPESAS.zip` at 52.4 MB of 1,607 MB (165 of 853).
    # Both ended mid-member with no EOCD record, and the UI reported them as completed
    # downloads. A dropped connection looks exactly like success.
    #
    # The central directory is the one part of a zip that cannot be written until the
    # whole stream has arrived, so parsing it is the proof of completeness. It is read
    # from the tail only -- no member is decompressed -- and it is done HERE rather than
    # left to the cleaner, because by then the partial file has a permanent-looking name
    # and the run that produced it is long gone.
    try:
        with zipfile.ZipFile(tmp) as archive:
            members = len(archive.namelist())
    except zipfile.BadZipFile as exc:
        size = tmp.stat().st_size
        tmp.unlink(missing_ok=True)
        raise OSError(
            f"{year} {label}: TRUNCATED. {size:,} bytes arrived but the zip has no "
            f"readable central directory ({exc}). The transfer was cut short; retry."
        ) from exc
    # The central directory proves the zip ENDS where a zip should end; it does
    # not prove nothing was dropped, because a transfer cut at a member boundary
    # can still leave a readable one. `qtdArquivos` is the only independent
    # statement of how many members there should be, so compare against it --
    # otherwise a short package is accepted, renamed to its final name, and
    # looks complete to everything downstream.
    if advertised is not None and members != int(advertised):
        size = tmp.stat().st_size
        tmp.unlink(missing_ok=True)
        raise OSError(
            f"{year} {label}: SHORT. {members} members in {size:,} bytes, but the "
            f"portal advertises {advertised}. The transfer was cut short; retry."
        )
    if members < MG_MUNICIPALITIES:
        print(
            f"  NOTE {year} {label}: {members} members for {MG_MUNICIPALITIES} "
            f"municipalities -- complete as far as the archive is concerned, but the "
            f"source did not file for every municipality this exercise"
        )
    tmp.replace(dest)
    print(
        f"  {dest.name}: {written / 1e6:.0f} MB, {members} members (seqZip {seq_zip})"
    )
    return dest


def main(
    years: set[int] | None = None,
    token: str | None = None,
    retries: int = 3,
    last_year: int | None = None,
) -> None:
    MG_INPUT.mkdir(parents=True, exist_ok=True)
    token = resolve_token(token)

    minutes = _token_minutes_left(token)
    if minutes is not None:
        if minutes <= 0:
            raise TokenError(
                f"the supplied token expired {abs(minutes):.0f} minutes ago.\n\n"
                f"{HOW_TO_GET_A_TOKEN}"
            )
        print(f"token valid for a further {minutes:.0f} min")
        if minutes < 20:
            print(
                "  WARNING under 20 minutes left. A whole-exercise package is hundreds "
                "of MB; start from a fresh token rather than half-finishing a year."
            )

    session = session_for(token)

    # `datCarga` is the portal-wide last-load stamp and the right poll signal for a
    # recurring pipeline: unchanged means there is nothing new to fetch. Also serves as
    # the cheapest possible check that the token works before a 300 MB request.
    stats = _get(session, MG_STATS_URL).json()
    print(
        f"portal datCarga={stats.get('datCarga')} files={stats.get('qtdArquivos')}"
    )

    # The SPA's exercise dropdown is a hardcoded client-side list that stops short of
    # the newest exercise; the API serves it regardless. Bound by the calendar, and let
    # `categories()` report an exercise that genuinely is not published.
    ceiling = last_year or datetime.now(tz=UTC).year
    span = (
        years if years is not None else set(range(MG_FIRST_YEAR, ceiling + 1))
    )
    if not span:
        raise ValueError("no exercises requested")
    print(f"exercises: {sorted(span)}")

    # TokenError is deliberately NOT retried and NOT collected: it subclasses
    # RuntimeError, so without the explicit re-raise below an expired token would be
    # retried three times per package and then reported as thirteen separate per-year
    # failures, burying the one line that says what actually went wrong.
    failures: list[tuple[int, str, str]] = []
    for year in sorted(span):
        try:
            found = categories(session, year)
        except TokenError:
            raise
        except RuntimeError as exc:
            print(f"  {year}: {exc}")
            failures.append((year, "-", str(exc)))
            continue
        for label in MG_CATEGORY_PHASES:
            for attempt in range(1, retries + 1):
                try:
                    download_package(session, year, label, found[label])
                    break
                except TokenError:
                    raise
                except (
                    requests.RequestException,
                    OSError,
                    RuntimeError,
                ) as exc:
                    print(f"  {year} {label} attempt {attempt}: {exc}")
                    if attempt == retries:
                        failures.append((year, label, str(exc)))

    if failures:
        print("\nFAILED:")
        for year, label, message in failures:
            print(f"  {year} {label}: {message}")
        raise SystemExit(1)
    print("MG download complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--token",
        help="TCE-MG proxy JWT. Defaults to $MG_API_TOKEN. Obtained by a human in a "
        "browser; this script never issues one.",
    )
    parser.add_argument(
        "--year", type=int, action="append", help="restrict to this exercise"
    )
    parser.add_argument(
        "--last-year",
        type=int,
        help="upper bound for a full backfill (default: current calendar year)",
    )
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()
    try:
        main(
            years=set(args.year) if args.year else None,
            token=args.token,
            retries=args.retries,
            last_year=args.last_year,
        )
    except TokenError as exc:
        print(f"\n{exc}", file=sys.stderr)
        raise SystemExit(2) from exc
