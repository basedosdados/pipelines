"""Download Santa Catarina execution documents from the SIGEF transparency portal.

SC is fetched from the portal's own export endpoint rather than its CKAN bulk files.
The reason is not preference: the bulk `empenhos-<ano>.csv` files cannot be parsed at
all (see `constants.SC_API`), while the export is correctly quoted. The export also
covers 2011+ against the bulk files' 2021+, and carries all three phases where the
bulk set has empenho live and liquidação/pagamento frozen at a 2022-03 snapshot.

One request per (visão, month): `exportcsv` takes `anomesinifiltro`/`anomesfimfiltro`
and returns the whole result set for the period, not a page of it. A month of empenho
is ~12.6 MB; the full 2011-2026 series is ~26M rows across the three visões.

**Every month is verified against the API's own `lista.total` before it is kept.** An
export endpoint that silently returns a partial result is the failure mode that matters
here, and it is cheap to rule out: the JSON sibling of `exportcsv` answers the same
filters with a row count, and on 2022-03 empenho both say 13,402.

Files are written as the raw bytes the server sends (cp1252). The transcode to UTF-8
happens in `clean_sc.py`, so `input/` stays a faithful mirror of the source.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clean_sc
from constants import (
    BROWSER_UA,
    INPUT_DIR,
    SC_API,
    SC_COUNT_ENDPOINT,
    SC_ENCODING,
    SC_EXPORT_ENDPOINT,
    SC_FIRST_YEAR,
    SC_LAST_YEAR,
    SC_VISOES,
)

SC_INPUT = INPUT_DIR / "sc"

# The portal is the only referer the endpoint is ever called from in the wild. It is
# not enforced today, but sending it costs nothing and keeps us honest about what the
# request is.
HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "*/*",
    "Referer": "https://www.transparencia.sc.gov.br/",
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def expected_rows(
    session: requests.Session, visao: str, ym: str
) -> int | None:
    """Control total for one (visão, month), from the JSON sibling of the export."""
    for attempt in range(4):
        try:
            r = session.get(
                f"{SC_API}/{SC_COUNT_ENDPOINT}",
                params={
                    "visao": visao,
                    "anomesinifiltro": ym,
                    "anomesfimfiltro": ym,
                    "page": 1,
                },
                timeout=300,
            )
            body = r.json()
            # A bad `visao` returns a bare `[]` rather than an error status, so the
            # shape is checked instead of the status code.
            if not isinstance(body, dict):
                raise ValueError(
                    f"unexpected payload for {visao} {ym}: {body!r:.80}"
                )
            return body.get("lista", {}).get("total", 0)
        except ValueError:
            raise
        except Exception:
            time.sleep(6 * (attempt + 1))
    return None


def _decode(raw: bytes) -> str:
    """cp1252 -> str, keeping the five bytes cp1252 leaves undefined.

    Those bytes (0x81, 0x8D, 0x8F, 0x90, 0x9D) decode to their latin-1 characters,
    which is lossless, rather than to U+FFFD.
    """
    try:
        return raw.decode(SC_ENCODING)
    except UnicodeDecodeError:
        return "".join(
            bytes([b]).decode(SC_ENCODING, errors="ignore")
            or bytes([b]).decode("latin-1")
            for b in raw
        )


def count_records(raw: bytes) -> int:
    """Records in the export, counted exactly the way `clean_sc` parses them.

    **This must not be an independent implementation.** SC uses a backslash as an escape
    character in some files and as literal data in others; under the wrong convention a
    single record can SPLIT IN TWO, so two parsers with different settings disagree on
    the row count itself. That is not hypothetical -- it rejected liquidação 2024-02
    four times as "82,095 rows, expected 82,094" when the file was fine and the counters
    simply disagreed. `clean_sc.parse_records` picks the convention per file; this
    reuses it so the download check and the clean can never drift apart.
    """
    return len(clean_sc.parse_records(_decode(raw))[0]) - 1


def fetch_month(
    session: requests.Session, visao: str, ym: str, want: int, retries: int = 4
) -> tuple[str, int]:
    dest = SC_INPUT / visao / f"{visao}_{ym}.csv"
    meta = dest.with_suffix(".json")
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and meta.exists():
        recorded = json.loads(meta.read_text())
        # Re-download when the source has grown: an open exercise keeps changing, and a
        # "file exists and is non-empty" check would freeze it at whatever it was.
        if recorded.get("rows") == want:
            return "skip", recorded["rows"]

    for attempt in range(retries):
        try:
            r = session.get(
                f"{SC_API}/{SC_EXPORT_ENDPOINT}",
                params={
                    "visao": visao,
                    "anomesinifiltro": ym,
                    "anomesfimfiltro": ym,
                },
                timeout=1800,
            )
            ctype = r.headers.get("content-type", "")
            if r.status_code != 200 or not ctype.startswith("text/csv"):
                print(
                    f"    {visao} {ym}: HTTP {r.status_code} {ctype[:40]} "
                    f"(attempt {attempt + 1})",
                    flush=True,
                )
                time.sleep(15 * (attempt + 1))
                continue

            raw = r.content
            got = count_records(raw)
            if got != want:
                # Refuse rather than keep it: a short export is the one failure this
                # source can produce that looks exactly like success.
                print(
                    f"    {visao} {ym}: got {got:,} rows, expected {want:,} "
                    f"(attempt {attempt + 1})",
                    flush=True,
                )
                time.sleep(20 * (attempt + 1))
                continue

            tmp = dest.with_suffix(".part")
            tmp.write_bytes(raw)
            tmp.replace(dest)
            meta.write_text(
                json.dumps(
                    {"visao": visao, "ym": ym, "rows": got, "bytes": len(raw)}
                )
            )
            return "ok", got
        except Exception as exc:
            print(
                f"    {visao} {ym}: {type(exc).__name__} (attempt {attempt + 1})",
                flush=True,
            )
            time.sleep(20 * (attempt + 1))
    return "FAIL", 0


def main(
    visoes: tuple[str, ...] = SC_VISOES,
    years: set[int] | None = None,
    pause: float = 0.6,
) -> None:
    session = _session()
    years = years or set(range(SC_FIRST_YEAR, SC_LAST_YEAR + 1))
    failures: list[str] = []
    totals: dict[str, int] = {}

    for visao in visoes:
        subtotal = 0
        for year in sorted(years):
            for month in range(1, 13):
                ym = f"{year}{month:02d}"
                want = expected_rows(session, visao, ym)
                if want is None:
                    failures.append(f"{visao} {ym} (count unavailable)")
                    continue
                if want == 0:
                    continue
                status, n = fetch_month(session, visao, ym, want)
                if status == "FAIL":
                    failures.append(f"{visao} {ym}")
                subtotal += n
                print(f"  {visao:<11} {ym} {status:<5} {n:>9,}", flush=True)
                time.sleep(pause)
        totals[visao] = subtotal
        print(f"== {visao}: {subtotal:,} rows", flush=True)

    for visao, n in totals.items():
        print(f"{visao:<12} {n:>12,}")
    if failures:
        raise SystemExit(
            f"{len(failures)} month(s) could not be downloaded or did not match the "
            f"published row count: {', '.join(failures[:20])}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--visao", choices=SC_VISOES, action="append")
    parser.add_argument("--year", type=int, action="append")
    args = parser.parse_args()
    main(
        visoes=tuple(args.visao) if args.visao else SC_VISOES,
        years=set(args.year) if args.year else None,
    )
