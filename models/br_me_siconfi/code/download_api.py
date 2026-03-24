"""
Download raw SICONFI DCA data from the Tesouro Nacional API.

Each municipality-year is saved as input/api/dca_{year}_{cod_ibge}.json.
Resumable: already-downloaded files are skipped automatically.

API docs: https://apidatalake.tesouro.gov.br/docs/siconfi/

Usage:
    python download_api.py                          # all municipalities, 2013-current year
    python download_api.py --start-year 2020        # from 2020 onward
    python download_api.py --co-esfera E            # states instead of municipalities
    python download_api.py --test                   # verify API connection and exit
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
RATE_LIMIT = 1.1  # seconds between requests


def make_session():
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"Accept": "application/json"})
    return s


def get_entes(session, co_esfera="M"):
    """Return list of all entities for the given sphere."""
    url = f"{API_BASE}/entes"
    entes = []
    offset = 0
    page_size = 5000
    while True:
        time.sleep(RATE_LIMIT)
        r = session.get(
            url,
            params={
                "co_esfera": co_esfera,
                "offset": offset,
                "limit": page_size,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        entes.extend(items)
        if not data.get("hasMore", False) or len(items) < page_size:
            break
        offset += page_size
    return entes


def download_dca(session, exercicio, id_ente):
    """Download all DCA data for one municipality-year. Returns dict or None."""
    time.sleep(RATE_LIMIT)
    r = session.get(
        f"{API_BASE}/dca",
        params={"an_exercicio": exercicio, "id_ente": id_ente},
        timeout=60,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()

    # Handle pagination if hasMore
    items = data.get("items", [])
    while data.get("hasMore", False):
        offset = data.get("offset", 0) + data.get("limit", len(items))
        time.sleep(RATE_LIMIT)
        r2 = session.get(
            f"{API_BASE}/dca",
            params={
                "an_exercicio": exercicio,
                "id_ente": id_ente,
                "offset": offset,
            },
            timeout=60,
        )
        r2.raise_for_status()
        data = r2.json()
        items.extend(data.get("items", []))

    data["items"] = items
    return data


def main():
    parser = argparse.ArgumentParser(
        description="Download SICONFI DCA data from API"
    )
    parser.add_argument(
        "--out-dir", default=None, help="Output directory for JSON files"
    )
    parser.add_argument(
        "--co-esfera",
        default="M",
        help="M=Municípios, E=Estados e DF (default: M)",
    )
    parser.add_argument("--start-year", type=int, default=2013)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument(
        "--test", action="store_true", help="Test API connection and exit"
    )
    args = parser.parse_args()

    # Resolve output directory relative to this script
    script_dir = Path(__file__).parent
    out_dir = (
        Path(args.out_dir) if args.out_dir else script_dir / "input" / "api"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    session = make_session()

    if args.test:
        print("Testing API connection...")
        time.sleep(RATE_LIMIT)
        r = session.get(f"{API_BASE}/entes", params={"limit": 2}, timeout=30)
        print(f"  /entes status: {r.status_code}")
        data = r.json()
        print(f"  items: {len(data.get('items', []))}")
        if data.get("items"):
            ente = data["items"][0]
            print(
                f"  sample: cod_ibge={ente.get('cod_ibge')}, ente={ente.get('ente')}"
            )
        print("OK")
        return

    years = list(range(args.start_year, args.end_year + 1))
    print(f"Fetching entity list (co_esfera={args.co_esfera})...")
    entes = get_entes(session, co_esfera=args.co_esfera)
    print(f"  Found {len(entes)} entities")

    total = len(entes) * len(years)
    done, skipped, failed = 0, 0, 0

    print(
        f"Downloading {total} files ({len(entes)} entities x {len(years)} years)"
    )
    print(
        f"Estimated time: {total * RATE_LIMIT / 3600:.1f} hours at {RATE_LIMIT}s/request"
    )
    print(f"Output: {out_dir}\n")

    for ente in entes:
        cod_ibge = ente.get("cod_ibge")
        nome = ente.get("ente", f"ente_{cod_ibge}")
        if not cod_ibge:
            continue

        for ano in years:
            done += 1
            out_file = out_dir / f"dca_{ano}_{cod_ibge}.json"

            if out_file.exists():
                skipped += 1
                continue

            try:
                data = download_dca(session, ano, cod_ibge)
            except Exception as e:
                failed += 1
                print(f"  ERROR {ano}/{nome}: {e}")
                continue

            if data is None or not data.get("items"):
                # No data for this combo — write an empty sentinel to avoid re-requesting
                out_file.write_text(
                    json.dumps(
                        {
                            "metadata": {
                                "exercicio": ano,
                                "cod_ibge": cod_ibge,
                            },
                            "data": {
                                "items": [],
                                "count": 0,
                                "hasMore": False,
                            },
                        }
                    )
                )
                continue

            payload = {
                "metadata": {
                    "download_date": datetime.now().isoformat(),
                    "exercicio": ano,
                    "cod_ibge": cod_ibge,
                    "nome": nome,
                },
                "data": data,
            }
            out_file.write_text(json.dumps(payload, ensure_ascii=False))

        # Progress report every 100 entities
        n_entes_done = done // len(years)
        if n_entes_done % 100 == 0 and done % len(years) == 0:
            remaining = total - done
            eta_h = remaining * RATE_LIMIT / 3600
            pct = 100 * done / total
            print(
                f"  [{done:,}/{total:,} ({pct:.1f}%)] "
                f"done={done - skipped - failed} skipped={skipped} failed={failed} "
                f"ETA={eta_h:.1f}h"
            )

    print(
        f"\nFinished. done={done - skipped - failed} skipped={skipped} failed={failed}"
    )
    session.close()


if __name__ == "__main__":
    main()
