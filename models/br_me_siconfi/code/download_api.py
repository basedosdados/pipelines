"""
Download raw SICONFI DCA data from the Tesouro Nacional API.

Downloads all government levels: Brasil (ID=1), states (2-digit IDs),
and municipalities (7-digit IDs) into one unified directory.

Each entity-year is saved as input/api/dca_{year}_{cod_ibge}.json.
Resumable: already-downloaded files are skipped automatically.

API docs: https://apidatalake.tesouro.gov.br/docs/siconfi/

Usage:
    python download_api.py                      # all entities, 2013-current year
    python download_api.py --workers 5          # parallel download with 5 workers
    python download_api.py --start-year 2020    # from 2020 onward
    python download_api.py --force              # re-download everything (full refresh)
    python download_api.py --test               # verify API connection and exit
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"
RATE_LIMIT = 1.1  # seconds between requests

# 2-digit IBGE codes for all 26 states + DF
UF_CODES = [
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    31,
    32,
    33,
    35,
    41,
    42,
    43,
    50,
    51,
    52,
    53,
]


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


def get_municipios(session):
    """Return list of all municipalities from /entes."""
    url = f"{API_BASE}/entes"
    entes = []
    offset = 0
    page_size = 5000
    while True:
        time.sleep(RATE_LIMIT)
        r = session.get(
            url,
            params={"offset": offset, "limit": page_size},
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


def get_all_entes(session):
    """Return full entity list: Brasil + states + municipalities."""
    nacional = [{"cod_ibge": "1", "ente": "Brasil"}]
    estados = [{"cod_ibge": str(c), "ente": f"UF_{c}"} for c in UF_CODES]
    municipios = get_municipios(session)
    return nacional + estados + municipios


def download_dca(session, exercicio, id_ente):
    """Download all DCA data for one entity-year. Returns dict or None."""
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


def run_worker(args, chunk_i, n_chunks, out_dir):
    """Fetch full entity list, slice this worker's chunk, and download."""
    session = make_session()
    years = list(range(args.start_year, args.end_year + 1))

    print(f"[worker {chunk_i}/{n_chunks}] Fetching entity list...")
    entes = get_all_entes(session)

    # Slice this worker's chunk (0-indexed internally)
    chunk_start = (chunk_i - 1) * len(entes) // n_chunks
    chunk_end = chunk_i * len(entes) // n_chunks
    entes = entes[chunk_start:chunk_end]

    total = len(entes) * len(years)
    done, skipped, failed = 0, 0, 0
    print(
        f"[worker {chunk_i}/{n_chunks}] "
        f"{len(entes)} entities, {total} files, "
        f"ETA ~{total * RATE_LIMIT / 3600:.1f}h"
    )

    for ente in entes:
        cod_ibge = ente.get("cod_ibge")
        nome = ente.get("ente", f"ente_{cod_ibge}")
        if not cod_ibge:
            continue

        for ano in years:
            done += 1
            out_file = out_dir / f"dca_{ano}_{cod_ibge}.json"

            if out_file.exists() and not args.force:
                skipped += 1
                continue

            try:
                data = download_dca(session, ano, cod_ibge)
            except Exception as e:
                failed += 1
                print(f"[worker {chunk_i}/{n_chunks}] ERROR {ano}/{nome}: {e}")
                continue

            if data is None or not data.get("items"):
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

        # Progress every 100 entities
        n_entes_done = done // len(years)
        if n_entes_done % 100 == 0 and done % len(years) == 0:
            remaining = total - done
            pct = 100 * done / total
            print(
                f"[worker {chunk_i}/{n_chunks}] "
                f"[{done:,}/{total:,} ({pct:.1f}%)] "
                f"downloaded={done - skipped - failed} skipped={skipped} failed={failed} "
                f"ETA={remaining * RATE_LIMIT / 3600:.1f}h"
            )

    print(
        f"[worker {chunk_i}/{n_chunks}] Finished. "
        f"downloaded={done - skipped - failed} skipped={skipped} failed={failed}"
    )
    session.close()


def main():
    parser = argparse.ArgumentParser(
        description="Download SICONFI DCA data from API (all government levels)"
    )
    parser.add_argument(
        "--out-dir", default=None, help="Output directory for JSON files"
    )
    parser.add_argument("--start-year", type=int, default=2013)
    parser.add_argument("--end-year", type=int, default=datetime.now().year)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel download workers",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download files even if they already exist",
    )
    parser.add_argument(
        "--chunk", type=int, default=None, help=argparse.SUPPRESS
    )  # internal use
    parser.add_argument(
        "--test", action="store_true", help="Test API connection and exit"
    )
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    out_dir = (
        Path(args.out_dir) if args.out_dir else script_dir / "input" / "api"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.test:
        session = make_session()
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

    # Worker mode: called internally by --workers
    if args.chunk is not None:
        run_worker(
            args, chunk_i=args.chunk, n_chunks=args.workers, out_dir=out_dir
        )
        return

    # Single-worker mode
    if args.workers == 1:
        run_worker(args, chunk_i=1, n_chunks=1, out_dir=out_dir)
        return

    # Multi-worker mode: spawn N subprocesses, each handling one chunk
    n = args.workers
    print(f"Launching {n} workers (logs: /tmp/siconfi_worker_{{1..{n}}}.log)")

    base_cmd = [
        sys.executable,
        "-u",
        __file__,
        "--workers",
        str(n),
        "--start-year",
        str(args.start_year),
        "--end-year",
        str(args.end_year),
    ]
    if args.out_dir:
        base_cmd += ["--out-dir", args.out_dir]
    if args.force:
        base_cmd += ["--force"]

    procs = []
    for i in range(1, n + 1):
        log_path = f"/tmp/siconfi_worker_{i}.log"
        log_file = open(log_path, "w")  # noqa: SIM115
        cmd = [*base_cmd, "--chunk", str(i)]
        p = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
        procs.append((i, p, log_path))
        print(f"  worker {i}: PID {p.pid}, log: {log_path}")

    print("All workers started. Waiting for completion...")
    for i, p, _log_path in procs:
        p.wait()
        print(f"  worker {i} finished (exit code {p.returncode})")

    print("All workers done.")


if __name__ == "__main__":
    main()
