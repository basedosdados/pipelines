"""Pure download and cleaning functions for br_pncp. No Prefect imports.

Both the recurring pipeline (``tasks.py``) and the one-shot onboarding scripts
under ``models/br_pncp/code/`` import from here, so the transform exists in
exactly one place.

The PNCP consulta API has four constraints that shape the download:

1. ``tamanhoPagina`` is capped at 500 and floored at 10.
2. A date window may not exceed 365 days (HTTP 422 beyond that).
3. Large result sets make the server fail rather than paginate: a full-year
   window on a high-volume modalidade intermittently returns HTTP 500, and pages
   deep into a large result set return HTTP 504. ``fetch_range`` halves any
   window the server refuses, so this needs no per-endpoint tuning.
4. Requests are rate limited (HTTP 429) well below what a naive loop issues, and
   the limit applies per source IP across all endpoints — two processes hitting
   the API concurrently throttle each other. The 429 body is HTML, not JSON, and
   carries no ``Retry-After``.

Output conventions, both required downstream:

* Staging parquet is **all-STRING**. The dbt models ``safe_cast`` every column,
  and ``upload_to_gcs`` infers the staging schema from a stringified one-row
  header, so typed parquet is rejected. The cast goes through arrow rather than
  ``astype(str)``, which would write the literal ``"nan"`` for NULL — a value
  ``safe_cast`` cannot turn back into NULL.
* The partition column ``ano`` is written into the **directory name only**.
  Writing it into the file as well makes the hive-partitioned dataset unreadable
  ("Field ano has incompatible types: string vs dictionary<values=int32>").
"""

from __future__ import annotations

import concurrent.futures
import csv
import gzip
import http.client
import json
import os
import random
import shutil
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_pncp.constants import constants

BASE_URL = constants.BASE_URL.value
USER_AGENT = constants.USER_AGENT.value
PAGE_SIZE = constants.PAGE_SIZE.value
ENDPOINTS = constants.ENDPOINTS.value
ARCHITECTURE_DIR = constants.ARCHITECTURE_DIR.value

# Grain: the column(s) that uniquely identify a row, and the column used to pick
# the surviving version when the same key appears in several harvest windows.
DEDUP_KEYS = {
    "contratacao": (["id_contratacao_pncp"], "data_atualizacao"),
    "contrato": (["id_contrato_pncp"], "data_atualizacao"),
    "ata_registro_preco": (["id_ata_pncp"], "data_atualizacao"),
    "instrumento_cobranca": (
        [
            "cnpj_orgao",
            "ano_contrato",
            "sequencial_contrato",
            "sequencial_instrumento_cobranca",
        ],
        "data_atualizacao",
    ),
    "plano_contratacao_anual": (
        ["id_pca_pncp", "numero_item"],
        "data_atualizacao",
    ),
}

# Which raw field the partition year is derived from, per table.
PARTITION_SOURCE = {
    "contratacao": "dataPublicacaoPncp",
    "contrato": "dataPublicacaoPncp",
    "ata_registro_preco": "dataPublicacaoPncp",
    "instrumento_cobranca": "dataInclusao",
    "plano_contratacao_anual": "anoPca",
}

# Tables whose API payload nests a list that must be exploded into one row per
# element, mapping table -> the raw array field.
EXPLODE = {"plano_contratacao_anual": "itens"}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #


class ServerOverloadError(Exception):
    """The window is too large for the server to answer. Split it."""


class RateLimitedError(Exception):
    """The API is refusing requests for rate. Back off; do NOT split.

    Splitting a rate-limited window is actively harmful: it replaces one
    refused request with two, against the component that is already saying
    it has had too many.
    """


class Throttle:
    """Shared pacer with adaptive backoff, safe to call from several threads.

    Requests are spread across worker threads to hide the API's 5-7s response
    time, so the rate cap has to be enforced globally rather than per thread.
    ``wait`` reserves the next slot under the lock and sleeps outside it, which
    keeps the aggregate request rate at most ``1 / interval`` regardless of how
    many workers are running.
    """

    def __init__(self, min_interval: float = 0.35):
        self.base = min_interval
        self.interval = min_interval
        self._next_free = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            start = max(time.monotonic(), self._next_free)
            self._next_free = start + self.interval
        delay = start - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def penalise(self) -> None:
        # Capped low on purpose. The API's limit is on *concurrency*, not rate,
        # so a global rate penalty is a blunt instrument: when four workers cross
        # the ceiling together they each penalise, compounding 1.6^4 in one burst
        # and serialising every worker behind a ~6s pacer. Sustained throughput
        # then collapsed to ~590 pages/hour. Staying below the ceiling and
        # penalising gently is far faster than exceeding it and backing off hard.
        with self._lock:
            self.interval = min(self.interval * 1.3, 2.0)

    def relax(self) -> None:
        # Recover faster than the 5%/success the first version used: at an 8s
        # penalised interval that took ~50 successes to return to baseline, which
        # dominated the run long after the rate limiter had stopped complaining.
        with self._lock:
            self.interval = max(self.base, self.interval * 0.8)


THROTTLE = Throttle(float(os.environ.get("PNCP_MIN_INTERVAL", "0.05")))

# Per-page logging. A window takes minutes, so without it a healthy run and a
# stalled one are indistinguishable from the outside.
VERBOSE = os.environ.get("PNCP_VERBOSE", "1") == "1"

# Everything meaning "the connection misbehaved, retry" rather than "the
# server answered and said no". IncompleteRead is an http.client exception,
# not a URLError, so a narrower tuple lets a truncated chunked response
# escape and abort a multi-hour harvest.
TRANSPORT_ERRORS = (
    urllib.error.URLError,
    http.client.HTTPException,
    ConnectionError,
    TimeoutError,
    ssl.SSLError,
    json.JSONDecodeError,
)


def request(path: str, params: dict, max_tries: int = 6) -> dict:
    """One API call, with rate-limit backoff and overload detection."""
    url = BASE_URL + path + "?" + urllib.parse.urlencode(params)
    rate_limited = False
    for attempt in range(max_tries):
        THROTTLE.wait()
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
            )
            with urllib.request.urlopen(req, timeout=240) as resp:
                if resp.status == 204:
                    THROTTLE.relax()
                    return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
                payload = json.loads(resp.read().decode("utf-8"))
            THROTTLE.relax()
            return payload
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                THROTTLE.penalise()
                rate_limited = True
                time.sleep(min(60, 5 * (attempt + 1)) + random.uniform(0, 2))
                continue
            if exc.code in (500, 502, 503, 504):
                # Retry a couple of times; a persistent failure means the result
                # set is too large and the caller must split the window.
                if attempt >= 2:
                    raise ServerOverloadError(f"{exc.code} on {url}") from exc
                time.sleep(4 * (attempt + 1))
                continue
            if exc.code == 422:
                raise ServerOverloadError(f"422 on {url}") from exc
            raise RuntimeError(
                f"HTTP {exc.code} on {url}: {exc.read()[:200]!r}"
            ) from exc
        except TRANSPORT_ERRORS:
            if attempt >= max_tries - 2:
                raise ServerOverloadError(
                    f"transport failure on {url}"
                ) from None
            time.sleep(4 * (attempt + 1))
    if rate_limited:
        raise RateLimitedError(f"rate limited on {url}")
    raise ServerOverloadError(f"exhausted retries on {url}")


def fetch_window(path: str, params: dict, label: str = "") -> list[dict]:
    """Page through one window, raising ServerOverloadError if it is too large."""
    records: list[dict] = []
    page = 1
    started = time.monotonic()
    while True:
        payload = request(
            path, {**params, "pagina": page, "tamanhoPagina": PAGE_SIZE}
        )
        batch = payload.get("data") or []
        records.extend(batch)
        total_pages = payload.get("totalPaginas") or 0
        if VERBOSE and label:
            print(
                f"      {label} page {page}/{total_pages} "
                f"(+{len(batch)}, {time.monotonic() - started:.0f}s, "
                f"pacer {THROTTLE.interval:.2f}s)",
                flush=True,
            )
        if page >= total_pages or not batch:
            break
        page += 1
    return records


def windows(start: date, end: date, days: int):
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=days - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


def fetch_range(
    path: str,
    date_params: tuple[str, str],
    lo: date,
    hi: date,
    extra: dict,
    label: str = "",
) -> list[dict]:
    """Fetch [lo, hi], halving the window whenever the server buckles."""
    p_from, p_to = date_params
    params = {
        **extra,
        p_from: lo.strftime("%Y%m%d"),
        p_to: hi.strftime("%Y%m%d"),
    }
    for cooldown in (60, 180, 420):
        try:
            return fetch_window(path, params, label)
        except RateLimitedError:
            print(
                f"      .. rate limited on {lo}..{hi}, cooling down {cooldown}s",
                flush=True,
            )
            time.sleep(cooldown)
        except ServerOverloadError:
            break
    else:
        raise RateLimitedError(
            f"still rate limited on {lo}..{hi} after cooldowns"
        )

    try:
        return fetch_window(path, params, label)
    except ServerOverloadError:
        if lo == hi:
            # A single day the server cannot serve. Report and continue rather
            # than aborting the whole harvest.
            print(
                f"      !! unrecoverable single day {lo} on {path}", flush=True
            )
            return []
        mid = lo + (hi - lo) // 2
        print(f"      .. splitting {lo}..{hi} on {path}", flush=True)
        return fetch_range(
            path, date_params, lo, mid, extra, label
        ) + fetch_range(
            path, date_params, mid + timedelta(days=1), hi, extra, label
        )


def write_chunk(target: Path, records: list[dict]) -> None:
    """Write one NDJSON chunk atomically, so an interrupted run leaves no partial."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".partial")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    tmp.replace(target)


def harvest(
    table: str,
    input_dir: Path,
    start: date,
    end: date,
    path_override: str | None = None,
    max_workers: int = int(os.environ.get("PNCP_WORKERS", "3")),
) -> int:
    """Download one table's records for [start, end] into gzipped NDJSON chunks.

    A chunk file is skipped when it already exists, which makes a long backfill
    resumable after an interruption.

    Args:
        table: Table slug, a key of ``constants.ENDPOINTS``.
        input_dir: Root for raw chunks; files land in ``<input_dir>/<table>/``.
        start: First date of the harvest range, inclusive.
        end: Last date of the harvest range, inclusive.
        path_override: Use this API path instead of the endpoint's default.
            The backfill passes the publication-date endpoints here.
        max_workers: Windows fetched concurrently. The API answers a page in
            5-8s, so the harvest is latency-bound and needs concurrency. Do not
            raise this to 4: that sits on the API's concurrency ceiling and
            measured 2x *slower* than 3, because all workers trip 429s together
            and the compounding penalty serialises them.

    Returns:
        Number of records downloaded in this call (skipped chunks count zero).
    """
    spec = ENDPOINTS[table]
    path = path_override or spec["path"]
    combos = (
        [
            {"codigoModalidadeContratacao": m}
            for m in constants.MODALIDADES.value
        ]
        if spec["by_modalidade"]
        else [{}]
    )

    jobs = []
    for lo, hi in windows(start, end, spec["window_days"]):
        for extra in combos:
            tag = f"{lo:%Y%m%d}_{hi:%Y%m%d}"
            if extra:
                tag += f"_m{extra['codigoModalidadeContratacao']:02d}"
            target = input_dir / table / f"{tag}.jsonl.gz"
            if target.exists():
                continue
            jobs.append((lo, hi, extra, tag, target))

    if not jobs:
        return 0

    failures: list[str] = []

    def run_job(job) -> int:
        lo, hi, extra, tag, target = job
        print(f"  {table} {tag}: start", flush=True)
        try:
            records = fetch_range(
                path, spec["date_params"], lo, hi, extra, f"{table} {tag}"
            )
        except Exception as exc:
            # Deliberately broad. A harvest runs for hours; one window that
            # cannot be fetched must not discard the other thousands. No chunk
            # file is written, so re-running retries exactly this window, and
            # the count is reported at the end so it is never silent.
            failures.append(tag)
            print(
                f"  !! {table} {tag}: FAILED ({type(exc).__name__}: {exc})",
                flush=True,
            )
            return 0
        write_chunk(target, records)
        print(f"  {table} {tag}: {len(records):>7,} rows", flush=True)
        return len(records)

    # Windows are independent and each writes its own chunk file, so they
    # parallelise cleanly. The workers hide the API's 5-7s per-page latency;
    # the shared THROTTLE still bounds the aggregate request rate, so raising
    # this does not raise the rate against the server's limiter.
    total = 0
    if max_workers <= 1:
        for job in jobs:
            total += run_job(job)
        return total

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as pool:
        for count in pool.map(run_job, jobs):
            total += count
    if failures:
        print(
            f"  !! {table}: {len(failures)} window(s) failed and were left for a "
            f"re-run: {', '.join(failures[:10])}"
            + (" ..." if len(failures) > 10 else ""),
            flush=True,
        )
    return total


# --------------------------------------------------------------------------- #
# Cleaning
# --------------------------------------------------------------------------- #


def read_architecture(table: str) -> list[dict]:
    with (Path(ARCHITECTURE_DIR) / f"{table}.csv").open(
        encoding="utf-8"
    ) as fh:
        return list(csv.DictReader(fh))


def dig(record: dict, path: str):
    """Resolve a dotted original_name against a nested payload."""
    cur = record
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def as_date(value):
    """Normalise PNCP date and date-time strings to an ISO date."""
    if value in (None, ""):
        return None
    text = str(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def as_number(value, integer: bool):
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return str(int(number)) if integer else repr(number)


def as_bool(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return {"true": "true", "false": "false"}.get(str(value).strip().lower())


def as_string(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip() or None


def convert(value, bq_type: str):
    """Cast one raw value to its final *string* representation.

    The real type is applied first, so the string form is the one the dbt
    ``safe_cast`` expects: an INT64 year serialises as ``"2025"``, not
    ``"2025.0"``.
    """
    if bq_type == "DATE":
        return as_date(value)
    if bq_type == "INT64":
        return as_number(value, integer=True)
    if bq_type == "FLOAT64":
        return as_number(value, integer=False)
    if bq_type == "BOOLEAN":
        return as_bool(value)
    return as_string(value)


def partition_year(record: dict, table: str) -> int | None:
    raw = record.get(PARTITION_SOURCE[table])
    if raw in (None, ""):
        return None
    if table == "plano_contratacao_anual":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    iso = as_date(raw)
    return int(iso[:4]) if iso else None


def flatten(record: dict, table: str, columns: list[dict]) -> list[dict]:
    """Turn one API record into one or more flat, all-string rows."""
    array_field = EXPLODE.get(table)
    children = (record.get(array_field) or [{}]) if array_field else [{}]

    year = partition_year(record, table)
    rows = []
    for child in children:
        row = {}
        for spec in columns:
            name, bq_type, original = (
                spec["name"],
                spec["bigquery_type"],
                spec["original_name"],
            )
            if name == "ano" or not original:
                row[name] = None
                continue
            if array_field and original.startswith(f"{array_field}."):
                raw = dig(child, original.split(".", 1)[1])
            else:
                raw = dig(record, original)
            row[name] = convert(raw, bq_type)
        row["ano"] = str(year) if year is not None else None
        rows.append(row)
    return rows


def iter_raw(input_dir: Path, table: str):
    for path in sorted((input_dir / table).glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def clean_table(
    input_dir: Path,
    output_dir: Path,
    table: str,
    replace: bool = True,
    batch_rows: int = int(os.environ.get("PNCP_BATCH_ROWS", "50000")),
) -> dict:
    """Stream one table's raw chunks into partitioned all-STRING parquet.

    **Deduplication happens in the dbt model, not here.** An earlier version held
    every row in a ``{key: row}`` dict to keep the latest ``data_atualizacao``,
    which is fine for a small table and fatal for this one: contrato alone is
    4.8M rows of 46 string columns, tens of GB of Python objects, and it
    exhausted the machine's RAM. The models already carry an unconditional
    ``QUALIFY row_number() ... = 1`` on the same key, so the in-memory pass was
    duplicated work as well as a memory bomb. Staging therefore holds duplicates
    by design and BigQuery collapses them.

    Memory is bounded by ``batch_rows`` per open partition: rows accumulate per
    year and flush to a numbered parquet part on reaching the threshold. Chunks
    are read in chronological filename order, so typically only one or two years
    are open at a time. Several parts per partition directory are fine — a
    hive-partitioned external table reads every file in the directory.

    Args:
        input_dir: Root holding ``<table>/*.jsonl.gz``.
        output_dir: Root to write ``<table>/ano=<year>/data_NNNN.parquet`` under.
        table: Table slug.
        replace: Remove the table's existing output tree first. True for the
            one-shot backfill, where the run produces the complete table. False
            for an incremental pipeline run, which produces only the partitions
            its window touched and must not delete the others.
        batch_rows: Rows buffered per partition before a part is written.

    Returns:
        Summary with raw and written row counts and the years touched. Note that
        ``written_rows`` counts rows *before* deduplication, which is what
        staging will contain; the materialized table will hold fewer.
    """
    columns = read_architecture(table)
    names = [c["name"] for c in columns]
    file_names = [n for n in names if n != "ano"]
    schema = pa.schema([(n, pa.string()) for n in file_names])

    table_dir = output_dir / table
    if replace and table_dir.exists():
        shutil.rmtree(table_dir)

    buffers: dict[str, list[dict]] = {}
    parts: dict[str, int] = {}
    raw_rows = 0
    written = 0
    undated = 0

    def flush(year: str) -> int:
        rows = buffers.pop(year, None)
        if not rows:
            return 0
        index = parts.get(year, 0)
        parts[year] = index + 1
        target = table_dir / f"ano={year}"
        target.mkdir(parents=True, exist_ok=True)
        arrays = [
            pa.array([r.get(n) for r in rows], type=pa.string())
            for n in file_names
        ]
        pq.write_table(
            pa.Table.from_arrays(arrays, schema=schema),
            target / f"data_{index:04d}.parquet",
            compression="snappy",
        )
        return len(rows)

    for record in iter_raw(input_dir, table):
        for row in flatten(record, table, columns):
            raw_rows += 1
            year = row["ano"]
            if year is None:
                undated += 1
                continue
            buffers.setdefault(year, []).append(row)
            if len(buffers[year]) >= batch_rows:
                written += flush(year)

    for year in sorted(buffers):
        written += flush(year)

    return {
        "table": table,
        "raw_rows": raw_rows,
        "written_rows": written,
        "undated_dropped": undated,
        "years": sorted(parts),
    }


# --------------------------------------------------------------------------- #
# dicionario
# --------------------------------------------------------------------------- #

# table -> [(code column, label column)] where PNCP ships the label alongside
# the code, so the dictionary is derived from the data and cannot drift from it.
DICIONARIO_DERIVED = {
    "contratacao": [
        ("id_modalidade", "modalidade"),
        ("id_modo_disputa", "modo_disputa"),
        ("id_situacao_compra", "situacao_compra"),
        ("id_tipo_instrumento_convocatorio", "tipo_instrumento_convocatorio"),
        ("codigo_amparo_legal", "nome_amparo_legal"),
    ],
    "contrato": [
        ("id_tipo_contrato", "tipo_contrato"),
        ("id_categoria_processo", "categoria_processo"),
    ],
    "instrumento_cobranca": [
        ("id_tipo_instrumento_cobranca", "tipo_instrumento_cobranca"),
    ],
    "plano_contratacao_anual": [
        ("id_classificacao_catalogo", "nome_classificacao_catalogo"),
    ],
}

# Codes the API never labels. Source: PNCP manual de integração domain tables.
DICIONARIO_HARDCODED = {
    "id_esfera": {
        "F": "Federal",
        "E": "Estadual",
        "M": "Municipal",
        "D": "Distrital",
        "N": "Não se aplica",
    },
    "id_poder": {
        "E": "Executivo",
        "L": "Legislativo",
        "J": "Judiciário",
        "N": "Não se aplica",
    },
    "tipo_pessoa_fornecedor": {
        "PJ": "Pessoa jurídica",
        "PF": "Pessoa física",
        "PE": "Pessoa estrangeira",
    },
}

DICIONARIO_HARDCODED_TABLES = {
    "id_esfera": ["contratacao", "contrato"],
    "id_poder": ["contratacao", "contrato"],
    "tipo_pessoa_fornecedor": ["contrato"],
}


def distinct_pairs(
    output_dir: Path, table: str, code_col: str, label_col: str
) -> dict:
    """Read the distinct code -> label pairs present in a cleaned table."""
    import pyarrow.dataset as pa_ds

    table_dir = output_dir / table
    if not table_dir.exists():
        return {}
    dataset = pa_ds.dataset(table_dir, format="parquet", partitioning="hive")
    if (
        code_col not in dataset.schema.names
        or label_col not in dataset.schema.names
    ):
        return {}
    scanned = dataset.to_table(columns=[code_col, label_col])
    pairs: dict[str, str] = {}
    for code, label in zip(
        scanned.column(code_col).to_pylist(),
        scanned.column(label_col).to_pylist(),
        strict=True,
    ):
        if code is None or label is None:
            continue
        pairs.setdefault(str(code), str(label))
    return pairs


def build_dicionario(output_dir: Path) -> int:
    """Rebuild the dicionario table from the cleaned fact tables.

    Returns the number of dictionary rows written.
    """
    rows: list[dict] = []

    for table, pairs_spec in DICIONARIO_DERIVED.items():
        for code_col, label_col in pairs_spec:
            mapping = distinct_pairs(output_dir, table, code_col, label_col)
            for code, label in sorted(
                mapping.items(), key=lambda kv: (len(kv[0]), kv[0])
            ):
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": code_col,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
            print(
                f"  dicionario {table}.{code_col}: {len(mapping)} keys",
                flush=True,
            )

    for column, mapping in DICIONARIO_HARDCODED.items():
        for table in DICIONARIO_HARDCODED_TABLES[column]:
            for code, label in mapping.items():
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )

    names = [c["name"] for c in read_architecture("dicionario")]
    target = output_dir / "dicionario"
    target.mkdir(parents=True, exist_ok=True)
    arrays = [
        pa.array([r.get(n) for r in rows], type=pa.string()) for n in names
    ]
    pq.write_table(
        pa.Table.from_arrays(
            arrays, schema=pa.schema([(n, pa.string()) for n in names])
        ),
        target / "data.parquet",
        compression="snappy",
    )
    print(f"  dicionario: {len(rows):,} rows", flush=True)
    return len(rows)
