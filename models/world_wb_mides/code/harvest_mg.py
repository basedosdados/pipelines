"""Harvest TCE-MG SICOM municipal files one municipality at a time.

WHY THIS EXISTS ALONGSIDE `download_mg.py`
------------------------------------------
`download_mg.py` pulls whole-state packages: one zip per exercise per category,
853 municipalities inside. That route is cheap in requests and *unreliable in
practice*. The gateway builds those packages on the fly and answers with chunked
transfer encoding -- no Content-Length -- so a connection dropped mid-stream
yields a file with a valid `PK` header, a plausible size, and no central
directory. Both the portal's own browser UI and `curl` produced exactly that on
2026-09-21: `2025_EMPENHOS.zip` at 54.8 MB of 583 MB (191 of 853 municipalities),
`2025_DESPESAS.zip` at 52.4 MB of 1,607 MB (165 of 853). A dropped connection is
indistinguishable from success until something downstream reads the archive.

This module takes the other route the gateway offers: one file per
(municipality, exercise, category), 3 KB to a few hundred MB each. The unit of
failure becomes one municipality instead of a whole state, every unit is
independently verifiable, and a 20-hour harvest resumes exactly where it stopped.

THE TOKEN IS AN INPUT. IT IS NOT ACQUIRED HERE.
-----------------------------------------------
Every endpoint needs two headers together: the static `Authorization: Bearer`
published in the SPA's own JS bundle, and `AuthorizationProxy: token <JWT>`. The
JWT is minted only by a page load scored by reCAPTCHA v3, lives 120 minutes, and
has no refresh endpoint -- the SPA stores it and nothing else. A human loads the
portal in a real browser and copies it out; see `--help-token`.

Because a full harvest outlasts any single token, a 401 is NOT fatal here. The
worker pool parks, re-reads the token file on an interval, and resumes the moment
a fresh token appears. Nothing is lost and nothing is re-downloaded.

WHAT IS VERIFIED, AND WHY THAT SET
-----------------------------------
`baixarArquivo` does not return a zip. It returns JSON --
`{seqArquivo, nomeArquivo, tamanhoBytes, bytesConteudo}` -- with the archive
base64-encoded in `bytesConteudo`, so 26 GB of zip moves as ~35 GB on the wire.
That shape is a gift for integrity: a short read breaks JSON parsing long before
anything reaches disk. Three layers, cheapest first:

  1. the response parses as JSON            (catches a truncated transfer)
  2. the base64 decodes                     (catches a corrupted body)
  3. the zip's central directory parses     (catches a short archive)

Deliberately NOT checked: `tamanhoBytes` from the listing against the delivered
size. The packages are rebuilt per request, so the two legitimately disagree by a
few hundred bytes (Abaeté 2025 despesa: 3,060,565 advertised, 3,061,488
delivered). Equality there would reject good files; it is recorded, not enforced.

Members are listed, never decompressed. Reading the central directory proves the
whole stream arrived -- it is the one part of a zip that cannot be written early.

A BONUS THAT MATTERS DOWNSTREAM
--------------------------------
`nomeArquivo` is `SICOM.<year>.<ibge>.<categoria>.zip`, e.g.
`SICOM.2025.3100203.despesa.zip`. The IBGE municipality code is in the name. MG's
liquidacao/pagamento/rsp CSVs carry no municipality column at all -- the existing
cleaner recovers it from the bulk zip's *member* names. Here it arrives in the
file name itself, per municipality, for free.

Usage:
    export MG_TOKEN_FILE=/path/to/token.txt
    python harvest_mg.py --plan                      # enumerate, no downloads
    python harvest_mg.py --year 2025                 # harvest one exercise
    python harvest_mg.py                             # everything, 2014-2026
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import queue
import random
import re
import sys
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import certifi
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import BROWSER_UA, MG_API, MG_CA_BUNDLE, MG_STATIC_BEARER

# Raw data never lands in the repo or in Dropbox: 26 GB would trigger a sync and
# risk a commit. Overridable, but the default is the documented scratch location.
DATA_ROOT = Path(
    os.environ.get(
        "MG_DATA_DIR", Path.home() / "Downloads" / "world_wb_mides_data"
    )
)
MG_INPUT = DATA_ROOT / "input" / "mg"
MANIFEST = MG_INPUT / "_manifest.json"
LEDGER = MG_INPUT / "_ledger.jsonl"

# The four categories MiDES consumes, as the API spells them. The display labels
# on the portal ("Despesas", "Licitações") are NOT these; the per-file endpoint
# takes the singular unaccented slug and answers an empty body for anything else.
CATEGORIES = ("despesa", "empenho", "contrato", "licitacao")

FIRST_YEAR = 2014
# The SPA's year dropdown stops short of the live exercise; the API serves it
# regardless. Bounded by the calendar, and an exercise with no data simply
# enumerates to nothing.
LAST_YEAR = datetime.now(tz=UTC).year

# Measured 2026-09-22 on the same link: 1 worker 164 KB/s, 2 workers 302,
# 8 workers 690, 12 workers 1106. The gateway caps ~250 KB/s per CONNECTION and
# ~1.1 MB/s per client, so twelve is where the client ceiling is reached and a
# thirteenth would only add a connection for no bytes. Verified against the bulk
# route, which hit the same ~1 MB/s wall at 4 streams and did not move at 6.
WORKERS = 12
TIMEOUT = 900
TOKEN_POLL_SECONDS = 60
MAX_ATTEMPTS = 4

HELP_TOKEN = """
HOW TO OBTAIN A TCE-MG TOKEN

There is no refresh endpoint and no service credential. A page load scored by
reCAPTCHA v3 is the only issuer, so a human does this in a real browser:

  1. Open https://dadosabertos.tce.mg.gov.br/ and let it finish loading.
     reCAPTCHA v3 is invisible and scores the session -- nothing to click.
  2. In the developer console:
         localStorage.getItem('tokenAuthorizationProxy')
  3. Write that value (no "token " prefix) to a file, and point at it:
         export MG_TOKEN_FILE=~/mg_token.txt

The token lives 120 minutes. This harvester does not die when it expires: it
parks, re-reads the file every minute, and resumes. Refresh by reloading the
portal page -- a reload mints a new 120-minute token -- and overwriting the file.
""".strip()


class TokenExpiredError(RuntimeError):
    """A 401. The token is stale; the pool parks rather than failing."""


# --------------------------------------------------------------------------- #
# token
# --------------------------------------------------------------------------- #
class TokenFile:
    """The token, re-read from disk whenever it changes.

    A harvest outlives its token several times over, so the value cannot be
    captured once at startup. Reading is guarded by mtime so the common path is a
    stat, not a read, and by a lock so eight workers cannot tear a half-written
    file mid-refresh.
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._mtime: float | None = None
        self._value: str = ""

    def get(self) -> str:
        with self._lock:
            try:
                mtime = self.path.stat().st_mtime
            except FileNotFoundError:
                raise TokenExpiredError(
                    f"token file {self.path} does not exist"
                ) from None
            if mtime != self._mtime:
                raw = self.path.read_text().strip()
                if raw.lower().startswith("token "):
                    raw = raw[len("token ") :].strip()
                self._value, self._mtime = raw, mtime
            if self._value.count(".") != 2:
                raise TokenExpiredError(f"{self.path} does not hold a JWT")
            return self._value

    def minutes_left(self) -> float | None:
        """Minutes until `exp`, decoded (not verified) from the payload."""
        try:
            payload = self.get().split(".")[1]
            payload += "=" * (-len(payload) % 4)
            exp = json.loads(base64.urlsafe_b64decode(payload))["exp"]
        except Exception:
            return None
        return (exp - datetime.now(tz=UTC).timestamp()) / 60


def ca_bundle() -> str:
    """certifi plus the intermediate TCE-MG omits from its chain.

    Both TCE-MG hosts send the leaf alone, without `Sectigo Public Server
    Authentication CA OV R36`. macOS `curl` hides this by filling the gap from the
    system keychain; Python does not. Note the intermediate is necessary but not
    sufficient -- its own issuer, Sectigo Root R46, must be in certifi, which it
    is only from ~2023 onward. The repo venv pins certifi 2021.10.8 and fails
    every request here with `unable to get issuer certificate`; run this module
    against a modern certifi.
    """
    if not MG_CA_BUNDLE.exists():
        raise FileNotFoundError(f"missing pinned intermediate {MG_CA_BUNDLE}")
    descriptor, path = tempfile.mkstemp(prefix="tce_mg_ca_", suffix=".pem")
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(Path(certifi.where()).read_bytes())
        handle.write(b"\n")
        handle.write(MG_CA_BUNDLE.read_bytes())
    return path


CA = None


def session_for(token_file: TokenFile) -> requests.Session:
    session = requests.Session()
    session.verify = CA
    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "*/*",
            "Authorization": f"Bearer {MG_STATIC_BEARER}",
        }
    )
    return session


def _request(session, token_file, method, url, **kwargs):
    """One request with the current token, 401 raised as TokenExpiredError."""
    headers = kwargs.pop("headers", {})
    headers["AuthorizationProxy"] = f"token {token_file.get()}"
    response = session.request(
        method,
        url,
        headers=headers,
        timeout=kwargs.pop("timeout", TIMEOUT),
        **kwargs,
    )
    if response.status_code == 401:
        raise TokenExpiredError(f"401 from {url}")
    response.raise_for_status()
    return response


def wait_for_fresh_token(token_file: TokenFile, stop: threading.Event) -> None:
    """Park until the token file changes to something with time left on it.

    Called from every worker, so the whole pool converges here on expiry and
    leaves together. Prints once per minute rather than once per worker per
    minute.
    """
    announced = False
    while not stop.is_set():
        try:
            left = token_file.minutes_left()
            if left is not None and left > 1:
                if announced:
                    print(
                        f"  token refreshed, {left:.0f} min -- resuming",
                        flush=True,
                    )
                return
        except TokenExpiredError:
            pass
        if not announced:
            print(
                f"\n  *** TOKEN EXPIRED. Waiting for a fresh one in "
                f"{token_file.path}.\n"
                f"  *** Reload https://dadosabertos.tce.mg.gov.br/ and copy\n"
                f"  ***   localStorage.getItem('tokenAuthorizationProxy')\n"
                f"  *** into that file. Nothing is lost; the harvest resumes.\n",
                flush=True,
            )
            announced = True
        stop.wait(TOKEN_POLL_SECONDS)


# --------------------------------------------------------------------------- #
# enumeration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Item:
    year: int
    categoria: str
    municipio: str
    seq: int
    advertised: int

    @property
    def key(self) -> str:
        return f"{self.year}|{self.categoria}|{self.municipio}"


def municipalities(session, token_file) -> list[str]:
    listed = _request(
        session, token_file, "GET", MG_API + "/buscarMunicipios", timeout=120
    ).json()
    names = [entry["campo"] for entry in listed]
    if not names:
        raise RuntimeError("buscarMunicipios returned an empty list")
    return names


def detail(
    session, token_file, municipio: str, categoria: str, year: int
) -> list[dict]:
    """Files published for one municipality/category/exercise.

    `origem=SICOM` is mandatory and undocumented: without it the gateway answers
    400 Bad Request, which reads like a malformed name rather than a missing
    parameter. An empty `arquivoList` is a real answer -- in 2017 only 696 of 853
    municipalities filed despesa -- and is recorded as such, not retried.
    """
    response = _request(
        session,
        token_file,
        "GET",
        MG_API + "/buscarDetalhesCategorias",
        params={
            "municipio": municipio,
            "categoria": categoria,
            "exercicio": year,
            "origem": "SICOM",
        },
        timeout=120,
    )
    if not response.content:
        return []
    try:
        return response.json().get("arquivoList") or []
    except ValueError:
        return []


def load_cache() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {}


def municipality_names(session, token_file, cache: dict) -> list[str]:
    names = cache.get("municipios")
    if not names:
        names = municipalities(session, token_file)
        cache["municipios"] = names
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(json.dumps(cache))
    return names


def enumerate_pair(
    token_file, cache, names, year, categoria, stop
) -> list[Item]:
    """Resolve one exercise/category to its `seqArquivo` ids, cached on disk.

    854 listing calls, ~70 s. There is no cheaper route: `categoria` is mandatory
    on `buscarDetalhesCategorias` (omitting it is a 400, and an empty or bogus
    value returns an empty body), so the four categories cannot be resolved in one
    pass. Sweeping the `seqArquivo` space instead would need ~111,000 requests to
    cover all ten categories the portal publishes, against ~44,000 here.

    The result is cached per pair, so this cost is paid once across every restart
    -- and, because it is per pair rather than for the whole span, downloading
    starts about 70 seconds after launch instead of an hour.
    """
    entries: dict = cache.setdefault("entries", {})
    key = f"{year}|{categoria}"
    if key not in entries:
        found: dict[str, list] = {}
        unresolved: dict[str, str] = {}
        lock = threading.Lock()
        work = queue.Queue()
        for name in names:
            work.put(name)

        def worker():
            local = session_for(token_file)
            while not stop.is_set():
                try:
                    name = work.get_nowait()
                except queue.Empty:
                    return
                try:
                    files = None
                    error = "retries exhausted"
                    for attempt in range(MAX_ATTEMPTS):
                        try:
                            files = detail(
                                local, token_file, name, categoria, year
                            )
                            break
                        except TokenExpiredError:
                            wait_for_fresh_token(token_file, stop)
                            error = "token kept expiring"
                        except requests.RequestException as exc:
                            error = f"{type(exc).__name__}: {exc}"
                            if attempt < MAX_ATTEMPTS - 1:
                                time.sleep(2**attempt + random.random())
                    if files is None:
                        # A municipality that could not be listed is NOT the same
                        # as one with nothing to list, and the difference is
                        # invisible once the manifest is cached. An earlier
                        # version skipped silently here and lost 164 of 853
                        # municipalities from 2023 contrato -- alphabetically
                        # contiguous, because a token expired mid-pair and every
                        # worker exhausted its retries at once. Only the
                        # portal-count cross-check in verify_mg_harvest.py caught
                        # it. Record it; the caller refuses to cache the pair.
                        with lock:
                            unresolved[name] = error
                        continue
                    if files:
                        with lock:
                            found[name] = [
                                [
                                    int(f["seqArquivo"]),
                                    int(f.get("tamanhoBytes") or 0),
                                ]
                                for f in files
                            ]
                finally:
                    work.task_done()

        threads = [
            threading.Thread(target=worker, daemon=True)
            for _ in range(WORKERS)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if stop.is_set():
            return []
        if unresolved:
            # Caching a partial enumeration is worse than not caching it: every
            # later run trusts the manifest and the gap never reappears.
            sample = list(unresolved)[:5]
            raise RuntimeError(
                f"{year} {categoria}: {len(unresolved)} of {len(names)} "
                f"municipalities could not be listed "
                f"(e.g. {sample}; first error: {unresolved[sample[0]]}). "
                f"NOT caching this pair -- re-run to retry it."
            )
        entries[key] = found
        MANIFEST.write_text(json.dumps(cache))
        total = sum(size for files in found.values() for _, size in files)
        print(
            f"  enumerated {year} {categoria:<10} {len(found):>4} municipalities, "
            f"{total / 1e6:>8.1f} MB",
            flush=True,
        )

    return [
        Item(year, categoria, name, seq, size)
        for name, files in (entries.get(key) or {}).items()
        for seq, size in files
    ]


# --------------------------------------------------------------------------- #
# transfer
# --------------------------------------------------------------------------- #
_NAME_RE = re.compile(r'"nomeArquivo"\s*:\s*"([^"]*)"')
_B64_RE = re.compile(r'"bytesConteudo"\s*:\s*"')


def stream_decode(response, destination: Path) -> tuple[int, str]:
    """Stream the JSON body, base64-decoding `bytesConteudo` straight to disk.

    Buffering the whole response would mean holding the JSON string *and* the
    decoded archive in memory at once -- for Belo Horizonte's despesa, across
    eight workers, that is gigabytes for no reason. Instead the body is consumed
    in chunks: everything up to the opening quote of `bytesConteudo` is header,
    everything after it is base64 decoded 4 characters at a time until the
    closing quote.

    Returns (bytes written, nomeArquivo). Raises ValueError if the field never
    appears or the body ends inside it -- which is exactly what a truncated
    transfer looks like.
    """
    header = b""
    name = ""
    carry = b""
    written = 0
    started = False
    closed = False
    with open(destination, "wb") as handle:
        for chunk in response.iter_content(1 << 20):
            if not chunk:
                continue
            if not started:
                header += chunk
                match = _B64_RE.search(header.decode("utf-8", "replace"))
                if not match:
                    # Guard against a gateway error page: the real header is a
                    # few hundred bytes, so anything large without the field is
                    # not the response we asked for.
                    if len(header) > 1 << 20:
                        raise ValueError(
                            f"no bytesConteudo in first MB: {header[:200]!r}"
                        )
                    continue
                found = _NAME_RE.search(header.decode("utf-8", "replace"))
                name = found.group(1) if found else ""
                chunk = header[match.end() :]
                started = True
            # From here every chunk is base64 until the closing quote.
            quote = chunk.find(b'"')
            if quote != -1:
                chunk, closed = chunk[:quote], True
            data = carry + chunk
            usable = len(data) - (len(data) % 4)
            carry = data[usable:]
            if usable:
                handle.write(base64.b64decode(data[:usable]))
                written += usable
            if closed:
                break
        if carry:
            handle.write(base64.b64decode(carry + b"=" * (-len(carry) % 4)))
    if not started:
        raise ValueError("response carried no bytesConteudo field")
    if not closed:
        raise ValueError(
            "response ended inside bytesConteudo -- truncated transfer"
        )
    return destination.stat().st_size, name


def verify_zip(path: Path) -> int:
    """Member count, read from the central directory. Never decompresses.

    The central directory sits at the end of the archive and cannot be written
    until the whole stream has arrived, so parsing it is the proof of
    completeness. `testzip()` would additionally prove every member decompresses,
    but that is 26 GB of CPU to catch a failure mode -- silent bit corruption
    inside an otherwise well-formed archive -- that TLS already rules out.
    """
    with zipfile.ZipFile(path) as archive:
        return len(archive.namelist())


def destination_for(item: Item, name: str) -> Path:
    stem = name or f"SICOM.{item.year}.{item.municipio}.{item.categoria}.zip"
    return MG_INPUT / str(item.year) / item.categoria / stem


def fetch(session, token_file, item: Item, stop) -> tuple[str, dict]:
    """One municipality-exercise-category file, verified, onto disk.

    Written to a `.part` beside the destination and renamed only after the zip
    verifies, so a file with a final name is always a complete file -- the
    property the bulk route could not offer.
    """
    temporary = (
        MG_INPUT / str(item.year) / item.categoria / f".{item.seq}.part"
    )
    temporary.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if stop.is_set():
            return "stopped", {}
        try:
            with session.get(
                MG_API + f"/baixarArquivo/{item.seq}",
                headers={"AuthorizationProxy": f"token {token_file.get()}"},
                stream=True,
                timeout=TIMEOUT,
            ) as response:
                if response.status_code == 401:
                    raise TokenExpiredError("401")
                response.raise_for_status()
                written, name = stream_decode(response, temporary)
            members = verify_zip(temporary)
            final = destination_for(item, name)
            final.parent.mkdir(parents=True, exist_ok=True)
            temporary.replace(final)
            return "ok", {
                "path": str(final.relative_to(MG_INPUT)),
                "bytes": written,
                "advertised": item.advertised,
                "members": members,
            }
        except TokenExpiredError:
            wait_for_fresh_token(token_file, stop)
        except (
            requests.RequestException,
            ValueError,
            zipfile.BadZipFile,
            binascii.Error,
            OSError,
        ) as exc:
            temporary.unlink(missing_ok=True)
            if attempt == MAX_ATTEMPTS:
                return "failed", {"error": f"{type(exc).__name__}: {exc}"}
            time.sleep(min(2**attempt, 30) + random.random())
    return "failed", {"error": "exhausted attempts"}


# --------------------------------------------------------------------------- #
# ledger
# --------------------------------------------------------------------------- #
def load_ledger() -> dict[str, dict]:
    """Completed units, keyed year|categoria|municipio.

    Append-only JSONL rather than a rewritten index: a harvest that is killed
    mid-write loses at most the last line, and the last line is one municipality
    that gets re-fetched. An entry is trusted only if its file is still on disk at
    the recorded size, so deleting a file is enough to force a re-fetch.
    """
    done: dict[str, dict] = {}
    if not LEDGER.exists():
        return done
    for line in LEDGER.read_text().splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get("status") == "ok":
            path = MG_INPUT / record["path"]
            if not path.exists() or path.stat().st_size != record["bytes"]:
                continue
        done[record["key"]] = record
    return done


def harvest(
    items: list[Item],
    token_file: TokenFile,
    stop: threading.Event,
    done: dict[str, dict] | None = None,
    label: str = "",
) -> dict[str, dict]:
    """Fetch one batch, returning the updated `done` map so a caller can chain.

    `done` is passed in and handed back rather than re-read per call: the ledger
    grows to ~44,000 lines, and re-parsing it before each of 52 batches is both
    wasteful and, worse, would re-stat every file already on disk each time.
    """
    if done is None:
        done = load_ledger()
    todo = [i for i in items if i.key not in done]
    want = sum(i.advertised for i in todo)
    print(
        f"  {label}{len(items):,} units | {len(items) - len(todo):,} already done"
        f" | {len(todo):,} to fetch (~{want / 1e9:.2f} GB)",
        flush=True,
    )
    if not todo:
        return done

    work = queue.Queue()
    for item in todo:
        work.put(item)
    ledger_lock = threading.Lock()
    counters = {"ok": 0, "failed": 0, "empty": 0, "bytes": 0}
    started = time.time()
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    # The ledger handle is appended to by `record()` for the whole run and
    # closed in the finally below; a `with` block cannot span that.
    handle = open(LEDGER, "a")  # noqa: SIM115

    def record(item: Item, status: str, payload: dict):
        with ledger_lock:
            handle.write(
                json.dumps(
                    {
                        "key": item.key,
                        "year": item.year,
                        "categoria": item.categoria,
                        "municipio": item.municipio,
                        "seq": item.seq,
                        "status": status,
                        "at": datetime.now(tz=UTC).isoformat(
                            timespec="seconds"
                        ),
                        **payload,
                    }
                )
                + "\n"
            )
            handle.flush()
            done[item.key] = {"status": status, **payload}
            counters[status] = counters.get(status, 0) + 1
            counters["bytes"] += payload.get("bytes", 0)
            finished = counters["ok"] + counters["failed"]
            if finished % 25 == 0 or status == "failed":
                elapsed = max(time.time() - started, 1)
                rate = counters["bytes"] / elapsed
                remaining = (len(todo) - finished) / max(
                    finished / elapsed, 1e-9
                )
                print(
                    f"  {finished:>6}/{len(todo)}  "
                    f"{counters['bytes'] / 1e9:>6.2f} GB  "
                    f"{rate / 1024:>5.0f} KB/s  "
                    f"fail={counters['failed']}  "
                    f"eta={remaining / 3600:>4.1f} h",
                    flush=True,
                )

    def worker():
        local = session_for(token_file)
        while not stop.is_set():
            try:
                item = work.get_nowait()
            except queue.Empty:
                return
            try:
                status, payload = fetch(local, token_file, item, stop)
                if status != "stopped":
                    record(item, status, payload)
                    if status == "failed":
                        print(
                            f"  FAILED {item.key}: {payload.get('error')}",
                            flush=True,
                        )
            finally:
                work.task_done()

    threads = [
        threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)
    ]
    for thread in threads:
        thread.start()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        print("\ninterrupted -- finishing in-flight transfers", flush=True)
        stop.set()
    for thread in threads:
        thread.join(timeout=60)
    handle.close()
    elapsed = time.time() - started
    print(
        f"  -> fetched {counters['ok']:,} files, {counters['bytes'] / 1e9:.2f} GB "
        f"in {elapsed / 60:.0f} min | failed {counters['failed']}",
        flush=True,
    )
    return done


def main() -> None:
    global CA
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--categoria", action="append", choices=CATEGORIES)
    parser.add_argument(
        "--token-file", default=os.environ.get("MG_TOKEN_FILE")
    )
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument(
        "--plan",
        action="store_true",
        help="enumerate and report, download nothing",
    )
    parser.add_argument("--help-token", action="store_true")
    args = parser.parse_args()

    if args.help_token:
        print(HELP_TOKEN)
        return
    if not args.token_file:
        print(HELP_TOKEN, file=sys.stderr)
        raise SystemExit(2)

    globals()["WORKERS"] = args.workers
    CA = ca_bundle()
    token_file = TokenFile(Path(args.token_file).expanduser())
    left = token_file.minutes_left()
    print(
        f"token valid for {left:.0f} min"
        if left
        else "token expiry unreadable"
    )

    years = (
        sorted(args.year)
        if args.year
        else list(range(FIRST_YEAR, LAST_YEAR + 1))
    )
    categorias = tuple(args.categoria) if args.categoria else CATEGORIES
    print(
        f"exercises {years[0]}-{years[-1]} | categories {', '.join(categorias)}"
    )
    print(f"data root {MG_INPUT}")

    stop = threading.Event()
    session = session_for(token_file)
    cache = load_cache()
    names = municipality_names(session, token_file, cache)
    print(f"municipalities: {len(names)}")

    # Newest exercise first. MiDES already holds MG through 2021, so the recent
    # years are the actual gap -- and if a 20-hour run is cut short, the part that
    # completed should be the part that was missing.
    pairs = [(y, c) for y in sorted(years, reverse=True) for c in categorias]

    if args.plan:
        total_items = 0
        total_bytes = 0
        for year, categoria in pairs:
            items = enumerate_pair(
                token_file, cache, names, year, categoria, stop
            )
            total_items += len(items)
            total_bytes += sum(i.advertised for i in items)
        print(f"\nTOTAL {total_items:,} files  {total_bytes / 1e9:.2f} GB")
        return

    # Interleaved: each pair is enumerated and then immediately fetched, so bytes
    # start landing ~70 s after launch rather than after the whole ~1 h
    # enumeration. Both halves are cached/ledgered, so a restart resumes mid-pair.
    done = load_ledger()
    already = sum(1 for r in done.values() if r.get("status") == "ok")
    print(f"ledger: {already:,} units already complete")

    # Pairs already fetched whole by bulk_mg.py. Their files are on disk but have
    # no per-municipality ledger entries -- the ledger is keyed by municipality
    # NAME, and a bulk archive only ever reveals the IBGE code. Without this skip
    # every bulk-fetched pair would be enumerated and downloaded a second time,
    # which would undo the entire point of running bulk first.
    bulk_done: set[tuple[int, str]] = set()
    bulk_ledger = MG_INPUT / "_bulk_ledger.jsonl"
    if bulk_ledger.exists():
        for line in bulk_ledger.read_text().splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get("status") in ("ok", "absent"):
                bulk_done.add((entry["year"], entry["categoria"]))
    if bulk_done:
        print(f"skipping {len(bulk_done)} pairs already handled by bulk_mg.py")
    print(flush=True)

    for index, (year, categoria) in enumerate(pairs, 1):
        if stop.is_set():
            break
        if (year, categoria) in bulk_done:
            continue
        label = f"[{index}/{len(pairs)}] {year} {categoria}: "
        items = enumerate_pair(token_file, cache, names, year, categoria, stop)
        if stop.is_set():
            break
        done = harvest(items, token_file, stop, done=done, label=label)
    total_ok = sum(1 for r in done.values() if r.get("status") == "ok")
    total_bytes = sum(
        r.get("bytes", 0) for r in done.values() if r.get("status") == "ok"
    )
    print(
        f"\nHARVEST COMPLETE: {total_ok:,} files, {total_bytes / 1e9:.2f} GB",
        flush=True,
    )


if __name__ == "__main__":
    main()
