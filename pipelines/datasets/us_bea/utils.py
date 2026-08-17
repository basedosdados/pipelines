"""BEA API client + download/clean transform for us_bea (shared by the recurring
pipeline and the one-shot bootstrap in ``models/us_bea/code/``).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap
``models/us_bea/code/clean.py`` imports the same row-builders and fetch helpers,
so the two cannot drift apart.

Two schemas are in play and must not be confused:

- ``STAGING_SCHEMAS`` (here) — the RAW STAGING schema this pipeline writes:
  ``year`` INT64, ``value`` FLOAT64, everything else STRING (including
  ``quarter``/``month``), with staging column names ``table_name``/``series_code``.
- the architecture CSVs under ``models/us_bea/code/architecture/`` — the FINAL
  post-dbt schema (``table_id``/``series_id``, ``quarter``/``month`` INT64). The
  dbt models rename and recast staging into that shape.

The bootstrap uploads STAGING_SCHEMAS as TYPED parquet (the one-shot onboarding
path accepts it). The recurring pipeline instead writes ALL-STRING parquet: the
``dump_header`` parquet bug makes ``upload_to_gcs`` infer every staging column as
STRING, so typed parquet is rejected on read. Values pass through the typed
staging schema FIRST (so ``year`` serializes as ``"1959"`` not ``"1959.0"``) and
are only then cast to string via arrow — never ``astype(str)``, which would turn
NULL into the literal ``"nan"`` and defeat the dbt ``safe_cast``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipelines.datasets.us_bea.constants import constants

log = logging.getLogger("us_bea")

BASE = constants.BASE_URL.value
MISSING_TOKENS = set(constants.MISSING_TOKENS.value)


# --------------------------------------------------------------------------- #
# API key: env var locally, Vault on the deployed worker
# --------------------------------------------------------------------------- #
def _key() -> str:
    """Return the BEA API key: ``BEA_API_KEY`` env var if set, else Vault.

    Locally the key is provided via the environment. On the deployed Prefect
    worker there is no such env var, so it is read from HashiCorp Vault at
    ``constants.VAULT_SECRET_PATH`` under ``constants.VAULT_KEY``.

    Raises:
        RuntimeError: If the key is found in neither the environment nor Vault.
    """
    k = os.environ.get(constants.ENV_KEY.value, "").strip()
    if k:
        return k
    # Deployed worker: read from Vault. Imported lazily so local use (and unit
    # tests) never require hvac / Vault connectivity.
    from pipelines.utils.vault import get_credentials_from_secret

    tokens = get_credentials_from_secret(constants.VAULT_SECRET_PATH.value)
    k = str(tokens.get(constants.VAULT_KEY.value, "")).strip()
    if not k:
        raise RuntimeError(
            "BEA_API_KEY not set in environment and not found in Vault "
            f"at {constants.VAULT_SECRET_PATH.value!r}"
        )
    return k


_last_call = [0.0]


def _throttle() -> None:
    interval = constants.MIN_INTERVAL.value
    dt = time.monotonic() - _last_call[0]
    if dt < interval:
        time.sleep(interval - dt)
    _last_call[0] = time.monotonic()


class BEAError(RuntimeError):
    def __init__(self, code, desc, dataset, params):
        self.code, self.desc = code, desc
        super().__init__(
            f"BEA APIErrorCode {code}: {desc} [{dataset} {params}]"
        )


def call(
    method: str, dataset: str | None = None, *, max_retries: int = 6, **params
) -> dict:
    """One BEA API call. Handles 429/Retry-After and transient errors.

    Args:
        method: BEA API method (``GetData``, ``GetParameterValues`` …).
        dataset: BEA dataset name, or None for methods that take no dataset.
        max_retries: Attempts before giving up on transient failures.
        **params: Method parameters; None values are dropped.

    Returns:
        The ``Results`` node of the response.

    Raises:
        BEAError: On a non-throttle API error.
        RuntimeError: If all retries are exhausted.
    """
    _throttle()
    q = {"UserID": _key(), "method": method, "ResultFormat": "JSON"}
    if dataset:
        q["datasetname"] = dataset
    q.update({k: v for k, v in params.items() if v is not None})
    url = BASE + "?" + urllib.parse.urlencode(q)
    delay = 2.0
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                body = json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = int(e.headers.get("Retry-After", "60")) + 1
                time.sleep(wait)
                continue
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise
        res = body.get("BEAAPI", {}).get("Results", {})
        if isinstance(res, list):
            res = res[0] if res else {}
        err = res.get("Error") if isinstance(res, dict) else None
        if err:
            code = str(err.get("APIErrorCode", ""))
            desc = err.get("APIErrorDescription", "") or err.get(
                "ErrorDetail", {}
            ).get("Description", "")
            if code == "7":  # throttled
                time.sleep(61)
                continue
            raise BEAError(code, desc, dataset, params)
        return res
    raise RuntimeError(
        f"BEA call failed after retries: {method} {dataset} {params}"
    )


def _norm_pv(node: dict, param: str) -> dict:
    """ParamValue nodes are keyed differently per dataset. Normalize to
    ``{'key','desc'}``."""
    key = node.get("Key") if "Key" in node else None
    if key is None:
        key = (
            node.get(param)
            or node.get("TableName")
            or node.get("TableID")
            or node.get("LineCode")
            or node.get("Industry")
            or node.get("FrequencyID")
        )
    desc = node.get("Desc") or node.get("Description") or ""
    return {"key": str(key) if key is not None else None, "desc": desc}


def param_values(dataset: str, param: str) -> list[dict]:
    res = call("GetParameterValues", dataset, ParameterName=param)
    v = res.get("ParamValue", res)
    v = v if isinstance(v, list) else [v]
    return [_norm_pv(n, param) for n in v]


def param_values_filtered(dataset: str, target: str, **filters) -> list[dict]:
    res = call(
        "GetParameterValuesFiltered",
        dataset,
        TargetParameter=target,
        **filters,
    )
    v = res.get("ParamValue", res)
    v = v if isinstance(v, list) else [v]
    return [_norm_pv(n, target) for n in v]


def get_data(dataset: str, **params) -> list[dict]:
    """GetData -> list of data rows (possibly empty)."""
    res = call("GetData", dataset, **params)
    data = res.get("Data", [])
    if isinstance(data, dict):
        data = [data]
    return data


# --------------------------------------------------------------------------- #
# pure cleaning helpers
# --------------------------------------------------------------------------- #
def clean_value(raw) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if s in MISSING_TOKENS:
        return None
    s = s.replace(",", "")
    if (
        s.startswith("(")
        and s.endswith(")")
        and s[1:-1].replace(".", "").isdigit()
    ):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def split_time_period(
    tp: str | None,
) -> tuple[int | None, str | None, str | None]:
    """TimePeriod -> (year, quarter, month). YYYY / YYYYQn / YYYYMnn."""
    if tp is None:
        return None, None, None
    tp = tp.strip()
    try:
        if "Q" in tp:
            y, q = tp.split("Q")
            return int(y), q, None
        if "M" in tp:
            y, m = tp.split("M")
            return int(y), None, m.zfill(2)
        return int(tp[:4]), None, None
    except (ValueError, IndexError):
        return None, None, None


def line_from_code(code: str) -> str | None:
    """Regional 'Code' like 'SAGDP2N-1' -> line_code '1'."""
    if not code:
        return None
    return code.rsplit("-", 1)[-1] if "-" in code else None


def norm_gi_quarter(frequency: str, quarter) -> str | None:
    """GDPbyIndustry: annual rows have Quarter==Year; quarterly rows are I..IV."""
    if frequency == "A":
        return None
    roman = {"I": "1", "II": "2", "III": "3", "IV": "4"}
    return roman.get(str(quarter).strip(), str(quarter).strip() or None)


def _freq(year, quarter, month) -> str:
    return "M" if month else ("Q" if quarter else "A")


_REGION_PREFIX = {"91", "92", "93", "94", "95", "96", "97", "98"}


def _id_state(geo_fips: str) -> str | None:
    """NN000 -> NN for real states/territories; None for US(00000) and BEA regions."""
    if not geo_fips or len(geo_fips) != 5 or not geo_fips.endswith("000"):
        return None
    st = geo_fips[:2]
    if st == "00" or st in _REGION_PREFIX:
        return None
    return st


def _strip_tag(desc: str) -> str:
    # "[SAGDP2] Gross domestic product" -> "Gross domestic product"
    return re.sub(r"^\[[^\]]*\]\s*", "", desc or "").strip()


def line_desc_map(dataset: str, table: str) -> dict:
    out = {}
    for x in param_values_filtered(dataset, "LineCode", TableName=table):
        if x["key"] is not None:
            out[x["key"]] = _strip_tag(x["desc"])
    return out


# --------------------------------------------------------------- row builders --
def rows_nipa(api_rows):
    for r in api_rows:
        y, q, m = split_time_period(r.get("TimePeriod"))
        if y is None:
            continue
        yield {
            "year": y,
            "quarter": q,
            "month": m,
            "frequency": _freq(y, q, m),
            "table_name": r.get("TableName"),
            "line_number": r.get("LineNumber"),
            "series_code": r.get("SeriesCode"),
            "line_description": r.get("LineDescription"),
            "metric_name": r.get("METRIC_NAME"),
            "unit": r.get("CL_UNIT"),
            "unit_mult": r.get("UNIT_MULT"),
            "value": clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }


def rows_gi(api_rows, table_desc):
    for r in api_rows:
        try:
            y = int(r.get("Year"))
        except (TypeError, ValueError):
            continue
        freq = r.get("Frequency")
        yield {
            "year": y,
            "quarter": norm_gi_quarter(freq, r.get("Quarter")),
            "frequency": freq,
            "table_id": str(r.get("TableID")),
            "table_description": table_desc,
            "industry": r.get("Industry"),
            "industry_description": r.get("IndustrYDescription"),
            "value": clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }


def rows_regional(api_rows, table, line_code, line_desc, level):
    for r in api_rows:
        y, q, _ = split_time_period(r.get("TimePeriod"))
        if y is None:
            continue
        gf = r.get("GeoFips")
        base = {
            "year": y,
            "geo_fips": gf,
            "geo_name": r.get("GeoName"),
            "table_name": table,
            "line_code": line_code,
            "series_code": r.get("Code"),
            "line_description": line_desc,
            "unit": r.get("CL_UNIT"),
            "unit_mult": r.get("UNIT_MULT"),
            "value": clean_value(r.get("DataValue")),
            "note_ref": r.get("NoteRef"),
        }
        if level == "state":
            base.update(
                quarter=q, frequency=_freq(y, q, None), id_state=_id_state(gf)
            )
        elif level == "county":
            base.update(
                id_county=gf,
                id_state=(gf[:2] if gf and len(gf) == 5 else None),
            )
        elif level == "metro":
            base.update(id_cbsa=gf)
        yield base


# ----------------------------------------------------- table listing + fetch --
def nipa_table_names() -> list[str]:
    return [x["key"] for x in param_values("NIPA", "TableName")]


def gi_tables() -> list[dict]:
    """GDPbyIndustry TableIDs as ``[{'key','desc'}, …]``."""
    return param_values("GDPbyIndustry", "TableID")


def regional_table_names(prefixes) -> list[str]:
    tabs = [x["key"] for x in param_values("Regional", "TableName")]
    return [t for t in tabs if any(t.startswith(p) for p in prefixes)]


def fetch_nipa_table(t: str) -> list[dict]:
    """All rows for one NIPA TableName (A/Q/M, full history). [] on API error."""
    try:
        api = get_data("NIPA", TableName=t, Frequency="A,Q,M", Year="ALL")
    except BEAError as e:
        log.info("[nipa] %s skipped: %s", t, e.desc)
        return []
    return list(rows_nipa(api))


def fetch_gi_table(tid: str, desc: str) -> list[dict]:
    """All rows for one GDPbyIndustry TableID (annual then quarterly)."""
    out: list[dict] = []
    for freq in ("A", "Q"):
        try:
            api = get_data(
                "GDPbyIndustry",
                TableID=tid,
                Industry="ALL",
                Frequency=freq,
                Year="ALL",
            )
        except BEAError as e:
            log.info("[gi] T%s %s skipped: %s", tid, freq, e.desc)
            continue
        out.extend(rows_gi(api, desc))
    return out


def fetch_regional_table(t: str, geofips: str, level: str):
    """Yield row batches (one per LineCode) for one regional BEA TableName.

    Yielded per line code so the caller can flush between codes and keep peak
    RAM bounded — the county family alone is ~50M rows.
    """
    ldesc = line_desc_map("Regional", t)
    for lc in ldesc:
        try:
            api = get_data(
                "Regional",
                TableName=t,
                LineCode=lc,
                GeoFips=geofips,
                Year="ALL",
            )
        except BEAError as e:
            if e.code in ("204", "100"):  # no data / invalid line
                continue
            log.info("[%s] %s line %s: %s", level, t, lc, e.desc)
            continue
        yield list(rows_regional(api, t, lc, ldesc.get(lc, ""), level))


# ----------------------------------------------------------- dicionario rows --
_FREQ_LABELS = {"A": "Annual", "Q": "Quarterly", "M": "Monthly"}


def build_dicionario_rows() -> list[dict]:
    """Code->label maps for the dictionary-covered columns across all tables."""
    rows: list[dict] = []

    def add(id_tabela, nome_coluna, chave, valor):
        rows.append(
            {
                "id_tabela": id_tabela,
                "nome_coluna": nome_coluna,
                "chave": str(chave),
                "cobertura_temporal": "",
                "valor": valor,
            }
        )

    for x in param_values("NIPA", "TableName"):
        add("nipa", "table_id", x["key"], x["desc"])
    reg_tabs = {
        x["key"]: x["desc"] for x in param_values("Regional", "TableName")
    }
    for dbt_tbl, fam in constants.REGIONAL_FAMILIES.value.items():
        prefs = fam["prefixes"]
        for code, desc in reg_tabs.items():
            if any(code.startswith(p) for p in prefs):
                add(dbt_tbl, "table_id", code, desc)
    for x in param_values("GDPbyIndustry", "TableID"):
        add("gdp_by_industry", "table_id", x["key"], x["desc"])
    for x in param_values("GDPbyIndustry", "Industry"):
        add("gdp_by_industry", "industry", x["key"], x["desc"])
    for dbt_tbl, freqs in {
        "nipa": "AQM",
        "gdp_by_industry": "AQ",
        "regional_state": "AQ",
    }.items():
        for f in freqs:
            add(dbt_tbl, "frequency", f, _FREQ_LABELS[f])
    return rows


# --------------------------------------------------------------------------- #
# staging schemas (TYPED). The bootstrap writes these as typed parquet; the
# pipeline casts them to all-string (see module docstring).
# --------------------------------------------------------------------------- #
_STR = pa.string()
_INT = pa.int64()
_FLT = pa.float64()


def _schema(cols) -> pa.Schema:
    return pa.schema([pa.field(n, t) for n, t in cols])


STAGING_SCHEMAS = {
    "nipa": _schema(
        [
            ("year", _INT),
            ("quarter", _STR),
            ("month", _STR),
            ("frequency", _STR),
            ("table_name", _STR),
            ("line_number", _STR),
            ("series_code", _STR),
            ("line_description", _STR),
            ("metric_name", _STR),
            ("unit", _STR),
            ("unit_mult", _STR),
            ("value", _FLT),
            ("note_ref", _STR),
        ]
    ),
    "gdp_by_industry": _schema(
        [
            ("year", _INT),
            ("quarter", _STR),
            ("frequency", _STR),
            ("table_id", _STR),
            ("table_description", _STR),
            ("industry", _STR),
            ("industry_description", _STR),
            ("value", _FLT),
            ("note_ref", _STR),
        ]
    ),
    "regional_state": _schema(
        [
            ("year", _INT),
            ("quarter", _STR),
            ("frequency", _STR),
            ("geo_fips", _STR),
            ("id_state", _STR),
            ("geo_name", _STR),
            ("table_name", _STR),
            ("line_code", _STR),
            ("series_code", _STR),
            ("line_description", _STR),
            ("unit", _STR),
            ("unit_mult", _STR),
            ("value", _FLT),
            ("note_ref", _STR),
        ]
    ),
    "regional_county": _schema(
        [
            ("year", _INT),
            ("geo_fips", _STR),
            ("id_county", _STR),
            ("id_state", _STR),
            ("geo_name", _STR),
            ("table_name", _STR),
            ("line_code", _STR),
            ("series_code", _STR),
            ("line_description", _STR),
            ("unit", _STR),
            ("unit_mult", _STR),
            ("value", _FLT),
            ("note_ref", _STR),
        ]
    ),
    "regional_metro": _schema(
        [
            ("year", _INT),
            ("geo_fips", _STR),
            ("id_cbsa", _STR),
            ("geo_name", _STR),
            ("table_name", _STR),
            ("line_code", _STR),
            ("series_code", _STR),
            ("line_description", _STR),
            ("unit", _STR),
            ("unit_mult", _STR),
            ("value", _FLT),
            ("note_ref", _STR),
        ]
    ),
    "dicionario": _schema(
        [
            ("id_tabela", _STR),
            ("nome_coluna", _STR),
            ("chave", _STR),
            ("cobertura_temporal", _STR),
            ("valor", _STR),
        ]
    ),
}

# All-STRING variants (staging schema cast to string, column order preserved).
STRING_SCHEMAS = {
    t: pa.schema([pa.field(f.name, _STR) for f in s])
    for t, s in STAGING_SCHEMAS.items()
}


# --------------------------------------------------------------------------- #
# all-string partitioned writer (pipeline)
# --------------------------------------------------------------------------- #
class _StringWriter:
    """Buffers rows for one table and flushes all-STRING Snappy Parquet,
    hive-partitioned by ``year`` (kept in the file too, matching the reference
    ``us_bls_cpi`` writer). Flushing in chunks bounds peak RAM."""

    def __init__(self, table: str, output_dir: Path):
        self.table = table
        self.dir = output_dir / table
        self.typed = STAGING_SCHEMAS[table]
        self.string = STRING_SCHEMAS[table]
        self.buf: list[dict] = []
        self.total = 0
        self.seq = 0

    def add(self, rows) -> None:
        self.buf.extend(rows)
        if len(self.buf) >= constants.FLUSH_ROWS.value:
            self.flush()

    def flush(self) -> None:
        if not self.buf:
            return
        # Types-first: build with the typed staging schema so year is an int
        # (serializes "1959", not "1959.0") and value a float, THEN cast to
        # all-string via arrow (NULLs preserved, never "nan").
        typed = pa.Table.from_pylist(self.buf, schema=self.typed)
        at = typed.cast(self.string)
        for y in pc.unique(at.column("year")).to_pylist():
            sub = at.filter(pc.equal(at.column("year"), y))
            pdir = self.dir / f"year={y}"
            pdir.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                sub, pdir / f"data-{self.seq}.parquet", compression="snappy"
            )
            self.seq += 1
        self.total += len(self.buf)
        self.buf = []


def _write_dicionario(rows: list[dict], output_dir: Path) -> Path:
    tdir = output_dir / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pylist(rows, schema=STAGING_SCHEMAS["dicionario"])
    at = at.cast(STRING_SCHEMAS["dicionario"])
    pq.write_table(at, tdir / "data.parquet", compression="snappy")
    log.info("dicionario: %s rows -> %s", f"{len(rows):,}", tdir)
    return tdir


def clean_all(output_dir: Path) -> dict:
    """Download all six tables from the BEA API and write all-STRING parquet.

    The single entry point the recurring pipeline uses (via
    :func:`pipelines.datasets.us_bea.tasks.clean_bea`). Streams and flushes in
    chunks to bound RAM (county alone is ~50M rows).

    Args:
        output_dir: Root output directory; each table lands under
            ``<output_dir>/<table>/year=<YYYY>/data-<n>.parquet``.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year_month"`` —
        the latest ``"YYYY-MM"`` among monthly (frequency M) NIPA rows, which
        drives the source-update poll. None if no monthly rows are present.
    """
    output_dir = Path(output_dir)
    result: dict = {}
    max_ym: tuple[int, int] | None = None

    # nipa
    w = _StringWriter("nipa", output_dir)
    for t in nipa_table_names():
        rows = fetch_nipa_table(t)
        for r in rows:
            if r["month"] is not None:
                ym = (r["year"], int(r["month"]))
                if max_ym is None or ym > max_ym:
                    max_ym = ym
        w.add(rows)
    w.flush()
    log.info("nipa: %s rows -> %s", f"{w.total:,}", w.dir)
    result["nipa"] = str(w.dir)

    # gdp_by_industry
    w = _StringWriter("gdp_by_industry", output_dir)
    for x in gi_tables():
        w.add(fetch_gi_table(x["key"], (x["desc"] or "").strip()))
    w.flush()
    log.info("gdp_by_industry: %s rows -> %s", f"{w.total:,}", w.dir)
    result["gdp_by_industry"] = str(w.dir)

    # regional_state / regional_county / regional_metro
    for table, fam in constants.REGIONAL_FAMILIES.value.items():
        w = _StringWriter(table, output_dir)
        for t in regional_table_names(fam["prefixes"]):
            for batch in fetch_regional_table(t, fam["geofips"], fam["level"]):
                w.add(batch)
        w.flush()
        log.info("%s: %s rows -> %s", table, f"{w.total:,}", w.dir)
        result[table] = str(w.dir)

    # dicionario
    result["dicionario"] = str(
        _write_dicionario(build_dicionario_rows(), output_dir)
    )

    result["max_year_month"] = (
        f"{max_ym[0]}-{max_ym[1]:02d}" if max_ym else None
    )
    return result
