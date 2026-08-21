"""Download + cleaning transform for mx_sesnsp_incidencia_delictiva (shared by the
recurring pipeline and the one-shot bootstrap in
models/mx_sesnsp_incidencia_delictiva/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI
(``code/clean_data.py``) imports ``MONTHS``, ``melt_wide`` and ``write_partition``
directly so the wide→long melt lives in exactly one place. Schema/column order
come from the architecture CSVs (the single source of truth).

Download detail: the SESNSP landing page is behind an Imperva bot challenge and
is JS-rendered, and the files are anonymous SharePoint share links whose tokens
rotate monthly. The landing page is scraped with a real headless Chrome
(``selenium`` + the in-image ``google-chrome-stable``; the driver comes from
``webdriver_manager``) — the Prefect worker runs a pre-built image that does not
gain new pyproject deps per PR, so a pure-Python HTTP client such as ``curl_cffi``
cannot be added there, and a real browser passes Imperva natively rather than
merely spoofing a TLS fingerprint. The SharePoint host is plain Microsoft, so the
zips download with ``requests`` + a browser User-Agent. The current token for each
table is resolved by matching each anchor's visible label — never hardcoded.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.constants import (
    constants,
)

log = logging.getLogger("mx_sesnsp_incidencia_delictiva")

_ARCH = constants.ARCHITECTURE_DIR.value
ONGOING_TABLES = constants.ONGOING_TABLES.value

MONTHS = {
    "Enero": 1,
    "Febrero": 2,
    "Marzo": 3,
    "Abril": 4,
    "Mayo": 5,
    "Junio": 6,
    "Julio": 7,
    "Agosto": 8,
    "Septiembre": 9,
    "Octubre": 10,
    "Noviembre": 11,
    "Diciembre": 12,
}

# SharePoint personal share links embed the token as the path segment after
# `.../personal/cni_sspc_gob_mx/`.
_TOKEN_RE = re.compile(r"/personal/cni_sspc_gob_mx/([^?/]+)")

# Canonical spelling of every source header the melt reads. Matching is done on
# the normalized key (see `_canonical_key`), so a case change, added whitespace
# or dropped punctuation in a future SESNSP release still resolves.
_SOURCE_COLUMNS = (
    "Año",
    "Clave_Ent",
    "Entidad",
    "Cve. Municipio",
    "Municipio",
    "Bien jurídico afectado",
    "Tipo de delito",
    "Subtipo de delito",
    "Modalidad",
    "Sexo",
    "Rango de edad",
    *MONTHS,
)

# Extra normalized keys that map onto a canonical header. Accent-stripping alone
# cannot reach these: Spanish exports routinely spell "Año" as "anio" to dodge
# the ñ, which normalizes to a different key.
_HEADER_ALIASES = {
    "anio": "Año",
    "annio": "Año",
    "cvemun": "Cve. Municipio",
    "clavemunicipio": "Cve. Municipio",
    "claveent": "Clave_Ent",
    "cveent": "Clave_Ent",
    "rangoedad": "Rango de edad",
}

# utf-8 first: it raises on latin-1 accented bytes, so a genuine latin-1 file
# falls through. The reverse order would silently mojibake a utf-8 file, since
# latin-1 decodes every byte sequence.
_ENCODINGS = ("utf-8-sig", "latin-1")


def _canonical_key(name: object) -> str:
    """Reduce a header cell to an accent-, case- and punctuation-free key.

    ``"Año"``, ``"AÑO"`` and ``"anio "`` all collapse to ``"ano"``; ``"Cve.
    Municipio"`` collapses to ``"cvemunicipio"``.

    Args:
        name: A raw header cell. Typed `object` because a pandas column label is
            not guaranteed to be a str — an all-numeric header row reads back as
            ints — so the `str()` below is load-bearing, not redundant.

    Returns:
        The normalized matching key.
    """
    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Rename raw headers to the canonical spelling the melt expects.

    SESNSP re-exports these files monthly and the header spelling is not stable
    across releases. Renaming through `_canonical_key` absorbs case changes,
    stray whitespace and dropped punctuation without touching the melt.

    Note this does *not* repair mojibake — a utf-8 file decoded as latin-1 turns
    ``"Año"`` into ``"AÃ±o"``, whose key is ``"aao"``. That case is handled
    upstream by `_read_source_csv` trying utf-8 first.

    Args:
        df: The wide frame as read from the raw CSV.

    Returns:
        The same frame with recognized headers renamed in place.
    """
    lookup = {_canonical_key(c): c for c in _SOURCE_COLUMNS}
    lookup.update(_HEADER_ALIASES)
    rename = {}
    for actual in df.columns:
        canonical = lookup.get(_canonical_key(actual))
        if canonical is not None and canonical != actual:
            rename[actual] = canonical
    if rename:
        log.info("normalized %s header(s): %s", len(rename), rename)
    return df.rename(columns=rename)


def _read_source_csv(csv_path: Path) -> pd.DataFrame:
    """Read one wide SESNSP CSV, tolerating an encoding or header-spelling change.

    The 2026-08 release broke the pipeline with ``KeyError: 'Año'`` — the
    signature of an accented header that no longer resolves. Rather than pin a
    second guess, try each candidate encoding and accept the first whose header
    yields a usable year column after normalization.

    Args:
        csv_path: The raw wide CSV.

    Returns:
        The frame with canonical headers.

    Raises:
        ValueError: If no candidate encoding produces a year column. The message
            carries the decoded header so the next source change is one log line
            to diagnose, not a pandas traceback.
    """
    attempts: list[tuple[str, list[str]]] = []
    for encoding in _ENCODINGS:
        try:
            df = pd.read_csv(csv_path, encoding=encoding, dtype=str)
        except UnicodeDecodeError:
            continue
        df = normalize_headers(df)
        if "Año" in df.columns:
            log.info("%s: decoded as %s", csv_path.name, encoding)
            return df
        attempts.append((encoding, list(df.columns)[:12]))

    detail = "; ".join(f"{enc} -> {cols}" for enc, cols in attempts)
    raise ValueError(
        f"{csv_path.name}: no year column after trying {list(_ENCODINGS)}. "
        f"The SESNSP header changed. Decoded headers: {detail}"
    )


# ── download ────────────────────────────────────────────────────────────────
def _norm(text: str) -> str:
    """Lowercase and strip accents for robust label matching."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower()


def _classify_label(text: str) -> str | None:
    """Map a landing-page anchor label to one of the four ongoing table slugs.

    Returns None for anything this pipeline does not refresh: the ``Fuero
    federal`` series, the ``Tablero dinámico`` viewer, the legacy 2015-2025
    methodology (label carries ``2015``), and any non ``Fuero común`` link.

    Args:
        text: The anchor's visible label text.

    Returns:
        The table slug (e.g. ``"municipio_delitos"``) or None.
    """
    t = _norm(text)
    if "fuero comun" not in t:
        return None
    if "fuero federal" in t or "tablero" in t:
        return None
    # legacy methodology labels read "2015 - 2025"; the ongoing one starts with a
    # month range ending in the current year.
    if "2015" in t:
        return None
    grain = (
        "municipio"
        if "municipal" in t
        else ("estatal" if "estatal" in t else None)
    )
    measure = (
        "delitos"
        if "delitos" in t
        else ("victimas" if "victimas" in t else None)
    )
    if grain is None or measure is None:
        return None
    return f"{grain}_{measure}"


def resolve_tokens(html: str) -> dict[str, str]:
    """Parse the landing-page HTML into a ``{table_slug: share_token}`` map.

    Args:
        html: The full landing-page HTML (rendered by headless Chrome).

    Returns:
        A mapping of each ongoing table slug to its current SharePoint share
        token.

    Raises:
        ValueError: If any of the four ongoing tables cannot be resolved (the
            page layout or labels changed).
    """
    soup = BeautifulSoup(html, "html.parser")
    tokens: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = _TOKEN_RE.search(href)
        if not m:
            continue
        slug = _classify_label(a.get_text(" ", strip=True))
        if slug is None or slug in tokens:
            continue
        tokens[slug] = m.group(1)
    missing = [t for t in ONGOING_TABLES if t not in tokens]
    if missing:
        raise ValueError(
            f"could not resolve SharePoint tokens for {missing} on "
            f"{constants.GOB_MX_URL.value} (resolved: {sorted(tokens)})"
        )
    return {t: tokens[t] for t in ONGOING_TABLES}


def scrape_tokens() -> dict[str, str]:
    """Fetch the SESNSP landing page and resolve the four current share tokens.

    Drives a real headless Chrome (the in-image ``google-chrome-stable``, with the
    driver from ``webdriver_manager``): loading the page executes the Imperva
    challenge and renders the SharePoint anchors into the DOM. The wait resolves
    once at least one SharePoint personal link is present, which is the signal that
    the challenge passed and the JS finished.

    Returns:
        A mapping of each ongoing table slug to its current SharePoint share
        token.
    """
    options = Options()
    for arg in (
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
    ):
        options.add_argument(arg)
    options.add_argument(f"user-agent={constants.USER_AGENT.value}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(constants.GOB_MX_URL.value)
        # SharePoint anchors present => Imperva passed and the JS rendered.
        WebDriverWait(driver, 60).until(
            ec.presence_of_element_located(
                (By.CSS_SELECTOR, "a[href*='cni_sspc_gob_mx']")
            )
        )
        html = driver.page_source
    finally:
        driver.quit()
    return resolve_tokens(html)


def download_table(token: str, table: str, input_dir: Path) -> Path:
    """Download and extract one table's SharePoint zip into ``input_dir/<table>``.

    The 2026 files ship a single ``RNID-*.csv`` inside a zip. Some CSV names carry
    accents that break macOS ``unzip`` ("Illegal byte sequence"), so extraction
    uses Python ``zipfile`` on the in-memory bytes.

    Args:
        token: The SharePoint share token for this table.
        table: The table slug (used for the destination subdirectory).
        input_dir: Root download directory.

    Returns:
        The path to the extracted CSV.

    Raises:
        ValueError: If the archive contains no CSV.
    """
    url = constants.SHAREPOINT_DOWNLOAD.value.format(token=token)
    # SharePoint (Microsoft) is not behind Imperva — a plain requests GET with a
    # browser User-Agent returns the zip, as it did during onboarding.
    r = requests.get(
        url, headers={"User-Agent": constants.USER_AGENT.value}, timeout=600
    )
    r.raise_for_status()
    dest = input_dir / table
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"{table}: no CSV in archive from {url}")
        # single RNID-*.csv per 2026 archive; take the first if several.
        name = sorted(csv_names)[0]
        out = dest / Path(name).name
        out.write_bytes(zf.read(name))
    log.info("%s <- %s", table, out.name)
    return out


def download_all(input_dir: Path) -> Path:
    """Scrape the current tokens and download all four ongoing tables.

    Args:
        input_dir: Root download directory; each table lands in
            ``input_dir/<table>/<RNID>.csv``.

    Returns:
        The same ``input_dir``, for chaining.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    tokens = scrape_tokens()
    for table, token in tokens.items():
        download_table(token, table, input_dir)
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def arch_order(table: str) -> list[str]:
    """Read a table's column order from its architecture CSV (source of truth).

    Args:
        table: Table slug matching the CSV filename.

    Returns:
        Column names in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return [r["name"] for r in csv.DictReader(fh)]


# ── transform (shared with the validated bootstrap) ─────────────────────────
def melt_wide(df: pd.DataFrame, muni: bool, victimas: bool) -> pd.DataFrame:
    """Melt one wide chunk to the long architecture columns.

    Melts the 12 Spanish month columns (Enero..Diciembre) into ``mes`` (1..12) +
    ``cantidad``. Keeps explicit ``0`` counts; drops months that are blank (not
    yet published). Drops the geography *name* columns (Entidad, Municipio) —
    they live in br_bd_diretorios_mx.

    Args:
        df: A wide chunk with canonical headers (all-string).
        muni: True for municipal tables (keeps ``id_municipio``).
        victimas: True for víctimas tables (keeps ``sexo``/``rango_edad``).

    Returns:
        The long frame with the architecture columns present.
    """
    id_map = {
        "Clave_Ent": "id_entidad",
        "Bien jurídico afectado": "bien_juridico_afectado",
        "Tipo de delito": "tipo_delito",
        "Subtipo de delito": "subtipo_delito",
        "Modalidad": "modalidad",
    }
    if muni:
        id_map["Cve. Municipio"] = "id_municipio"
    if victimas:
        id_map["Sexo"] = "sexo"
        id_map["Rango de edad"] = "rango_edad"
    id_cols = list(id_map)
    month_cols = [m for m in MONTHS if m in df.columns]
    long = df.melt(
        id_vars=[*id_cols, "Año"],
        value_vars=month_cols,
        var_name="_mes",
        value_name="cantidad",
    )
    # drop not-yet-published months (blank), keep explicit 0
    long["cantidad"] = long["cantidad"].astype(str).str.strip()
    long = long[long["cantidad"] != ""].copy()
    long = long[long["cantidad"].str.lower() != "nan"].copy()
    long = long.rename(columns=id_map)
    long["ano"] = pd.to_numeric(long["Año"], errors="coerce").astype("Int64")
    long["mes"] = long["_mes"].map(MONTHS).astype("Int64")
    long["id_entidad"] = (
        long["id_entidad"].astype(str).str.strip().str.zfill(2)
    )
    if muni:
        long["id_municipio"] = (
            long["id_municipio"].astype(str).str.strip().str.zfill(5)
        )
    long["cantidad"] = pd.to_numeric(long["cantidad"], errors="coerce").astype(
        "Int64"
    )
    return long


def write_partition(
    long: pd.DataFrame, table: str, year: int, output_dir: Path
) -> int:
    """Write one year's rows as all-STRING Snappy Parquet, hive-partitioned by ano.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column, and ``pipelines.utils.gcs.dump_header`` stringifies the header
    file BigQuery infers the staging schema from, so typed parquet would be
    rejected. The cast maps each value to ``str`` while preserving NULL as None
    (never ``astype(str)``, which would render NULL as the literal ``"nan"`` and
    defeat the dbt ``safe_cast``).

    Args:
        long: The long frame for one year, from :func:`melt_wide`.
        table: Table slug (drives the architecture lookup and output path).
        year: Partition year.
        output_dir: Root output directory.

    Returns:
        The number of rows written.

    Raises:
        ValueError: If a required architecture column is absent from ``long``.
    """
    order = arch_order(table)
    missing = [c for c in order if c not in long.columns]
    if missing:
        raise ValueError(f"{table}: missing {missing}")
    out = long[order].copy()
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    for c in order:
        s = out[c]
        # pyrefly: ignore [bad-argument-type]
        out[c] = s.astype("object").where(s.notna(), None)
        out[c] = out[c].map(lambda v: None if v is None else str(v))
    at = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    pdir = output_dir / table / f"ano={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(at, pdir / "data.parquet", compression="snappy")
    return at.num_rows


def find_csv(input_dir: Path, table: str) -> Path:
    """Locate the extracted CSV for a table under ``input_dir/<table>``.

    Args:
        input_dir: Root download directory.
        table: Table slug.

    Returns:
        The path to the (single) CSV.

    Raises:
        FileNotFoundError: If no CSV is present.
    """
    hits = sorted((input_dir / table).glob("*.csv"))
    if not hits:
        raise FileNotFoundError(f"no CSV under {input_dir}/{table}")
    return hits[0]


def clean_table(
    csv_path: Path, table: str, output_dir: Path
) -> tuple[int, tuple[int, int] | None]:
    """Clean one table's wide CSV into partitioned parquet.

    Args:
        csv_path: The raw wide CSV (encoding resolved by `_read_source_csv`).
        table: Table slug.
        output_dir: Root output directory.

    Returns:
        ``(total_rows, max_year_month)`` where ``max_year_month`` is the latest
        ``(year, month)`` present, or None if the table is empty.
    """
    muni, victimas = ONGOING_TABLES[table]
    log.info("%s <- %s", table, csv_path.name)
    df = _read_source_csv(csv_path)
    total = 0
    max_ym: tuple[int, int] | None = None
    years = sorted(
        pd.to_numeric(df["Año"], errors="coerce").dropna().astype(int).unique()
    )
    for y in years:
        chunk = df[pd.to_numeric(df["Año"], errors="coerce") == y]
        long = melt_wide(chunk, muni, victimas)
        total += write_partition(long, table, y, output_dir)
        if len(long):
            mmax = int(long["mes"].dropna().max())
            if max_ym is None or (y, mmax) > max_ym:
                max_ym = (y, mmax)
    log.info("%s: %s rows across %s year(s)", table, f"{total:,}", len(years))
    return total, max_ym


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean all four ongoing tables from the downloaded CSVs.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.mx_sesnsp_incidencia_delictiva.tasks.clean_sesnsp`).

    Args:
        input_dir: Root download directory (``input_dir/<table>/<RNID>.csv``).
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year_month"`` —
        the latest ``"YYYY-MM"`` across the four tables, used to poll whether
        SESNSP has published a new month. None if all tables are empty.
    """
    result: dict = {}
    max_ym: tuple[int, int] | None = None
    for table in ONGOING_TABLES:
        csv_path = find_csv(input_dir, table)
        _, tmax = clean_table(csv_path, table, output_dir)
        result[table] = output_dir / table
        if tmax is not None and (max_ym is None or tmax > max_ym):
            max_ym = tmax
    result["max_year_month"] = (
        f"{max_ym[0]}-{max_ym[1]:02d}" if max_ym is not None else None
    )
    return result
