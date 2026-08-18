"""Download + cleaning transform for br_sfb_sicar (Cadastro Ambiental Rural).

Pure functions (no Prefect) — the single canonical location for the transform.
The recurring pipeline wraps them in @task (see tasks.py); the one-shot bootstrap
in ``models/br_sfb_sicar/code/clean.py`` re-exports them so the two cannot drift.

Per theme zip: read ALL ``.shp`` parts (large UFs split into ``_1.shp``,
``_2.shp``, …), reproject SIRGAS 2000 (EPSG:4674) -> WGS84 (EPSG:4326), emit WKT.
Geometry validity repair is left to BigQuery on ingest
(``st_geogfromtext(make_valid => true)``) — GEOS ``make_valid`` on a single dense
Amazonas ``app`` feature OOMs the worker. Output is an ALL-STRING parquet dataset
(the dbt model ``safe_cast``s every column), partitioned by ``data`` (snapshot)
and ``sigla_uf``. All-string is mandatory: ``upload_to_gcs`` infers the staging
schema from a stringified header, so typed parquet is rejected — cast via arrow
(None-for-NaN), never ``astype(str)`` which would render NULL as ``"nan"``.
"""

import ctypes
import glob
import os
import tempfile
import time as tm
import zipfile
from datetime import datetime

import geopandas as gpd
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from shapely.geometry.base import BaseGeometry

from pipelines.crawler.sfb_sicar.constants import architecture as arch

WGS84 = "EPSG:4326"
SIRGAS = "EPSG:4674"


# ── glibc malloc tuning (Linux workers only; no-op on macOS/non-glibc) ────────
# The clean churns through millions of tiny per-feature GEOS/shapely/WKT
# allocations. On the deployed Linux worker, glibc opens ``8 x ncores`` malloc
# arenas by default and each retains freed memory, so RSS balloons ~10x the
# real working set — a dense Amazonas ``app`` chunk that peaks at ~3.6 GB on a
# (non-glibc) macOS laptop OOMed the 32 GiB worker. The Docker image sets
# MALLOC_ARENA_MAX in the environment (read by glibc before Python starts,
# which is the reliable path), and the flow re-sets it via ``job_variables``;
# this ``mallopt`` call and the per-chunk ``malloc_trim`` are the in-process
# backstops for that same fix.
try:
    _LIBC = ctypes.CDLL("libc.so.6")
    _LIBC.mallopt(-8, 2)  # M_ARENA_MAX = -8: cap arenas at 2
except OSError:
    _LIBC = None  # not glibc (macOS) — nothing to tune


def _malloc_trim() -> None:
    """Return freed heap memory to the OS after a chunk (glibc only)."""
    if _LIBC is not None:
        _LIBC.malloc_trim(0)


# ── download ────────────────────────────────────────────────────────────────
def retry_download_car(
    car,
    state: str,
    polygon: str,
    folder: str,
    tries: int = 25,
    max_retries: int = 8,
) -> None:
    """Download one state x theme, retrying through the CAR server's timeouts.

    The CAR API is unstable — read timeouts are frequent — so the download is
    retried on ``httpx.ReadTimeout``. Each ``download_state`` call itself makes
    ``tries`` captcha attempts (solved by SICAR via Tesseract OCR).

    Args:
        car: A ``SICAR.Sicar`` instance.
        state: Two-letter UF code (SICAR's State enum accepts the string).
        polygon: SICAR Polygon enum value, e.g. ``"AREA_IMOVEL"``, ``"APPS"``.
        folder: Directory the zip is written into (``<UF>_<POLYGON>.zip``).
        tries: Captcha attempts per ``download_state`` call.
        max_retries: Timeout retries wrapping the whole download.

    Raises:
        httpx.ReadTimeout: If every retry times out.
    """
    retries = 0
    while retries < max_retries:
        try:
            car.download_state(
                state=state, polygon=polygon, folder=folder, tries=tries
            )
            return
        except httpx.ReadTimeout:
            retries += 1
            if retries >= max_retries:
                raise
            tm.sleep(8)


def release_dates_to_iso(release_dates: dict) -> dict:
    """Normalize SICAR's ``get_release_dates`` to ``{UF: 'YYYY-MM-DD'}``.

    ``get_release_dates`` returns ``{State: 'dd/mm/yyyy'}`` (keys are State
    enums, which are ``str`` subclasses). Both the enum ``.value`` and a bare
    string key resolve to the two-letter UF code.

    Args:
        release_dates: The raw mapping from ``Sicar().get_release_dates()``.

    Returns:
        ``{uf_code: 'YYYY-MM-DD'}``, skipping any UF whose date fails to parse.
    """
    out = {}
    for state, value in release_dates.items():
        uf = getattr(state, "value", state)
        try:
            iso = datetime.strptime(str(value), "%d/%m/%Y").date().isoformat()
        except (ValueError, TypeError):
            continue
        out[str(uf)] = iso
    return out


def max_release_iso(release_iso: dict) -> str | None:
    """Return the newest per-UF release date (ISO), the source ``max_date``.

    This is the ``latest period`` helper used for the source-update poll: the
    freshest snapshot the source currently advertises across all UFs.

    Args:
        release_iso: ``{uf: 'YYYY-MM-DD'}`` from :func:`release_dates_to_iso`.

    Returns:
        The maximum ISO date string, or ``None`` if the map is empty.
    """
    return max(release_iso.values()) if release_iso else None


# ── cleaning transform (canonical; bootstrap re-exports these) ───────────────
def read_theme_zip(zip_path: str) -> gpd.GeoDataFrame:
    """Read and concatenate ALL ``.shp`` parts inside a theme zip.

    Large UFs ship a theme split across ``_1.shp``, ``_2.shp``, … — the old
    crawler read only the first part and silently dropped the rest. Every part
    is read and concatenated here.

    Args:
        zip_path: Path to a ``<UF>_<THEME>.zip`` download.

    Returns:
        One GeoDataFrame with all parts stacked, in the source CRS.

    Raises:
        FileNotFoundError: If the zip contains no ``.shp``.
    """
    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(td)
        parts = sorted(glob.glob(os.path.join(td, "*.shp")))
        if not parts:
            raise FileNotFoundError(f"no .shp in {zip_path}")
        frames = [gpd.read_file(p) for p in parts]
        crs = frames[0].crs or SIRGAS
        gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=crs)
    return gdf


def geometry_to_wkt(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Reproject SIRGAS 2000 -> WGS84 and return WKT strings (None for empty).

    Validity repair is deliberately NOT done here. GEOS ``make_valid`` on a
    self-intersecting Amazonas ``app`` multipolygon (hundreds of thousands of
    vertices tracing every watercourse) can balloon to gigabytes for a *single*
    feature and OOMs the worker no matter how small the chunk. It is also
    redundant: every dbt model ingests the WKT via
    ``safe.st_geogfromtext(geometria, make_valid => true)``, so BigQuery repairs
    the geometry on load. WKT serialization does not require validity, so the raw
    (winding-corrected by pyogrio on read) geometry is emitted as-is.
    """
    if gdf.crs is None:
        gdf = gdf.set_crs(SIRGAS)
    geom = gdf.to_crs(WGS84).geometry

    def _wkt(g):
        if g is None or g.is_empty:
            return None
        return (
            g.wkt if isinstance(g, BaseGeometry) and not g.is_empty else None
        )

    return geom.map(_wkt)


def _num_str(series: pd.Series) -> pd.Series:
    """Numeric -> string, NaN/inf -> None (never the literal ``'nan'``)."""

    def f(v):
        if v is None or pd.isna(v):
            return None
        return repr(float(v))

    return series.map(f)


def _date_str(series: pd.Series) -> pd.Series:
    """``dd/mm/yyyy`` -> ``'YYYY-MM-DD'`` string, unparseable -> None."""

    def f(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v).strip()
        if s in ("", "None", "nan", "0", "00/00/0000"):
            return None
        try:
            return datetime.strptime(s, "%d/%m/%Y").date().isoformat()
        except ValueError:
            return None

    return series.map(f)


def _str(series: pd.Series) -> pd.Series:
    def f(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v).strip()
        return s if s not in ("", "None", "nan") else None

    return series.map(f)


def build_table_df(
    table: str, gdf: gpd.GeoDataFrame, snapshot_iso: str, sigla_uf: str
) -> pd.DataFrame:
    """Map a raw theme GeoDataFrame to the architecture's all-string columns.

    ``sigla_uf`` and ``id_municipio`` are derived from the authoritative CAR key
    ``cod_imovel`` (``UF-IBGE7-HASH``) so they stay internally consistent.

    Args:
        table: Output table slug (an ``arch.TABLES`` key).
        gdf: Raw theme GeoDataFrame.
        snapshot_iso: Per-UF release date, ``'YYYY-MM-DD'``.
        sigla_uf: UF code (fallback when ``cod_imovel`` is absent).

    Returns:
        An all-string DataFrame in architecture column order.
    """
    spec = arch.TABLES[table]
    n = len(gdf)
    cod = (
        gdf["cod_imovel"].astype("string")
        if "cod_imovel" in gdf
        else pd.Series([None] * n)
    )
    out = {}
    for col in spec:
        name, src, typ = col["name"], col["src"], col["type"]
        if src == "__snapshot__":
            out[name] = pd.Series([snapshot_iso] * n)
        elif src == "__uf_cod__":
            out[name] = (
                _str(gdf["cod_estado"])
                if "cod_estado" in gdf
                else pd.Series([sigla_uf] * n)
            )
        elif src == "__uf_split__":
            out[name] = cod.map(
                lambda c: c.split("-")[0] if isinstance(c, str) else sigla_uf
            )
        elif src == "__muni_split__":
            out[name] = cod.map(
                lambda c: (
                    c.split("-")[1]
                    if isinstance(c, str) and len(c.split("-")) > 1
                    else None
                )
            )
        elif src == "__wkt__":
            out[name] = geometry_to_wkt(gdf)
        elif typ == "FLOAT64":
            out[name] = (
                _num_str(gdf[src]) if src in gdf else pd.Series([None] * n)
            )
        elif typ == "DATE":
            out[name] = (
                _date_str(gdf[src]) if src in gdf else pd.Series([None] * n)
            )
        else:  # STRING
            out[name] = _str(gdf[src]) if src in gdf else pd.Series([None] * n)
    return pd.DataFrame(out)


def filter_to_uf(gdf: gpd.GeoDataFrame, sigla_uf: str):
    """Keep only rows whose ``cod_imovel`` UF-prefix matches this file's UF.

    A handful of properties from a neighbouring state (border/administrative
    quirks) appear in a UF's download; they are ingested from their own UF's file
    under that UF's snapshot date, so drop them here to avoid a property landing
    under two snapshot dates.

    Returns:
        A ``(gdf, dropped_count)`` tuple.
    """
    if "cod_imovel" not in gdf:
        return gdf, 0
    pref = gdf["cod_imovel"].astype("string").str.split("-").str[0]
    keep = pref == sigla_uf
    return gdf.loc[keep].reset_index(drop=True), (~keep).sum()


def all_string_schema(columns) -> pa.Schema:
    return pa.schema([(c, pa.string()) for c in columns])


def write_table_partitioned(
    df: pd.DataFrame, out_root: str, table: str
) -> str:
    """Write hive-partitioned parquet (``data``, ``sigla_uf`` are the keys).

    All columns are cast to arrow string before writing so BigQuery's inferred
    (all-STRING) staging schema accepts the files.

    Args:
        df: The all-string DataFrame from :func:`build_table_df`.
        out_root: Output root; the table lands under ``<out_root>/<table>/``.
        table: Table slug.

    Returns:
        The table's output directory.
    """
    root = os.path.join(out_root, table)
    table_pa = pa.Table.from_pandas(
        df, schema=all_string_schema(list(df.columns)), preserve_index=False
    )
    pq.write_to_dataset(
        table_pa,
        root_path=root,
        partition_cols=["data", "sigla_uf"],
        compression="snappy",
        existing_data_behavior="overwrite_or_ignore",
    )
    return root


def _adaptive_chunk_size(
    shp: str,
    budget_bytes: int,
    cap: int,
    floor: int = 200,
    windows: int = 5,
    per_window: int = 128,
) -> tuple[int, int]:
    """Return ``(n_features, chunk)``, sizing chunks by geometry bytes not count.

    Feature-count chunking OOMs on vertex-dense themes: an Amazonas ``app`` (APP
    strips follow every watercourse) feature carries orders of magnitude more
    vertices than an ``area_imovel`` boundary, so a fixed 50k chunk that is a few
    GB for one table is tens of GB for the other.

    Sizing off the file *head* is not enough — the dense features cluster deep in
    the file, so a 256-feature head probe read Amazonas ``app`` at ~3 KB/feat and
    still OOMed at a 50k chunk (densest window ~45 KB/feat). This samples
    ``windows`` evenly spaced blocks and sizes off the **densest** block's WKB
    bytes per feature, so the estimate is driven by the worst region rather than
    a lucky sparse head. With ``make_valid`` moved to BigQuery (see
    :func:`geometry_to_wkt`), the remaining peak is only the shapely load, the
    reproject copy, and WKT expansion — a few times the chunk's raw WKB — so a
    256 MB budget keeps peak near 1 GB, well under the 32 GiB worker.
    """
    n = int(pyogrio.read_info(shp)["features"])
    if n == 0:
        return 0, cap
    per = 1.0
    for i in range(windows):
        pos = min(int(i * n / windows), max(n - 1, 0))
        take = min(per_window, n - pos)
        if take <= 0:
            continue
        g = gpd.GeoDataFrame(
            pyogrio.read_dataframe(shp, skip_features=pos, max_features=take)
        ).geometry
        b = int(g.to_wkb().map(lambda x: len(x) if x else 0).sum())
        per = max(per, b / take)
    chunk = max(floor, min(cap, int(budget_bytes / per)))
    print(
        f"  {os.path.basename(shp)}: {n} feats, "
        f"~{per / 1024:.1f}KB/feat(densest window) -> chunk={chunk}"
    )
    return n, chunk


def partition_dir(
    out_root: str, table: str, snapshot_iso: str, sigla_uf: str
) -> str:
    """The hive partition directory ``<out_root>/<table>/data=…/sigla_uf=…``."""
    return os.path.join(
        out_root, table, f"data={snapshot_iso}", f"sigla_uf={sigla_uf}"
    )


def extract_theme_zip(zip_path: str, dest: str) -> list[str]:
    """Extract a theme zip to ``dest``; return its sorted ``.shp`` part paths.

    Large UFs split a theme across ``_1.shp``, ``_2.shp``, … — all are returned.

    Raises:
        FileNotFoundError: If the zip contains no ``.shp``.
    """
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(dest)
    parts = sorted(glob.glob(os.path.join(dest, "*.shp")))
    if not parts:
        raise FileNotFoundError(f"no .shp in {zip_path}")
    return parts


def plan_shp_ranges(
    parts: list[str], budget_bytes: int, cap: int = 25000
) -> list[tuple[str, int, int]]:
    """Split extracted ``.shp`` parts into geometry-budgeted feature ranges.

    Returns ``[(shp_path, start, count), …]`` — one entry per chunk, sized by
    :func:`_adaptive_chunk_size` so each range holds ~``budget_bytes`` of raw
    geometry. The recurring pipeline cleans each range in its own subprocess.
    """
    ranges = []
    for shp in parts:
        n_feats, chunk = _adaptive_chunk_size(
            shp, budget_bytes=budget_bytes, cap=cap
        )
        for start in range(0, max(n_feats, 1), chunk):
            ranges.append((shp, start, chunk))
    return ranges


def clean_shp_range(
    shp: str,
    start: int,
    count: int,
    part_dir: str,
    table: str,
    snapshot_iso: str,
    sigla_uf: str,
    file_idx: int,
) -> tuple[int, int]:
    """Clean ONE feature range ``[start, start+count)`` into one parquet part.

    This is the smallest unit of work and the one the recurring pipeline runs in
    a fresh short-lived subprocess (see
    :mod:`pipelines.crawler.sfb_sicar._clean_runner`). Because that process exits
    after writing its single part, the OS reclaims all of its memory — glibc
    arena growth and heap fragmentation, which otherwise creep up chunk after
    chunk in one long-lived process and OOM the worker on dense Amazonas ``app``,
    cannot accumulate. Peak RSS is permanently one range, whatever the allocator
    or core count.

    Reprojects SIRGAS 2000 -> WGS84 and emits WKT; BigQuery repairs validity on
    ingest (see :func:`geometry_to_wkt`). Writes
    ``<part_dir>/part-<file_idx>.parquet`` (all-STRING payload).

    Returns:
        A ``(rows_written, dropped_foreign_uf)`` tuple.
    """
    os.makedirs(part_dir, exist_ok=True)
    gdf = gpd.GeoDataFrame(
        pyogrio.read_dataframe(shp, skip_features=start, max_features=count)
    )
    if len(gdf) == 0:
        return 0, 0
    if gdf.crs is None:
        gdf = gdf.set_crs(SIRGAS)
    gdf, dropped = filter_to_uf(gdf, sigla_uf)
    if len(gdf) == 0:
        return 0, dropped
    df = build_table_df(table, gdf, snapshot_iso, sigla_uf)
    payload = [c for c in df.columns if c not in ("data", "sigla_uf")]
    pq.write_table(
        pa.Table.from_pandas(
            df[payload],
            schema=all_string_schema(payload),
            preserve_index=False,
        ),
        os.path.join(part_dir, f"part-{file_idx:05d}.parquet"),
        compression="snappy",
    )
    return len(df), dropped


def clean_theme_chunked(
    zip_path: str,
    out_root: str,
    table: str,
    snapshot_iso: str,
    sigla_uf: str,
    chunk_size: int | None = None,
    budget_bytes: int = 96 * 1024 * 1024,
) -> tuple[int, int]:
    """Clean a whole theme zip in-process, one geometry-budgeted range at a time.

    Convenience wrapper for local / one-shot bootstrap use, where a single
    long-lived process is fine (macOS malloc does not fragment the way the Linux
    worker's glibc does). The recurring pipeline instead runs each range from
    :func:`plan_shp_ranges` in its own subprocess via
    :mod:`pipelines.crawler.sfb_sicar._clean_runner` — see :func:`clean_shp_range`
    for why per-range process isolation is what keeps the worker bounded.

    Args:
        zip_path: The downloaded ``<UF>_<THEME>.zip``.
        out_root: Parquet output root.
        table: Output table slug.
        snapshot_iso: Per-UF release date, ``'YYYY-MM-DD'`` (the ``data`` key).
        sigla_uf: UF code (the ``sigla_uf`` key).
        chunk_size: Hard cap on features per range; ``None`` uses 25000.
        budget_bytes: Raw-geometry byte budget per range.

    Returns:
        A ``(rows_written, dropped_foreign_uf)`` tuple.
    """
    cap = chunk_size or 25000
    part_dir = partition_dir(out_root, table, snapshot_iso, sigla_uf)
    rows = 0
    dropped = 0
    with tempfile.TemporaryDirectory() as td:
        parts = extract_theme_zip(zip_path, td)
        ranges = plan_shp_ranges(parts, budget_bytes=budget_bytes, cap=cap)
        for idx, (shp, start, count) in enumerate(ranges):
            r, d = clean_shp_range(
                shp, start, count, part_dir, table, snapshot_iso, sigla_uf, idx
            )
            rows += r
            dropped += d
            _malloc_trim()
    return rows, dropped
