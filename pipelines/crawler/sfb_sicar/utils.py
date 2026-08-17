"""Download + cleaning transform for br_sfb_sicar (Cadastro Ambiental Rural).

Pure functions (no Prefect) — the single canonical location for the transform.
The recurring pipeline wraps them in @task (see tasks.py); the one-shot bootstrap
in ``models/br_sfb_sicar/code/clean.py`` re-exports them so the two cannot drift.

Per theme zip: read ALL ``.shp`` parts (large UFs split into ``_1.shp``,
``_2.shp``, …), reproject SIRGAS 2000 (EPSG:4674) -> WGS84 (EPSG:4326), make the
geometry valid, emit WKT. Output is an ALL-STRING partitioned parquet dataset
(the dbt model ``safe_cast``s every column), partitioned by ``data`` (snapshot)
and ``sigla_uf``. All-string is mandatory: ``upload_to_gcs`` infers the staging
schema from a stringified header, so typed parquet is rejected — cast via arrow
(None-for-NaN), never ``astype(str)`` which would render NULL as ``"nan"``.
"""

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
from shapely import make_valid
from shapely.geometry.base import BaseGeometry

from pipelines.crawler.sfb_sicar.constants import architecture as arch

WGS84 = "EPSG:4326"
SIRGAS = "EPSG:4674"


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
    """Reproject to WGS84, make valid, return WKT strings (None for empty).

    The old crawler skipped the reprojection and emitted SIRGAS-2000 WKT; here
    the geometry is reprojected to WGS84 and repaired with ``make_valid`` before
    serialization.
    """
    if gdf.crs is None:
        gdf = gdf.set_crs(SIRGAS)
    geom = gdf.to_crs(WGS84).geometry

    def _wkt(g):
        if g is None or g.is_empty:
            return None
        if not g.is_valid:
            g = make_valid(g)
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


def clean_theme_chunked(
    zip_path: str,
    out_root: str,
    table: str,
    snapshot_iso: str,
    sigla_uf: str,
    chunk_size: int = 50000,
) -> tuple[int, int]:
    """Stream-clean a theme zip in feature chunks (bounded memory).

    Reading a whole state's shapefile at once OOMs the worker — a big UF's
    ``area_imovel`` (let alone ``app`` with tens of millions of polygons) far
    exceeds even 32 GiB once geopandas + ``make_valid`` + WKT expand it. This
    reads each ``.shp`` part ``chunk_size`` features at a time (pyogrio seeks via
    the ``.shx`` index, so paging is cheap), cleans the chunk, and writes it as a
    parquet part directly into the hive partition
    ``<out_root>/<table>/data=<snapshot>/sigla_uf=<uf>/`` — the same layout
    :func:`write_table_partitioned` produces, so ``upload_to_gcs`` reads the
    partition keys from the path and the file payload stays all-STRING.

    Args:
        zip_path: The downloaded ``<UF>_<THEME>.zip``.
        out_root: Parquet output root.
        table: Output table slug.
        snapshot_iso: Per-UF release date, ``'YYYY-MM-DD'`` (the ``data`` key).
        sigla_uf: UF code (the ``sigla_uf`` key).
        chunk_size: Features per chunk; caps peak memory.

    Returns:
        A ``(rows_written, dropped_foreign_uf)`` tuple.

    Raises:
        FileNotFoundError: If the zip contains no ``.shp``.
    """
    part_dir = os.path.join(
        out_root, table, f"data={snapshot_iso}", f"sigla_uf={sigla_uf}"
    )
    os.makedirs(part_dir, exist_ok=True)
    rows = 0
    dropped = 0
    file_idx = 0
    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(td)
        parts = sorted(glob.glob(os.path.join(td, "*.shp")))
        if not parts:
            raise FileNotFoundError(f"no .shp in {zip_path}")
        for shp in parts:
            n_feats = int(pyogrio.read_info(shp)["features"])
            for start in range(0, max(n_feats, 1), chunk_size):
                gdf = gpd.GeoDataFrame(
                    pyogrio.read_dataframe(
                        shp, skip_features=start, max_features=chunk_size
                    )
                )
                if len(gdf) == 0:
                    continue
                if gdf.crs is None:
                    gdf = gdf.set_crs(SIRGAS)
                gdf, drop_n = filter_to_uf(gdf, sigla_uf)
                dropped += drop_n
                if len(gdf) == 0:
                    continue
                df = build_table_df(table, gdf, snapshot_iso, sigla_uf)
                payload = [
                    c for c in df.columns if c not in ("data", "sigla_uf")
                ]
                pq.write_table(
                    pa.Table.from_pandas(
                        df[payload],
                        schema=all_string_schema(payload),
                        preserve_index=False,
                    ),
                    os.path.join(part_dir, f"part-{file_idx:05d}.parquet"),
                    compression="snappy",
                )
                file_idx += 1
                rows += len(df)
                del gdf, df
    return rows, dropped
