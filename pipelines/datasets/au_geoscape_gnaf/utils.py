"""Source resolution + download + cleaning transform for au_geoscape_gnaf.

Shared by the recurring pipeline (wrapped in ``@task`` in ``tasks.py``) and the
one-shot bootstrap in ``models/au_geoscape_gnaf/code/clean.py`` (which imports
``clean_all`` directly). Pure functions, no Prefect imports, so they are
importable and unit-testable.

Each quarterly G-NAF release is a full snapshot. The per-state PSV tables are
read straight out of the downloaded all-states zip (no full extraction); the
default geocode, locality/street points, and ABS mesh-block codes are folded into
the three backbone tables; output is written as parquet partitioned by
``snapshot_date`` + ``id_state`` (CNPJ-style stacking).

Two output modes, one transform:

- ``stringify=False`` — typed parquet (dates/floats/int). Used by the one-shot
  bootstrap, which uploads with an explicit typed hive schema.
- ``stringify=True`` — all-STRING parquet + a 0-row ``00_header.parquet`` guard
  per partition. Required by the pipeline: ``upload_to_gcs`` infers the staging
  schema from a stringified header, so typed parquet is rejected, and the dbt
  models ``safe_cast`` every column back to its real type. The raw PSV values are
  already ISO dates / plain decimals / small ints, so the string path keeps them
  verbatim (empty -> NULL); it never round-trips through float, so no ``"1959.0"``
  / ``"nan"`` artifacts appear.

The partition columns ``snapshot_date`` / ``id_state`` live in the hive path only,
never in the file body. ``year`` is written into the file for parity with the
bootstrap but the dbt model derives it from ``snapshot_date`` regardless.
"""

from __future__ import annotations

import csv
import datetime as dt
import gc
import io
import logging
import re
import resource
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.au_geoscape_gnaf.constants import constants

log = logging.getLogger("au_geoscape_gnaf")


def _tune_container_memory() -> None:
    """Cap glibc arenas + Arrow threads so RSS stays near the live set.

    On a many-core worker node the container sees every CPU, so glibc opens up to
    ``8 * ncpu`` malloc arenas and pyarrow sizes its pools to the core count. A
    churn-heavy per-state merge then scatters temporaries across arenas/threads
    and never returns them, so RSS explodes past the hard 16Gi limit even though
    the live set is ~2GB — while macOS (one arena, eager release) does the same
    work in ~1.5GB, which is why it never reproduced locally. Must run before the
    heavy pandas/pyarrow work (module import time).
    """
    if sys.platform.startswith("linux"):
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6")
            libc.mallopt(-8, 2)  # M_ARENA_MAX = 2
            libc.mallopt(
                -1, 0
            )  # M_TRIM_THRESHOLD = 0 (return freed mem eagerly)
        except (OSError, AttributeError):
            pass
    try:
        pa.set_cpu_count(2)
        pa.set_io_thread_count(2)
    except (ValueError, OSError):
        pass


_tune_container_memory()


def _rss_gb() -> float:
    """Return peak process RSS in GB (``ru_maxrss`` is bytes on macOS, KB on Linux)."""
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1024**3 if sys.platform == "darwin" else r / 1024**2


def _free() -> None:
    """Return freed memory to the OS between the big per-state allocations.

    The clean folds several ~5M-row frames per state via successive merges. On
    Linux the pyarrow pool (jemalloc) retains freed buffers for reuse, so the
    intermediates accumulate as RSS across the merges and the worker OOMs at its
    hard 16Gi limit — even though the live set is ~2GB (macOS releases eagerly,
    which is why it never reproduced locally). Dropping Python refs, purging the
    Arrow pool, and trimming glibc arenas after each merge/state keeps RSS near
    the live set.
    """
    gc.collect()
    pa.default_memory_pool().release_unused()
    if sys.platform.startswith("linux"):
        # Return glibc malloc arenas (numpy/pandas side) to the OS too.
        try:
            import ctypes

            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except (OSError, AttributeError):
            pass


PA_TYPE = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}

# Row-group slice for the streaming stringify writer (bounds peak write memory).
_WRITE_CHUNK = 500_000

_FILENAME_RE = re.compile(r"g-naf_([a-z]{3})(\d{2})_", re.IGNORECASE)


# ── source resolution (CKAN) ─────────────────────────────────────────────────
def _snapshot_from_url(url: str) -> str:
    """Derive ``snapshot_date`` (first of the release month) from the zip name.

    ``.../g-naf_aug26_allstates_gda2020_psv_110.zip`` -> ``"2026-08-01"``.

    Args:
        url: Download URL of the release zip.

    Returns:
        The snapshot date as ``"YYYY-MM-01"``.
    """
    name = url.rsplit("/", 1)[-1]
    m = _FILENAME_RE.search(name)
    if not m:
        raise RuntimeError(f"Cannot parse release month from {name!r}")
    mon, yy = m.group(1).lower(), int(m.group(2))
    month = constants.MONTHS.value.get(mon)
    if month is None:
        raise RuntimeError(f"Unknown month token {mon!r} in {name!r}")
    return dt.date(2000 + yy, month, 1).isoformat()


def resolve_source() -> dict:
    """Resolve the current GDA2020 all-states PSV release from the CKAN API.

    Queries ``package_show`` and selects the single resource whose URL is the
    GDA2020 all-states PSV zip. The resource UUID and build token change every
    quarter, so this must run at flow time rather than reading a hard-coded URL.

    Returns:
        ``{"url": <download url>, "snapshot_date": "YYYY-MM-01"}``.

    Raises:
        RuntimeError: if zero or more than one matching resource is found.
    """
    headers = {"User-Agent": constants.USER_AGENT.value}
    r = requests.get(
        constants.CKAN_PACKAGE_SHOW.value, headers=headers, timeout=120
    )
    r.raise_for_status()
    resources = r.json()["result"]["resources"]

    hits = []
    for res in resources:
        url = (res.get("url") or "").strip()
        low = url.lower()
        if (
            (res.get("format") or "").upper() == "ZIP"
            and "gda2020" in low
            and "allstates" in low.replace("_", "").replace("-", "")
            and "psv" in low
        ):
            hits.append(url)

    if len(hits) != 1:
        raise RuntimeError(
            f"Expected exactly 1 GDA2020 all-states PSV resource, found "
            f"{len(hits)}: {hits}"
        )
    url = hits[0]
    return {"url": url, "snapshot_date": _snapshot_from_url(url)}


def download_zip(url: str, input_dir: Path) -> Path:
    """Stream the release zip into ``input_dir``.

    A browser User-Agent is mandatory (data.gov.au 403s automated clients).

    Args:
        url: Download URL of the release zip.
        input_dir: Directory to write the zip into (created if absent).

    Returns:
        The path of the downloaded zip.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / url.rsplit("/", 1)[-1]
    headers = {"User-Agent": constants.USER_AGENT.value}
    log.info("downloading %s", dest.name)
    with requests.get(url, headers=headers, stream=True, timeout=1800) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for block in r.iter_content(chunk_size=8 << 20):
                fh.write(block)
    return dest


# ── architecture + PSV reading ───────────────────────────────────────────────
def load_arch(table: str) -> list[tuple[str, str]]:
    """Return the architecture ``(name, bigquery_type)`` pairs in order.

    Args:
        table: Table slug whose ``code/architecture/<table>.csv`` to read.

    Returns:
        Ordered ``(column_name, bigquery_type)`` pairs, including the partition
        columns ``snapshot_date`` / ``id_state`` (filtered out in the writer,
        since they are encoded in the hive path).
    """
    arch_dir = constants.ARCHITECTURE_DIR.value
    rows = []
    with open(arch_dir / f"{table}.csv", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((r["name"], r["bigquery_type"]))
    return rows


def _zip_base(zf: zipfile.ZipFile) -> tuple[str, str]:
    """Locate the Standard and Authority-Code folders inside the G-NAF zip.

    Args:
        zf: Open G-NAF release zip.

    Returns:
        ``(standard_dir, authority_code_dir)`` member-path prefixes.
    """
    for n in zf.namelist():
        if n.endswith("_STATE_psv.psv") and "/Standard/" in n:
            std = n.rsplit("/", 1)[0]  # .../Standard
            auth = std.rsplit("/", 1)[0] + "/Authority Code"
            return std, auth
    raise RuntimeError("Standard folder not found in zip")


def read_psv(
    zf: zipfile.ZipFile, path: str, usecols: list[str] | None = None
) -> pd.DataFrame:
    """Read a pipe-separated member of the zip as all-string columns.

    Uses the Arrow-backed ``string`` dtype rather than object strings: NSW alone
    is ~5.6M addresses across ~40 columns, and object strings (a Python object
    per cell) blow past the 16 Gi worker — the Arrow buffer representation is
    ~4x smaller and keeps the per-state clean within budget. Empty cells stay
    ``""`` (``na_filter=False``); the writer maps them to NULL.

    Args:
        zf: Open G-NAF release zip.
        path: Member path of the ``.psv`` file to read.
        usecols: Optional subset of columns to load.

    Returns:
        The parsed table with empty cells kept as ``""`` (not NaN).
    """
    with zf.open(path) as fh:
        return pd.read_csv(
            io.TextIOWrapper(fh, "utf-8"),
            sep="|",
            dtype="string[pyarrow]",
            na_filter=False,
            usecols=usecols,
        )


def _date(s: pd.Series) -> pd.Series:
    """Parse a ``YYYY-MM-DD`` string series to dates (``""`` -> NaT/None)."""
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").dt.date


def _float(s: pd.Series) -> pd.Series:
    """Parse a string series to floats (``""`` -> NaN)."""
    return pd.to_numeric(s.replace("", None), errors="coerce")


def active(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Keep only non-retired rows, one per key (first wins).

    Args:
        df: Source frame containing a ``DATE_RETIRED`` column.
        key: Column to deduplicate on.
    """
    a = df[df["DATE_RETIRED"] == ""]
    return a.drop_duplicates(subset=[key], keep="first")


# ── writer ───────────────────────────────────────────────────────────────────
def _string_schema(names: list[str]) -> pa.Schema:
    return pa.schema([pa.field(n, pa.string()) for n in names])


def write_partition(
    df: pd.DataFrame,
    table: str,
    arch: list[tuple[str, str]],
    state_pid: str,
    snapshot: str,
    output_dir: Path,
    stringify: bool,
) -> int:
    """Write one state's dataframe as parquet under the hive path.

    The partition columns ``snapshot_date`` / ``id_state`` are excluded from the
    file (they live in the ``snapshot_date=.../id_state=.../`` path instead).

    Args:
        df: Cleaned frame for one state (all columns are strings).
        table: Table slug (output subdirectory).
        arch: Architecture ``(name, bigquery_type)`` pairs giving column order.
        state_pid: ASGS state code (1-9) used in the hive path.
        snapshot: Snapshot date ``"YYYY-MM-DD"`` used in the hive path.
        output_dir: Root output directory.
        stringify: When True, write all-STRING parquet (verbatim raw values,
            ``""`` -> NULL) plus a 0-row header guard — the pipeline path. When
            False, write typed parquet (dates/floats/int) — the bootstrap path.

    Returns:
        Number of rows written.
    """
    cols = [c for c, _ in arch if c not in ("snapshot_date", "id_state")]
    d = (
        output_dir
        / table
        / f"snapshot_date={snapshot}"
        / f"id_state={state_pid}"
    )
    d.mkdir(parents=True, exist_ok=True)

    if stringify:
        # All-STRING, verbatim: empty cells -> NULL. Raw PSV values are already
        # ISO dates / plain decimals / small ints, so no typed round-trip is
        # needed (and none of the "1959.0" / "nan" traps can appear).
        schema = _string_schema(cols)
        # 0-row header first, so a later save_header_files/dump_header pass never
        # reads a large first parquet (OOM guard, cf. au_ato_abr).
        header = pa.table(
            {c: pa.array([], type=pa.string()) for c in cols}, schema=schema
        )
        pq.write_table(header, d / "00_header.parquet", compression="snappy")
        # Stream the parquet in row-group slices, one column at a time within each
        # slice. Building all ~45 column arrays for the whole frame at once holds a
        # second full copy alongside the source frame; on the Linux worker (inflated
        # allocator, see `_tune_container_memory`) that doubling tops the hard 16Gi
        # limit at ~5.2M rows. A bounded slice keeps the write near the live set.
        # Empty string -> NULL (NA is already null).
        n = len(df)
        writer = pq.ParquetWriter(
            d / "data.parquet", schema, compression="snappy"
        )
        try:
            for start in range(0, n, _WRITE_CHUNK):
                sl = df.iloc[start : start + _WRITE_CHUNK]
                arrays = []
                for c in cols:
                    # pyrefly: ignore [bad-argument-type]  # pandas-stubs rejects None; valid at runtime
                    a = pa.array(sl[c].replace("", None))
                    if not pa.types.is_string(a.type):
                        a = a.cast(pa.string())
                    arrays.append(a)
                writer.write_table(
                    pa.table(
                        dict(zip(cols, arrays, strict=True)), schema=schema
                    )
                )
                del arrays
        finally:
            writer.close()
        return n
    else:
        fields, data = [], {}
        for name, bqt in arch:
            if name in ("snapshot_date", "id_state"):
                continue
            col = df[name]
            if bqt == "DATE":
                data[name] = _date(col)
            elif bqt == "FLOAT64":
                data[name] = _float(col)
            elif bqt == "INT64":
                data[name] = pd.to_numeric(col, errors="coerce").astype(
                    "Int64"
                )
            else:
                # pyrefly: ignore [bad-argument-type]  # pandas-stubs rejects None `other`; valid at runtime
                data[name] = col.where(col != "", None)
            fields.append(pa.field(name, PA_TYPE[bqt]))
        tbl = pa.Table.from_pydict(
            {c: data[c] for c in cols}, schema=pa.schema(fields)
        )

    pq.write_table(tbl, d / "data.parquet", compression="snappy")
    return len(tbl)


# ── table builders ───────────────────────────────────────────────────────────
def build_address_detail(
    zf: zipfile.ZipFile, std: str, state: str, pid: str, snapshot: str
) -> pd.DataFrame:
    """Build the ``address_detail`` frame for one state.

    Folds the active default geocode (geocode_type, longitude, latitude) and the
    ABS mesh-block codes (id_mb_2016, id_mb_2021) into ADDRESS_DETAIL, and adds
    the snapshot_date / year / id_state columns.

    Args:
        zf: Open G-NAF release zip.
        std: Standard-folder member prefix.
        state: State/territory abbreviation (e.g. ``NSW``).
        pid: ASGS state code (1-9) for this state.
        snapshot: Snapshot date ``"YYYY-MM-DD"``.

    Returns:
        The cleaned per-state address frame (all string columns).
    """
    ad = read_psv(zf, f"{std}/{state}_ADDRESS_DETAIL_psv.psv")
    ad = ad.rename(
        columns={
            "FLAT_TYPE_CODE": "flat_type",
            "LEVEL_TYPE_CODE": "level_type",
            "LEVEL_GEOCODED_CODE": "level_geocoded",
        }
    )
    ad.columns = [
        c if c in ("flat_type", "level_type", "level_geocoded") else c.lower()
        for c in ad.columns
    ]

    # The folds below use column-wise `.map()` rather than `pd.merge`. A left
    # merge copies the whole ~34-column `ad` frame on every fold; on the Linux
    # worker those transients (plus the allocator overhead, see
    # `_tune_container_memory`) push RSS past the hard 16Gi limit at ~5.2M rows.
    # `.map()` adds one column at a time against a pid-indexed lookup, so the
    # peak stays near the live set (macOS merged fine, which is why it only bit
    # on the worker). Keys are unique — `active()` already dedups each.
    key = ad["address_detail_pid"]

    # default geocode -> geocode_type, longitude, latitude
    gi = active(
        read_psv(
            zf,
            f"{std}/{state}_ADDRESS_DEFAULT_GEOCODE_psv.psv",
            usecols=[
                "ADDRESS_DETAIL_PID",
                "DATE_RETIRED",
                "GEOCODE_TYPE_CODE",
                "LONGITUDE",
                "LATITUDE",
            ],
        ),
        "ADDRESS_DETAIL_PID",
    ).set_index("ADDRESS_DETAIL_PID")
    ad["geocode_type"] = key.map(gi["GEOCODE_TYPE_CODE"])
    ad["longitude"] = key.map(gi["LONGITUDE"])
    ad["latitude"] = key.map(gi["LATITUDE"])
    del gi
    _free()

    # mesh blocks 2016 / 2021 -> id_mb_2016 / id_mb_2021 (bridge -> MB code table)
    for yr in ("2016", "2021"):
        br = active(
            read_psv(
                zf,
                f"{std}/{state}_ADDRESS_MESH_BLOCK_{yr}_psv.psv",
                usecols=["ADDRESS_DETAIL_PID", "DATE_RETIRED", f"MB_{yr}_PID"],
            ),
            "ADDRESS_DETAIL_PID",
        )
        mbi = active(
            read_psv(
                zf,
                f"{std}/{state}_MB_{yr}_psv.psv",
                usecols=[f"MB_{yr}_PID", "DATE_RETIRED", f"MB_{yr}_CODE"],
            ),
            f"MB_{yr}_PID",
        ).set_index(f"MB_{yr}_PID")
        # bridge: address_detail_pid -> MB code (map the code onto the bridge,
        # then map the bridge onto ad — both single-column maps).
        br[f"id_mb_{yr}"] = br[f"MB_{yr}_PID"].map(mbi[f"MB_{yr}_CODE"])
        bri = br.set_index("ADDRESS_DETAIL_PID")[f"id_mb_{yr}"]
        ad[f"id_mb_{yr}"] = key.map(bri)
        del br, mbi, bri
        _free()

    ad["snapshot_date"] = snapshot
    ad["year"] = str(dt.date.fromisoformat(snapshot).year)
    ad["id_state"] = pid
    for c in (
        "geocode_type",
        "longitude",
        "latitude",
        "id_mb_2016",
        "id_mb_2021",
    ):
        if c not in ad:
            ad[c] = ""
    return ad


def build_street_locality(
    zf: zipfile.ZipFile, std: str, state: str, pid: str, snapshot: str
) -> pd.DataFrame:
    """Build the ``street_locality`` frame for one state.

    Folds the active STREET_LOCALITY_POINT longitude/latitude into
    STREET_LOCALITY and adds snapshot_date / year / id_state.

    Args:
        zf: Open G-NAF release zip.
        std: Standard-folder member prefix.
        state: State/territory abbreviation.
        pid: ASGS state code (1-9) for this state.
        snapshot: Snapshot date ``"YYYY-MM-DD"``.

    Returns:
        The cleaned per-state street frame (all string columns).
    """
    sl = read_psv(zf, f"{std}/{state}_STREET_LOCALITY_psv.psv").rename(
        columns={
            "STREET_LOCALITY_PID": "street_locality_pid",
            "DATE_CREATED": "date_created",
            "DATE_RETIRED": "date_retired",
            "STREET_NAME": "street_name",
            "STREET_TYPE_CODE": "street_type",
            "STREET_SUFFIX_CODE": "street_suffix",
            "STREET_CLASS_CODE": "street_class",
            "LOCALITY_PID": "locality_pid",
            "GNAF_STREET_PID": "gnaf_street_pid",
            "GNAF_RELIABILITY_CODE": "gnaf_reliability",
        }
    )
    pt = active(
        read_psv(
            zf,
            f"{std}/{state}_STREET_LOCALITY_POINT_psv.psv",
            usecols=[
                "STREET_LOCALITY_PID",
                "DATE_RETIRED",
                "LONGITUDE",
                "LATITUDE",
            ],
        ),
        "STREET_LOCALITY_PID",
    ).rename(
        columns={
            "STREET_LOCALITY_PID": "street_locality_pid",
            "LONGITUDE": "longitude",
            "LATITUDE": "latitude",
        }
    )[["street_locality_pid", "longitude", "latitude"]]
    sl = sl.merge(pt, on="street_locality_pid", how="left")
    sl["snapshot_date"] = snapshot
    sl["year"] = str(dt.date.fromisoformat(snapshot).year)
    sl["id_state"] = pid
    for c in ("longitude", "latitude"):
        if c not in sl:
            sl[c] = ""
    return sl


def build_locality(
    zf: zipfile.ZipFile, std: str, state: str, pid: str, snapshot: str
) -> pd.DataFrame:
    """Build the ``locality`` frame for one state.

    Folds the active LOCALITY_POINT longitude/latitude into LOCALITY, resolves
    ``id_state`` from STATE_PID, and adds snapshot_date / year.

    Args:
        zf: Open G-NAF release zip.
        std: Standard-folder member prefix.
        state: State/territory abbreviation.
        pid: ASGS state code (1-9); LOCALITY also carries its own STATE_PID.
        snapshot: Snapshot date ``"YYYY-MM-DD"``.

    Returns:
        The cleaned per-state locality frame (all string columns).
    """
    lo = read_psv(zf, f"{std}/{state}_LOCALITY_psv.psv").rename(
        columns={
            "LOCALITY_PID": "locality_pid",
            "DATE_CREATED": "date_created",
            "DATE_RETIRED": "date_retired",
            "LOCALITY_NAME": "locality_name",
            "PRIMARY_POSTCODE": "primary_postcode",
            "LOCALITY_CLASS_CODE": "locality_class",
            "STATE_PID": "state_pid",
            "GNAF_LOCALITY_PID": "gnaf_locality_pid",
            "GNAF_RELIABILITY_CODE": "gnaf_reliability",
        }
    )
    pt = active(
        read_psv(
            zf,
            f"{std}/{state}_LOCALITY_POINT_psv.psv",
            usecols=["LOCALITY_PID", "DATE_RETIRED", "LONGITUDE", "LATITUDE"],
        ),
        "LOCALITY_PID",
    ).rename(
        columns={
            "LOCALITY_PID": "locality_pid",
            "LONGITUDE": "longitude",
            "LATITUDE": "latitude",
        }
    )[["locality_pid", "longitude", "latitude"]]
    lo = lo.merge(pt, on="locality_pid", how="left")
    lo["snapshot_date"] = snapshot
    lo["year"] = str(dt.date.fromisoformat(snapshot).year)
    lo["id_state"] = lo["state_pid"]  # STATE_PID is already the ASGS code 1-9
    for c in ("longitude", "latitude"):
        if c not in lo:
            lo[c] = ""
    return lo


# ── dictionary ───────────────────────────────────────────────────────────────
# authority-code table -> coded column name in our schema (valor = NAME)
AUT_MAP = {
    "FLAT_TYPE": "flat_type",
    "LEVEL_TYPE": "level_type",
    "GEOCODED_LEVEL_TYPE": "level_geocoded",
    "GEOCODE_TYPE": "geocode_type",
    "STREET_TYPE": "street_type",
    "STREET_SUFFIX": "street_suffix",
    "STREET_CLASS": "street_class",
    "LOCALITY_CLASS": "locality_class",
    "GEOCODE_RELIABILITY": "gnaf_reliability",
}
# coded columns with no AUT table (hardcoded PT labels)
HARDCODED = {
    "alias_principal": {"A": "Alias", "P": "Principal"},
    "primary_secondary": {
        "P": "Endereço primário",
        "S": "Endereço secundário",
    },
    "confidence": {
        "2": "Endereço presente em três ou mais conjuntos de dados contribuidores",
        "1": "Endereço presente em dois conjuntos de dados contribuidores",
        "0": "Endereço presente em apenas um conjunto de dados contribuidor",
        "-1": "Endereço não presente em nenhum conjunto de dados contribuidor",
    },
}
# which table each coded column belongs to (id_tabela)
COL_TABLE = {
    "flat_type": "address_detail",
    "level_type": "address_detail",
    "level_geocoded": "address_detail",
    "geocode_type": "address_detail",
    "alias_principal": "address_detail",
    "confidence": "address_detail",
    "primary_secondary": "address_detail",
    "street_type": "street_locality",
    "street_suffix": "street_locality",
    "street_class": "street_locality",
    "gnaf_reliability": "street_locality",
    "locality_class": "locality",
}


def build_dicionario(zf: zipfile.ZipFile, auth: str, output_dir: Path) -> int:
    """Build the ``dicionario`` from the authority-code tables + hardcoded sets.

    Emits one ``(id_tabela, nome_coluna, chave, cobertura_temporal, valor)`` row
    per code, using the AUT ``NAME`` as the label for AUT-backed columns and the
    hardcoded PT labels for the columns G-NAF has no AUT table for.

    Args:
        zf: Open G-NAF release zip.
        auth: Authority-Code folder member prefix.
        output_dir: Root output directory (writes ``dicionario/data.parquet``).

    Returns:
        Number of dictionary rows written.
    """
    recs = []
    for aut, col in AUT_MAP.items():
        df = read_psv(zf, f"{auth}/Authority_Code_{aut}_AUT_psv.psv")
        for _, r in df.iterrows():
            recs.append((COL_TABLE[col], col, r["CODE"], "", r["NAME"]))
    for col, m in HARDCODED.items():
        for k, v in m.items():
            recs.append((COL_TABLE[col], col, k, "", v))
    # gnaf_reliability also appears on locality -> emit that table's rows too
    rel = read_psv(
        zf, f"{auth}/Authority_Code_GEOCODE_RELIABILITY_AUT_psv.psv"
    )
    for _, r in rel.iterrows():
        recs.append(("locality", "gnaf_reliability", r["CODE"], "", r["NAME"]))
    d = pd.DataFrame(
        recs,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    schema = pa.schema([pa.field(c, pa.string()) for c in d.columns])
    tbl = pa.Table.from_pandas(
        d.astype(str), schema=schema, preserve_index=False
    )
    dd = output_dir / "dicionario"
    dd.mkdir(parents=True, exist_ok=True)
    pq.write_table(tbl, dd / "data.parquet", compression="snappy")
    return len(d)


# ── orchestration ────────────────────────────────────────────────────────────
def clean_all(
    zip_path: Path,
    output_dir: Path,
    snapshot_date: str,
    states: list[str] | None = None,
    stringify: bool = True,
) -> dict:
    """Clean every selected state of one release into partitioned parquet.

    Args:
        zip_path: Downloaded all-states G-NAF release zip.
        output_dir: Directory to write ``<table>/snapshot_date=.../id_state=.../``.
        snapshot_date: Snapshot date ``"YYYY-MM-DD"`` (first of the release month).
        states: Subset of state abbreviations to process (default: all nine).
        stringify: All-STRING output for the pipeline (default) vs typed output
            for the bootstrap.

    Returns:
        Mapping of each table slug to its output directory, plus
        ``"snapshot_date"`` and ``"counts"``.
    """
    zip_path = Path(zip_path)
    output_dir = Path(output_dir)
    states = states or constants.ALL_STATES.value

    arch = {
        t: load_arch(t)
        for t in ("address_detail", "street_locality", "locality")
    }
    builders = {
        "address_detail": build_address_detail,
        "street_locality": build_street_locality,
        "locality": build_locality,
    }
    totals = {t: 0 for t in builders}

    # print() (not log.info) so Prefect's log_prints captures it on the worker.
    print(
        f"[gnaf] clean_all start; pandas={pd.__version__} "
        f"pyarrow={pa.__version__} stringify={stringify} states={states} "
        f"RSS={_rss_gb():.1f}GB",
        flush=True,
    )
    with zipfile.ZipFile(zip_path) as zf:
        std, auth = _zip_base(zf)
        # state -> pid map from the per-state STATE tables
        pid_of = {}
        for s in constants.ALL_STATES.value:
            srow = read_psv(zf, f"{std}/{s}_STATE_psv.psv")
            pid_of[s] = str(srow.iloc[0]["STATE_PID"])
        for state in states:
            pid = pid_of[state]
            for t, fn in builders.items():
                df = fn(zf, std, state, pid, snapshot_date)
                n = write_partition(
                    df, t, arch[t], pid, snapshot_date, output_dir, stringify
                )
                totals[t] += n
                del df
                _free()
            print(
                f"[gnaf] cleaned {state} (id_state={pid}) "
                f"RSS={_rss_gb():.1f}GB",
                flush=True,
            )
        ndic = build_dicionario(zf, auth, output_dir)
    print(
        f"[gnaf] clean_all done RSS={_rss_gb():.1f}GB counts={totals}",
        flush=True,
    )

    totals["dicionario"] = ndic
    return {
        "address_detail": output_dir / "address_detail",
        "street_locality": output_dir / "street_locality",
        "locality": output_dir / "locality",
        "dicionario": output_dir / "dicionario",
        "snapshot_date": snapshot_date,
        "counts": totals,
    }
