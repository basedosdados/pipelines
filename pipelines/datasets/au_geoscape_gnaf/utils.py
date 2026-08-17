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
import io
import logging
import re
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.au_geoscape_gnaf.constants import constants

log = logging.getLogger("au_geoscape_gnaf")

PA_TYPE = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}

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
        # Build the table one column at a time, staying in Arrow. Converting the
        # whole frame back to Python `str` at once (a per-cell object for ~5.6M x
        # 40) blows past the worker; Arrow keeps it a compact buffer and only one
        # column is ever transient. Empty string -> NULL (NA is already null).
        arrays = []
        for c in cols:
            # pyrefly: ignore [bad-argument-type]  # pandas-stubs rejects None; valid at runtime
            a = pa.array(df[c].replace("", None))
            if not pa.types.is_string(a.type):
                a = a.cast(pa.string())
            arrays.append(a)
        tbl = pa.table(dict(zip(cols, arrays, strict=True)), schema=schema)
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

    # default geocode -> geocode_type, longitude, latitude
    g = active(
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
    ).rename(
        columns={
            "ADDRESS_DETAIL_PID": "address_detail_pid",
            "GEOCODE_TYPE_CODE": "geocode_type",
            "LONGITUDE": "longitude",
            "LATITUDE": "latitude",
        }
    )[["address_detail_pid", "geocode_type", "longitude", "latitude"]]
    ad = ad.merge(g, on="address_detail_pid", how="left")

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
        mb = active(
            read_psv(
                zf,
                f"{std}/{state}_MB_{yr}_psv.psv",
                usecols=[f"MB_{yr}_PID", "DATE_RETIRED", f"MB_{yr}_CODE"],
            ),
            f"MB_{yr}_PID",
        )[[f"MB_{yr}_PID", f"MB_{yr}_CODE"]]
        br = br.merge(mb, on=f"MB_{yr}_PID", how="left")
        br = br.rename(
            columns={
                "ADDRESS_DETAIL_PID": "address_detail_pid",
                f"MB_{yr}_CODE": f"id_mb_{yr}",
            }
        )[["address_detail_pid", f"id_mb_{yr}"]]
        ad = ad.merge(br, on="address_detail_pid", how="left")

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

    with zipfile.ZipFile(zip_path) as zf:
        std, auth = _zip_base(zf)
        # state -> pid map from the per-state STATE tables
        pid_of = {}
        for s in constants.ALL_STATES.value:
            srow = read_psv(zf, f"{std}/{s}_STATE_psv.psv")
            pid_of[s] = str(srow.iloc[0]["STATE_PID"])
        log.info("state->pid: %s", pid_of)
        for state in states:
            pid = pid_of[state]
            for t, fn in builders.items():
                df = fn(zf, std, state, pid, snapshot_date)
                n = write_partition(
                    df, t, arch[t], pid, snapshot_date, output_dir, stringify
                )
                totals[t] += n
                log.info("  %s %s: %d", state, t, n)
                del df
        ndic = build_dicionario(zf, auth, output_dir)

    totals["dicionario"] = ndic
    return {
        "address_detail": output_dir / "address_detail",
        "street_locality": output_dir / "street_locality",
        "locality": output_dir / "locality",
        "dicionario": output_dir / "dicionario",
        "snapshot_date": snapshot_date,
        "counts": totals,
    }
