"""
Clean Geoscape G-NAF (au_geoscape_gnaf) into partitioned parquet.

Reads the per-state PSV tables directly from the downloaded all-states zip (no
full extraction), folds the default geocode / locality & street points / ABS
mesh-block codes into the three backbone tables, and writes typed parquet
partitioned by snapshot_date + id_state. Emulates br_me_cnpj: each quarterly
release is a snapshot stacked under its snapshot_date.

Column order/types come from code/architecture/*.csv (the authoritative schema).

Env:
  GNAF_DATA_DIR   default ~/Downloads/au_geoscape_gnaf_data
  GNAF_ZIP        default <DATA_DIR>/input/g-naf_may26_gda2020.zip
  GNAF_SNAPSHOT   default 2026-05-01   (first day of the release month)
  GNAF_STATES     default all          (comma list, e.g. OT,ACT to test)
"""

import csv
import datetime as dt
import io
import os
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")

DATA_DIR = os.path.expanduser(
    os.environ.get("GNAF_DATA_DIR", "~/Downloads/au_geoscape_gnaf_data")
)
ZIP = os.environ.get(
    "GNAF_ZIP", os.path.join(DATA_DIR, "input", "g-naf_may26_gda2020.zip")
)
OUT = os.path.join(DATA_DIR, "output")
SNAPSHOT = os.environ.get("GNAF_SNAPSHOT", "2026-05-01")
SNAP_DATE = dt.date.fromisoformat(SNAPSHOT)
SNAP_YEAR = SNAP_DATE.year
ALL_STATES = ["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"]
STATES = os.environ.get("GNAF_STATES", "").strip()
STATES = [s.strip() for s in STATES.split(",") if s.strip()] or ALL_STATES

PA_TYPE = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}


def load_arch(table: str) -> list[tuple[str, str]]:
    """Return the architecture (name, bigquery_type) pairs in order.

    Args:
        table: Table slug whose ``code/architecture/<table>.csv`` to read.

    Returns:
        Ordered ``(column_name, bigquery_type)`` pairs, including the partition
        columns ``snapshot_date`` / ``id_state`` (they are filtered out later,
        in ``write_parquet``, since they are encoded in the hive path).
    """
    rows = []
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as f:
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
            dtype=str,
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


def write_parquet(
    df: pd.DataFrame,
    table: str,
    arch: list[tuple[str, str]],
    state_pid: str,
) -> int:
    """Write one state's dataframe as typed parquet under the hive path.

    The partition columns ``snapshot_date`` / ``id_state`` are excluded from the
    file (they live in the ``snapshot_date=.../id_state=.../`` path instead).

    Args:
        df: Cleaned frame for one state.
        table: Table slug (output subdirectory).
        arch: Architecture ``(name, bigquery_type)`` pairs giving column order.
        state_pid: ASGS state code (1-9) used in the hive path.

    Returns:
        Number of rows written.
    """
    cols = [c for c, _ in arch if c not in ("snapshot_date", "id_state")]
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
            data[name] = pd.to_numeric(col, errors="coerce").astype("Int64")
        else:
            data[name] = col.where(col != "", None)
        fields.append(pa.field(name, PA_TYPE[bqt]))
    tbl = pa.Table.from_pydict(
        {c: data[c] for c in cols}, schema=pa.schema(fields)
    )
    d = os.path.join(
        OUT, table, f"snapshot_date={SNAPSHOT}", f"id_state={state_pid}"
    )
    os.makedirs(d, exist_ok=True)
    pq.write_table(tbl, os.path.join(d, "data.parquet"), compression="snappy")
    return len(tbl)


# ---------------------------------------------------------------- table builders
def build_address_detail(
    zf: zipfile.ZipFile,
    std: str,
    state: str,
    pid: str,
    arch: list[tuple[str, str]],
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
        arch: Architecture pairs (unused here; kept for a uniform builder API).

    Returns:
        The cleaned per-state address frame.
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

    ad["snapshot_date"] = SNAPSHOT
    ad["year"] = SNAP_YEAR
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
    zf: zipfile.ZipFile,
    std: str,
    state: str,
    pid: str,
    arch: list[tuple[str, str]],
) -> pd.DataFrame:
    """Build the ``street_locality`` frame for one state.

    Folds the active STREET_LOCALITY_POINT longitude/latitude into
    STREET_LOCALITY and adds snapshot_date / year / id_state.

    Args:
        zf: Open G-NAF release zip.
        std: Standard-folder member prefix.
        state: State/territory abbreviation.
        pid: ASGS state code (1-9) for this state.
        arch: Architecture pairs (unused here; uniform builder API).

    Returns:
        The cleaned per-state street frame.
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
    sl["snapshot_date"] = SNAPSHOT
    sl["year"] = SNAP_YEAR
    sl["id_state"] = pid
    for c in ("longitude", "latitude"):
        if c not in sl:
            sl[c] = ""
    return sl


def build_locality(
    zf: zipfile.ZipFile,
    std: str,
    state: str,
    pid: str,
    arch: list[tuple[str, str]],
) -> pd.DataFrame:
    """Build the ``locality`` frame for one state.

    Folds the active LOCALITY_POINT longitude/latitude into LOCALITY, resolves
    ``id_state`` from STATE_PID, and adds snapshot_date / year.

    Args:
        zf: Open G-NAF release zip.
        std: Standard-folder member prefix.
        state: State/territory abbreviation.
        pid: ASGS state code (1-9); LOCALITY also carries its own STATE_PID.
        arch: Architecture pairs (unused here; uniform builder API).

    Returns:
        The cleaned per-state locality frame.
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
    lo["snapshot_date"] = SNAPSHOT
    lo["year"] = SNAP_YEAR
    lo["id_state"] = lo["state_pid"]  # STATE_PID is already the ASGS code 1-9
    for c in ("longitude", "latitude"):
        if c not in lo:
            lo[c] = ""
    return lo


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


def build_dicionario(zf: zipfile.ZipFile, auth: str) -> int:
    """Build the ``dicionario`` from the authority-code tables + hardcoded sets.

    Emits one ``(id_tabela, nome_coluna, chave, cobertura_temporal, valor)`` row
    per code, using the AUT ``NAME`` as the label for AUT-backed columns and the
    hardcoded PT labels for the columns G-NAF has no AUT table for.

    Args:
        zf: Open G-NAF release zip.
        auth: Authority-Code folder member prefix.

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
    dd = os.path.join(OUT, "dicionario")
    os.makedirs(dd, exist_ok=True)
    pq.write_table(tbl, os.path.join(dd, "data.parquet"), compression="snappy")
    return len(d)


def main() -> None:
    """Clean every selected state into partitioned parquet + the dicionario."""
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
    with zipfile.ZipFile(ZIP) as zf:
        std, auth = _zip_base(zf)
        # state pid map from STATE tables
        pid_of = {}
        for s in ALL_STATES:
            srow = read_psv(zf, f"{std}/{s}_STATE_psv.psv")
            pid_of[s] = srow.iloc[0]["STATE_PID"]
        print(f"state->pid: {pid_of}")
        for state in STATES:
            pid = pid_of[state]
            for t, fn in builders.items():
                df = fn(zf, std, state, pid, arch[t])
                n = write_parquet(df, t, arch[t], pid)
                totals[t] += n
                print(f"  {state} {t}: {n:,}", flush=True)
                del df
        ndic = build_dicionario(zf, auth)
    print(f"\nTOTALS (states={','.join(STATES)}):")
    for t, n in totals.items():
        print(f"  {t}: {n:,}")
    print(f"  dicionario: {ndic}")


if __name__ == "__main__":
    main()
