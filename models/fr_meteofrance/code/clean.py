"""Clean the Météo-France sources into partitioned, all-STRING parquet.

Tables produced under ``$MF_OUTPUT`` (default ``~/Downloads/fr_meteofrance_data/output``):

* ``synop/ano=<year>/data.parquet``   3-hourly SYNOP observations, 1996-2026
* ``station_synop/data.parquet``      the SYNOP station list
* ``station_climatologique/data.parquet``  the climate-normals station list
* ``normale_climatologique/data.parquet``  1991-2020 normals and records, long format

Staging is all-STRING by house convention: the dbt models ``safe_cast`` every
column to its architecture type. Values are passed through their real pandas
dtype first and then cast with arrow, so ``1996`` does not serialize as
``"1996.0"`` and NULL does not serialize as ``"nan"``.
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import parse_ficheclim
from schema_map import (
    NORMALE_COLUMNS,
    STATION_CLIMATOLOGIQUE_COLUMNS,
    STATION_SYNOP_COLUMNS,
    SYNOP_COLUMNS,
    SYNOP_LEADING,
)

INPUT = Path(
    os.path.expanduser(
        os.environ.get("MF_INPUT", "~/Downloads/fr_meteofrance_data/input")
    )
)
OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output")
    )
)

CODED = {tgt for _s, tgt, _t, _u, is_dict, _d in SYNOP_COLUMNS if is_dict}
INT_COLS = {tgt for _s, tgt, t, _u, _d, _ds in SYNOP_COLUMNS if t == "INT64"}
FLOAT_COLS = {
    tgt for _s, tgt, t, _u, _d, _ds in SYNOP_COLUMNS if t == "FLOAT64"
}
RENAME = {src: tgt for src, tgt, *_ in SYNOP_COLUMNS}

SYNOP_ORDER = [c[0] for c in SYNOP_LEADING] + [c[1] for c in SYNOP_COLUMNS]
STATION_SYNOP_ORDER = [c[0] for c in STATION_SYNOP_COLUMNS]
STATION_CLIM_ORDER = [c[0] for c in STATION_CLIMATOLOGIQUE_COLUMNS]
NORMALE_ORDER = [c[0] for c in NORMALE_COLUMNS]


def to_string_table(df, order):
    """Cast every column to STRING, preserving NULL, in a fixed column order."""
    table = pa.Table.from_pandas(df[order], preserve_index=False)
    schema = pa.schema([(name, pa.string()) for name in order])
    return table.cast(schema)


def write_parquet(df, order, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")


def normalise_code(series):
    """``'2.0'`` -> ``'2'``; the source writes some code columns as reals."""
    s = series.astype("string").str.strip()
    s = s.mask(s.eq(""))
    return s.str.replace(r"^(-?\d+)\.0+$", r"\1", regex=True)


def read_synop_year(path):
    df = pd.read_csv(
        path, sep=";", dtype=str, low_memory=False, na_values=[""]
    )
    validity = pd.to_datetime(
        df["validity_time"], format="ISO8601", utc=True, errors="coerce"
    )
    bad = validity.isna().sum()
    if bad:
        raise ValueError(f"{path}: {bad} rows with unparseable validity_time")

    # A SYNOP message can be retransmitted or corrected: the same station and
    # validity time then appears more than once, distinguished only by
    # insert_time. Keep the latest insertion, which is the corrected report.
    df = df.assign(_validity=validity)
    insert = pd.to_datetime(
        df["insert_time"], format="ISO8601", utc=True, errors="coerce"
    )
    df = df.assign(_insert=insert, _row=range(len(df)))
    df = (
        df.sort_values(["_insert", "_row"], na_position="first")
        .drop_duplicates(subset=["geo_id_wmo", "_validity"], keep="last")
        .sort_values(["_validity", "geo_id_wmo"])
        .reset_index(drop=True)
    )

    out = pd.DataFrame(index=df.index)
    local = df["_validity"].dt.tz_convert("UTC").dt.tz_localize(None)
    out["ano"] = local.dt.year.astype("Int64")
    out["mes"] = local.dt.month.astype("Int64")
    out["data"] = local.dt.strftime("%Y-%m-%d")
    out["hora"] = local.dt.strftime("%H:%M:%S")
    out["indicatif_omm"] = df["geo_id_wmo"].astype("string").str.strip()
    for src, tgt in (
        ("reference_time", "date_heure_traitement"),
        ("insert_time", "date_heure_insertion"),
    ):
        ts = pd.to_datetime(
            df[src], format="ISO8601", utc=True, errors="coerce"
        )
        out[tgt] = (
            ts.dt.tz_convert("UTC")
            .dt.tz_localize(None)
            .dt.strftime("%Y-%m-%dT%H:%M:%S")
        )

    for src, tgt in RENAME.items():
        col = df[src]
        if tgt in CODED:
            out[tgt] = normalise_code(col)
        elif tgt in INT_COLS:
            out[tgt] = (
                pd.to_numeric(col, errors="coerce").round().astype("Int64")
            )
        elif tgt in FLOAT_COLS:
            out[tgt] = pd.to_numeric(col, errors="coerce").astype("Float64")
        else:
            out[tgt] = col.astype("string").str.strip().replace("", pd.NA)

    stations = df[
        ["geo_id_wmo", "geo_id_wigos", "name", "lat", "lon", "_validity"]
    ].rename(
        columns={
            "geo_id_wmo": "indicatif_omm",
            "geo_id_wigos": "indicatif_wigos",
            "name": "nom_station",
            "lat": "latitude",
            "lon": "longitude",
        }
    )
    return out, stations


def clean_synop():
    paths = sorted(glob.glob(str(INPUT / "synop_*.csv.gz")))
    if not paths:
        raise FileNotFoundError(f"no synop_*.csv.gz under {INPUT}")

    total = 0
    seen = []
    for path in paths:
        year = Path(path).stem.split("_")[1].split(".")[0]
        rows, stations = read_synop_year(path)
        for value, part in rows.groupby("ano"):
            write_parquet(
                part,
                SYNOP_ORDER,
                OUTPUT / "synop" / f"ano={value}" / "data.parquet",
            )
        total += len(rows)
        seen.append(stations)
        print(
            f"  synop {year}: {len(rows):>7,} rows, {rows['indicatif_omm'].nunique()} stations"
        )
    print(f"  synop total: {total:,} rows")
    return pd.concat(seen, ignore_index=True), total


def clean_station_synop(observed):
    """One row per station: latest published name and position, plus the geojson metadata."""
    observed = observed.sort_values("_validity")
    latest = observed.groupby("indicatif_omm", as_index=False).last()
    span = observed.groupby("indicatif_omm")["_validity"].agg(["min", "max"])

    geo = json.loads(
        (INPUT / "postes_synop.geojson").read_text(encoding="utf-8")
    )
    meta = pd.DataFrame(
        [
            {
                "indicatif_omm": f["properties"]["Id"],
                "altitude": f["properties"].get("Altitude"),
                "date_ouverture": (
                    f["properties"].get("Date_ouverture") or ""
                )[:10]
                or None,
            }
            for f in geo["features"]
        ]
    )

    df = latest.merge(meta, on="indicatif_omm", how="left")
    df["annee_debut_observation"] = (
        df["indicatif_omm"].map(span["min"].dt.year).astype("Int64")
    )
    df["annee_fin_observation"] = (
        df["indicatif_omm"].map(span["max"].dt.year).astype("Int64")
    )
    for col in ("latitude", "longitude", "altitude"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    df["geolocalisation"] = df.apply(
        lambda r: (
            None
            if pd.isna(r["longitude"]) or pd.isna(r["latitude"])
            else f"POINT({r['longitude']} {r['latitude']})"
        ),
        axis=1,
    )
    df = df.sort_values("indicatif_omm").reset_index(drop=True)
    write_parquet(
        df, STATION_SYNOP_ORDER, OUTPUT / "station_synop" / "data.parquet"
    )
    print(
        f"  station_synop: {len(df)} rows, {df['altitude'].notna().sum()} with altitude"
    )
    return df


def clean_normales():
    stations, normals = parse_ficheclim.parse_all(str(INPUT / "ficheclim"))

    st = pd.DataFrame(stations)
    for col in ("latitude", "longitude", "altitude"):
        st[col] = pd.to_numeric(st[col], errors="coerce").astype("Float64")
    st["geolocalisation"] = st.apply(
        lambda r: (
            None
            if pd.isna(r["longitude"]) or pd.isna(r["latitude"])
            else f"POINT({r['longitude']} {r['latitude']})"
        ),
        axis=1,
    )
    st = st.sort_values("numero_poste").reset_index(drop=True)
    write_parquet(
        st,
        STATION_CLIM_ORDER,
        OUTPUT / "station_climatologique" / "data.parquet",
    )

    nm = pd.DataFrame(normals)
    nm["valeur"] = pd.to_numeric(nm["valeur"], errors="coerce").astype(
        "Float64"
    )
    for col in (
        "annee_debut_reference",
        "annee_fin_reference",
        "jour_record",
        "annee_record",
    ):
        nm[col] = pd.to_numeric(nm[col], errors="coerce").astype("Int64")
    nm = nm.sort_values(["numero_poste", "indicateur", "periode"]).reset_index(
        drop=True
    )
    write_parquet(
        nm, NORMALE_ORDER, OUTPUT / "normale_climatologique" / "data.parquet"
    )

    print(f"  station_climatologique: {len(st)} rows")
    print(
        f"  normale_climatologique: {len(nm):,} rows, "
        f"{nm['indicateur'].nunique()} indicators"
    )
    return st, nm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only", choices=["synop", "normales"], help="clean a single group"
    )
    args = parser.parse_args()

    print(f"input  {INPUT}\noutput {OUTPUT}")
    if args.only != "normales":
        observed, _total = clean_synop()
        clean_station_synop(observed)
    if args.only != "synop":
        clean_normales()


if __name__ == "__main__":
    main()
