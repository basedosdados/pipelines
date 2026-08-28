"""Clean the *Données climatologiques de base* archives into partitioned parquet.

    uv run python models/fr_meteofrance/code/clim_clean.py [--only quot|mens]

Produces, under ``$MFC_OUTPUT`` (default ``~/Downloads/fr_meteofrance_clim/output``):

* ``quotidienne/<dept>_<period>.parquet``  poste x day
* ``mensuelle/<dept>_<period>.parquet``    poste x month
* ``poste/data.parquet``                                the station register

The daily series ships as two files per département and period — ``RR-T-Vent``
and ``autres-parametres`` — sharing the ``(NUM_POSTE, AAAAMMJJ)`` key. They are
**outer**-joined: 6,833 keys in département 01 alone exist only in the
autres-parametres side, so a left join would silently drop rows.

Station attributes (name, latitude, longitude, altitude) are lifted out of the
fact tables into ``poste`` rather than repeated on every one of ~230 million
rows. A station can move, so the register keeps the most recent non-null value.

Output is all-STRING per house convention; the dbt models ``safe_cast`` each
column to its architecture type.
"""

import argparse
import gzip
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import clim_schema as cs

INPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_INPUT", "~/Downloads/fr_meteofrance_clim/input")
    )
)
OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_OUTPUT", "~/Downloads/fr_meteofrance_clim/output")
    )
)

RE_QUOT = re.compile(
    r"^Q_(?P<dep>[0-9A-Z]+)_(?P<period>.+)_(?P<kind>RR-T-Vent|autres-parametres)\.csv\.gz$"
)
RE_MENS = re.compile(r"^MENSQ_(?P<dep>[0-9A-Z]+)_(?P<period>.+)\.csv\.gz$")

LEADING = ["annee", "mois", "date", "numero_poste"]
POSTE_COLS = [
    "numero_poste",
    "nom_poste",
    "id_departement",
    "latitude",
    "longitude",
    "altitude",
]


def header(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.readline().strip().split(";")


def to_string_table(df: pd.DataFrame, order: list[str]) -> pa.Table:
    table = pa.Table.from_pandas(df[order], preserve_index=False)
    return table.cast(pa.schema([(c, pa.string()) for c in order]))


def typed(series: pd.Series, bq_type: str) -> pd.Series:
    if bq_type == "INT64":
        return pd.to_numeric(series, errors="coerce").round().astype("Int64")
    if bq_type == "FLOAT64":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    s = series.astype("string").str.strip()
    return s.mask(s.eq(""))


def shape(df: pd.DataFrame, spec, date_col: str) -> pd.DataFrame:
    """Rename, type and add the temporal scaffolding for one source frame."""
    raw = df[date_col].astype("string").str.strip()
    out = pd.DataFrame(index=df.index)
    out["annee"] = pd.to_numeric(raw.str[:4], errors="coerce").astype("Int64")
    out["mois"] = pd.to_numeric(raw.str[4:6], errors="coerce").astype("Int64")
    if date_col == "AAAAMMJJ":
        out["date"] = raw.str[:4] + "-" + raw.str[4:6] + "-" + raw.str[6:8]
    out["numero_poste"] = df["NUM_POSTE"].astype("string").str.strip()
    for target, bq_type, _unit, _dict, _pt, _en, _es, source in spec:
        out[target] = (
            typed(df[source], bq_type) if source in df.columns else pd.NA
        )
    return out


def write_group(
    df: pd.DataFrame, order: list[str], root: Path, stem: str
) -> int:
    """One parquet per (département, period), matching the source's own layout.

    Deliberately NOT hive-partitioned by year. These series span ~175 years, so
    partitioning the staging files by year would emit tens of thousands of tiny
    parquet files — slow to write, slow for BigQuery to read, and pointless:
    the dbt model does a full scan of staging and produces the year-partitioned
    table itself. Keeping the source's dept x period unit also makes the
    incremental refresh natural, since Météo-France only rewrites the
    ``latest-<years>`` files.
    """
    if df.empty:
        return 0
    path = root / f"{stem}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")
    return len(df)


def collect_poste(df: pd.DataFrame, dep: str, sink: dict) -> None:
    """Keep the most recent non-null station attributes seen for each poste."""
    cols = ["NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI"]
    have = [c for c in cols if c in df.columns]
    sub = (
        df[have]
        .dropna(subset=["NUM_POSTE"])
        .drop_duplicates(subset=["NUM_POSTE"], keep="last")
    )
    for row in sub.itertuples(index=False):
        d = dict(zip(have, row, strict=True))
        sink[d["NUM_POSTE"]] = {
            "numero_poste": d["NUM_POSTE"],
            "nom_poste": d.get("NOM_USUEL"),
            "id_departement": dep,
            "latitude": d.get("LAT"),
            "longitude": d.get("LON"),
            "altitude": d.get("ALTI"),
        }


def clean_quot(descriptors, sink: dict) -> int:
    files = sorted((INPUT / "quot").glob("*.csv.gz"))
    groups = defaultdict(dict)
    for f in files:
        m = RE_QUOT.match(f.name)
        if not m:
            raise ValueError(f"unrecognised file name: {f.name}")
        groups[(m.group("dep"), m.group("period"))][m.group("kind")] = f

    spec = cs.expand(
        header(next(iter(groups.values()))["RR-T-Vent"])
        + [
            c
            for c in header(
                next(
                    f
                    for g in groups.values()
                    if "autres-parametres" in g
                    for f in [g["autres-parametres"]]
                )
            )
        ],
        cs.QUOT_PARAMS,
        cs.QUOT_FLAGS,
        descriptors,
    )
    seen, order = set(), []
    for row in spec:
        if row[0] not in seen:
            seen.add(row[0])
            order.append(row)
    columns = LEADING + [r[0] for r in order]

    total = 0
    for i, ((dep, period), parts) in enumerate(sorted(groups.items()), 1):
        frames = []
        for kind in ("RR-T-Vent", "autres-parametres"):
            if kind not in parts:
                continue
            df = pd.read_csv(
                parts[kind],
                sep=";",
                dtype=str,
                low_memory=False,
                na_values=[""],
            )
            collect_poste(df, dep, sink)
            frames.append(df.set_index(["NUM_POSTE", "AAAAMMJJ"]))
        if not frames:
            continue
        joined = frames[0]
        for extra in frames[1:]:
            dup = [c for c in extra.columns if c in joined.columns]
            joined = joined.join(extra.drop(columns=dup), how="outer")
        joined = joined.reset_index()
        shaped = shape(joined, order, "AAAAMMJJ")
        total += write_group(
            shaped, columns, OUTPUT / "quotidienne", f"{dep}_{period}"
        )
        print(
            f"  [{i}/{len(groups)}] quot {dep} {period}: {len(shaped):,} rows",
            flush=True,
        )
    return total


def clean_mens(descriptors, sink: dict) -> int:
    files = sorted((INPUT / "mens").glob("*.csv.gz"))
    spec = cs.expand(header(files[0]), cs.MENS_PARAMS, {}, descriptors)
    columns = ["annee", "mois", "numero_poste"] + [r[0] for r in spec]

    total = 0
    for i, f in enumerate(files, 1):
        m = RE_MENS.match(f.name)
        if not m:
            raise ValueError(f"unrecognised file name: {f.name}")
        df = pd.read_csv(
            f, sep=";", dtype=str, low_memory=False, na_values=[""]
        )
        collect_poste(df, m.group("dep"), sink)
        shaped = shape(df, spec, "AAAAMM")
        total += write_group(
            shaped,
            columns,
            OUTPUT / "mensuelle",
            f"{m.group('dep')}_{m.group('period')}",
        )
        if i % 25 == 0:
            print(
                f"  [{i}/{len(files)}] mens {f.name}: {len(shaped):,} rows",
                flush=True,
            )
    return total


def write_poste(sink: dict) -> int:
    df = pd.DataFrame(sink.values())
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
    df = df.sort_values("numero_poste").reset_index(drop=True)
    order = [*POSTE_COLS, "geolocalisation"]
    path = OUTPUT / "poste" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", choices=["quot", "mens"])
    args = parser.parse_args()
    descriptors = cs.descriptors()
    sink: dict = {}
    print(f"input {INPUT}\noutput {OUTPUT}")
    if args.only in (None, "mens"):
        print("mensuelle:", f"{clean_mens(descriptors, sink):,} rows")
    if args.only in (None, "quot"):
        print("quotidienne:", f"{clean_quot(descriptors, sink):,} rows")
    print("poste:", f"{write_poste(sink):,} rows")


if __name__ == "__main__":
    main()
