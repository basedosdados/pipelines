"""Validate the cleaned parquet before upload: keys, coverage, sparsity, dictionary."""

import os
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from schema_map import SYNOP_COLUMNS

OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MF_OUTPUT", "~/Downloads/fr_meteofrance_data/output")
    )
)
CODED = [tgt for _s, tgt, _t, _u, is_dict, _d in SYNOP_COLUMNS if is_dict]


def synop():
    files = sorted((OUTPUT / "synop").rglob("*.parquet"))
    total = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"synop: {len(files)} partitions, {total:,} rows")

    nonnull = Counter()
    keys = set()
    dup = 0
    codes = {c: set() for c in CODED}
    dmin = dmax = None
    for f in files:
        df = pd.read_parquet(f)
        for col in df.columns:
            nonnull[col] += df[col].notna().sum()
        k = pd.MultiIndex.from_frame(df[["data", "hora", "indicatif_omm"]])
        dup += len(k) - k.nunique()
        keys.update(df["indicatif_omm"].unique())
        for c in CODED:
            codes[c].update(df[c].dropna().unique())
        lo, hi = df["data"].min(), df["data"].max()
        dmin = lo if dmin is None else min(dmin, lo)
        dmax = hi if dmax is None else max(dmax, hi)

    print(f"  duplicate (data, hora, indicatif_omm) keys: {dup}")
    print(f"  stations: {len(keys)}   coverage: {dmin} .. {dmax}")
    sparse = sorted(c for c, n in nonnull.items() if n / total < 0.05)
    print(f"  columns under 5% non-null ({len(sparse)}):")
    for c in sparse:
        print(f"    {c:38s} {nonnull[c] / total:6.4%}")
    return codes


def dictionary(codes):
    d = pd.read_parquet(OUTPUT / "dicionario" / "data.parquet")
    print(f"\ndicionario: {len(d):,} rows")
    missing = 0
    for col, values in codes.items():
        known = set(
            d.loc[(d.id_tabela == "synop") & (d.nome_coluna == col), "chave"]
        )
        gap = values - known
        if gap:
            missing += len(gap)
            print(f"  UNCOVERED {col}: {sorted(gap)}")
    print(f"  uncovered synop code values: {missing}")

    nm = pd.read_parquet(OUTPUT / "normale_climatologique" / "data.parquet")
    for col in ("indicateur", "periode", "unite"):
        known = set(
            d.loc[
                (d.id_tabela == "normale_climatologique")
                & (d.nome_coluna == col),
                "chave",
            ]
        )
        gap = set(nm[col].dropna()) - known
        if gap:
            print(f"  UNCOVERED normale_climatologique.{col}: {sorted(gap)}")


def others():
    st = pd.read_parquet(OUTPUT / "station_synop" / "data.parquet")
    print(
        f"\nstation_synop: {len(st)} rows, "
        f"{st['indicatif_omm'].duplicated().sum()} duplicate keys, "
        f"{st['geolocalisation'].notna().sum()} with geometry"
    )

    sc = pd.read_parquet(OUTPUT / "station_climatologique" / "data.parquet")
    print(
        f"station_climatologique: {len(sc)} rows, "
        f"{sc['numero_poste'].duplicated().sum()} duplicate keys"
    )

    nm = pd.read_parquet(OUTPUT / "normale_climatologique" / "data.parquet")
    key = nm[["numero_poste", "indicateur", "periode"]]
    print(
        f"normale_climatologique: {len(nm):,} rows, "
        f"{len(key) - len(key.drop_duplicates()):,} duplicate keys, "
        f"{nm['valeur'].isna().mean():.3%} null valeur"
    )
    orphans = set(nm["numero_poste"]) - set(sc["numero_poste"])
    print(f"  normals with no station row: {len(orphans)}")


if __name__ == "__main__":
    codes = synop()
    others()
    dictionary(codes)
