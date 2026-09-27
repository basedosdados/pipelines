"""Validate the cleaned parquet before upload: keys, coverage, sparsity, dictionary.

    uv run python models/fr_meteofrance/code/validate.py

**Exits non-zero when a check fails.** Every check used to print and return, so a
run that found 12,000 duplicate keys was indistinguishable from a clean one by
exit status — and this script is the gate before uploading to BigQuery. Sparsity
is reported but never fails: columns legitimately under 5% non-null are expected
here and are declared in `schema.yml`'s `ignore_values`.
"""

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


def synop() -> tuple[dict, int]:
    """Report SYNOP shape and return ``(coded values seen, failure count)``."""
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
        k = pd.MultiIndex.from_frame(df[["date", "heure", "indicatif_omm"]])
        dup += len(k) - k.nunique()
        keys.update(df["indicatif_omm"].unique())
        for c in CODED:
            codes[c].update(df[c].dropna().unique())
        lo, hi = df["date"].min(), df["date"].max()
        dmin = lo if dmin is None else min(dmin, lo)
        dmax = hi if dmax is None else max(dmax, hi)

    print(f"  duplicate (date, heure, indicatif_omm) keys: {dup}")
    print(f"  stations: {len(keys)}   coverage: {dmin} .. {dmax}")
    sparse = sorted(c for c, n in nonnull.items() if n / total < 0.05)
    print(f"  columns under 5% non-null ({len(sparse)}):")
    for c in sparse:
        print(f"    {c:38s} {nonnull[c] / total:6.4%}")
    return codes, dup


def dictionary(codes) -> int:
    """Report dictionary coverage and return the number of uncovered values."""
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
            missing += len(gap)
            print(f"  UNCOVERED normale_climatologique.{col}: {sorted(gap)}")
    return missing


def others() -> int:
    """Report the register and normals tables; return the failure count."""
    failures = 0
    st = pd.read_parquet(OUTPUT / "station_synop" / "data.parquet")
    failures += int(st["indicatif_omm"].duplicated().sum())
    print(
        f"\nstation_synop: {len(st)} rows, "
        f"{st['indicatif_omm'].duplicated().sum()} duplicate keys, "
        f"{st['geolocalisation'].notna().sum()} with geometry"
    )

    sc = pd.read_parquet(OUTPUT / "station_climatologique" / "data.parquet")
    failures += int(sc["numero_poste"].duplicated().sum())
    print(
        f"station_climatologique: {len(sc)} rows, "
        f"{sc['numero_poste'].duplicated().sum()} duplicate keys"
    )

    nm = pd.read_parquet(OUTPUT / "normale_climatologique" / "data.parquet")
    key = nm[["numero_poste", "indicateur", "periode"]]
    failures += len(key) - len(key.drop_duplicates())
    print(
        f"normale_climatologique: {len(nm):,} rows, "
        f"{len(key) - len(key.drop_duplicates()):,} duplicate keys, "
        f"{nm['valeur'].isna().mean():.3%} null valeur"
    )
    orphans = set(nm["numero_poste"]) - set(sc["numero_poste"])
    print(f"  normals with no station row: {len(orphans)}")
    return failures + len(orphans)


if __name__ == "__main__":
    codes, failures = synop()
    failures += others()
    failures += dictionary(codes)
    if failures:
        print(f"\nVALIDATION FAILED: {failures} problems")
        raise SystemExit(1)
    print("\nvalidation OK")
