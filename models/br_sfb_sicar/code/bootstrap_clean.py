"""
One-shot onboarding bootstrap: clean downloaded CAR zips -> partitioned parquet.

Reads every <UF>_<THEME>.zip under INPUT, groups by output table, writes an
all-string hive-partitioned parquet dataset per table under OUTPUT.
Snapshot (data) per UF comes from the SICAR release-dates page (dd/mm/yyyy).
"""

import glob
import os
from datetime import datetime
from pathlib import Path

import architecture as A  # noqa: N812
import clean as C  # noqa: N812
import pandas as pd

SCRATCH = Path(
    os.environ.get(
        "CAR_DATA", str(Path.home() / "Downloads" / "br_sfb_sicar_data")
    )
)
INPUT = SCRATCH / "input"
OUTPUT = SCRATCH / "output"

POLY_TO_TABLE = {v: k for k, v in A.THEME_POLYGON.items()}


def load_release_dates():
    """Return {UF: 'YYYY-MM-DD'} from the SICAR release page (cached to file)."""
    cache = SCRATCH / "release_dates.csv"
    if cache.exists():
        d = pd.read_csv(cache, dtype=str)
        return dict(zip(d["uf"], d["data"], strict=False))
    from SICAR import Sicar

    rel = Sicar().get_release_dates()
    out = {
        s.value: datetime.strptime(v, "%d/%m/%Y").date().isoformat()
        for s, v in rel.items()
    }
    pd.DataFrame({"uf": list(out), "data": list(out.values())}).to_csv(
        cache, index=False
    )
    return out


def main(only_ufs=None):
    rel = load_release_dates()
    zips = sorted(glob.glob(str(INPUT / "*.zip")))
    per_table = {}  # table -> list[df]
    for zf in zips:
        base = os.path.basename(zf).replace(".zip", "")
        uf, poly = base.split("_", 1)
        if only_ufs and uf not in only_ufs:
            continue
        table = POLY_TO_TABLE.get(poly)
        if table is None:
            print("skip unknown theme:", poly)
            continue
        snap = rel.get(uf)
        if not snap:
            print("no release date for", uf, "- skipping")
            continue
        print(
            f"cleaning {uf} {poly} -> table {table} (snapshot {snap})",
            flush=True,
        )
        gdf = C.read_theme_zip(zf)
        gdf, dropped = C.filter_to_uf(gdf, uf)
        df = C.build_table_df(table, gdf, snap, uf)
        per_table.setdefault(table, []).append(df)
        print(
            f"   rows={len(df)}  null_geom={df['geometria'].isna().sum()}  "
            f"dropped_foreign_uf={dropped}",
            flush=True,
        )
        del gdf

    print("\n=== writing partitioned parquet ===", flush=True)
    import shutil

    for table, frames in per_table.items():
        root = os.path.join(str(OUTPUT), table)
        if os.path.isdir(root):
            shutil.rmtree(root)  # clear stale partitions before rewrite
        df = pd.concat(frames, ignore_index=True)
        C.write_table_partitioned(df, str(OUTPUT), table)
        print(f"{table}: {len(df)} rows -> {root}", flush=True)


if __name__ == "__main__":
    import sys

    ufs = sys.argv[1].split(",") if len(sys.argv) > 1 else None
    main(only_ufs=ufs)
