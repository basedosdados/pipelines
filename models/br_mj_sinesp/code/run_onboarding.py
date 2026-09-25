"""One-shot onboarding: download every year, clean, report coverage.

    python run_onboarding.py            # all years
    python run_onboarding.py 2025 2026  # a subset

Scratch data lives under $SINESP_DATA_DIR (default ~/Downloads/br_mj_sinesp_data)
and is deleted at the end of the onboarding run; it is fully reproducible.
"""

from __future__ import annotations

import json
import os
import sys

from constants import DATA_DIR, INPUT_DIR, OUTPUT_DIR
from utils import available_years, clean_all, download_year


def main(years: list[int] | None = None) -> None:
    years = years or available_years()
    print(f"data dir: {DATA_DIR}")
    for y in years:
        path = download_year(y)
        print(
            f"  downloaded {os.path.basename(path)} "
            f"({os.path.getsize(path) / 1e6:.1f} MB)",
            flush=True,
        )
    print("\ncleaning:")
    stats = clean_all(years, input_dir=INPUT_DIR, out_dir=OUTPUT_DIR)
    with open(
        os.path.join(DATA_DIR, "clean_stats.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(stats, f, ensure_ascii=False, indent=1)
    tot_m = sum(s["municipio_rows"] for s in stats)
    tot_u = sum(s["uf_rows"] for s in stats)
    tot_f = sum(s["flagged_rows"] for s in stats)
    print(f"\nmunicipio_mes {tot_m:,} rows ({tot_f:,} flagged nao_reportado)")
    print(f"uf_mes        {tot_u:,} rows")


if __name__ == "__main__":
    main([int(a) for a in sys.argv[1:]] or None)
