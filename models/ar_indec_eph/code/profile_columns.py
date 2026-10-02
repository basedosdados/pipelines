"""Profile every column's real values, so BigQuery types follow the data.

Samples a spread of waves across all four source eras and records, per column:
integer-like / decimal / non-numeric counts, observed min and max, distinct
count, and a few example values. Written to column_profile.json and used by
build_architecture.py to choose INT64 / FLOAT64 / STRING.
"""

import json
import re
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path

import pandas as pd

from models.ar_indec_eph.code.archives import (
    blank_to_na,
    data_members,
    extract,
    read_dta,
)
from models.ar_indec_eph.code.constants import CODE_DIR, TABLES, waves

# A spread across the eras: early Stata, late Stata, RAR, 2016 transition,
# post-2016 TXT, the 2023 Q4 redesign, and the latest wave.
SAMPLE = [
    (2003, 3),
    (2006, 2),
    (2009, 4),
    (2013, 1),
    (2015, 1),
    (2016, 2),
    (2018, 3),
    (2021, 3),
    (2023, 3),
    (2023, 4),
    (2025, 2),
    (2026, 1),
]
INT_RE = re.compile(r"^-?\d+$")
DEC_RE = re.compile(r"^-?\d+[.,]\d+$")


def norm(series: pd.Series) -> pd.Series:
    return blank_to_na(series.astype("string"))


def main() -> int:
    stats: dict[str, dict[str, dict]] = {
        t: defaultdict(
            lambda: {
                "n": 0,
                "null": 0,
                "int": 0,
                "dec": 0,
                "other": 0,
                "min": None,
                "max": None,
                "examples": [],
                "distinct": set(),
            }
        )
        for t in TABLES
    }

    by_key = {(w["year"], w["quarter"]): w for w in waves()}
    for key in SAMPLE:
        wave = by_key[key]
        tmp = Path(tempfile.mkdtemp(prefix="eph_prof_"))
        try:
            for table, member in data_members(wave).items():
                path = extract(wave, member, tmp)
                if wave["fmt"] == "dta":
                    df, _meta = read_dta(path)
                else:
                    df = pd.read_csv(
                        path,
                        sep=";",
                        encoding="latin-1",
                        dtype=str,
                        low_memory=False,
                    )
                    df.columns = [
                        str(c).strip().strip('"').upper() for c in df.columns
                    ]
                for col in df.columns:
                    if not col or col.startswith("UNNAMED"):
                        continue
                    s = norm(df[col])
                    st = stats[table][col]
                    st["n"] += len(s)
                    st["null"] += int(s.isna().sum())
                    nn = s.dropna()
                    if nn.empty:
                        continue
                    is_int = nn.str.match(INT_RE)
                    is_dec = nn.str.match(DEC_RE)
                    st["int"] += int(is_int.sum())
                    st["dec"] += int(is_dec.sum())
                    st["other"] += int((~is_int & ~is_dec).sum())
                    numeric = pd.to_numeric(
                        nn[is_int | is_dec].str.replace(",", ".", regex=False),
                        errors="coerce",
                    ).dropna()
                    if not numeric.empty:
                        lo, hi = float(numeric.min()), float(numeric.max())
                        st["min"] = (
                            lo if st["min"] is None else min(st["min"], lo)
                        )
                        st["max"] = (
                            hi if st["max"] is None else max(st["max"], hi)
                        )
                    if len(st["distinct"]) < 300:
                        st["distinct"].update(nn.unique()[:300])
                    if len(st["examples"]) < 6:
                        st["examples"] += [
                            v
                            for v in nn.unique()[:6]
                            if v not in st["examples"]
                        ][: 6 - len(st["examples"])]
                path.unlink(missing_ok=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(f"profiled {key[0]}Q{key[1]}", flush=True)

    out: dict[str, dict] = {}
    for table in TABLES:
        out[table] = {}
        for col, st in stats[table].items():
            d = dict(st)
            d["distinct"] = len(st["distinct"])
            d["examples"] = [str(e)[:40] for e in st["examples"]]
            out[table][col] = d
    (CODE_DIR / "column_profile.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    for table in TABLES:
        print(f"\n{table}: profiled {len(out[table])} columns")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
