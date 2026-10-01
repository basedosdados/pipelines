"""Check the cleaned us_bls_cex output before upload.

Reports, for every table: rows per year and on-disk size; for every column with
``covered_by_dictionary = yes``: the share of non-null values whose code appears
in ``dicionario`` (uncovered examples listed below 99%) and whether a code's
zero-padding changes across years (``1`` vs ``01``); plus three spot checks
against published BLS numbers.

Usage:
    python models/us_bls_cex/code/verify_output.py [--tables ...]
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_bls_cex.pumd_files import OUTPUT_DIR
from pipelines.datasets.us_bls_cex.utils import read_arch

TABLES = [
    "series",
    "annual",
    "ucc",
    "dicionario",
    "interview_household",
    "interview_member",
    "interview_expenditure",
    "interview_income",
    "diary_household",
    "diary_member",
    "diary_expenditure",
    "diary_income",
]


def dataset(table):
    return pads.dataset(OUTPUT_DIR / table, format="parquet")


def size_mb(table):
    return (
        sum(p.stat().st_size for p in (OUTPUT_DIR / table).rglob("*.parquet"))
        / 1e6
    )


def check_schema(table):
    arch = [a["name"] for a in read_arch(table)]
    for f in (OUTPUT_DIR / table).rglob("*.parquet"):
        s = pq.read_schema(f)
        assert s.names == arch, (
            f"{f}: column names/order differ from architecture"
        )
        assert all(str(t) == "string" for t in s.types), (
            f"{f}: non-string column"
        )


def rows_per_year(table) -> dict:
    ds = dataset(table)
    if "year" not in ds.schema.names:
        return {"all": ds.count_rows()}
    t = ds.to_table(columns=["year"])
    vc = pc.value_counts(t.column("year")).to_pylist()  # pyrefly: ignore
    return {
        int(d["values"]): d["counts"]
        for d in sorted(vc, key=lambda d: d["values"])
    }


def drift(per_year: dict[str, set]) -> list[str]:
    """Codes whose numeric value is written with different padding over time."""
    forms = defaultdict(lambda: defaultdict(list))
    for year, vals in per_year.items():
        for v in vals:
            if v.lstrip("-").isdigit():
                forms[int(v)][v].append(year)
    out = []
    for reps in forms.values():
        if len(reps) > 1:
            out.append(
                "; ".join(
                    f"{r!r} {min(y)}-{max(y)}" for r, y in sorted(reps.items())
                )
            )
    return out


def coverage(tables, dic):
    keys = (
        dic.groupby(["id_tabela", "nome_coluna"])["chave"].apply(set).to_dict()
    )
    print(
        "\n## Dictionary coverage (share of non-null values found in dicionario)"
    )
    low, drifted = [], []
    for table in tables:
        cols = [
            a["name"]
            for a in read_arch(table)
            if a["covered_by_dictionary"] == "yes"
        ]
        if not cols:
            continue
        ds = dataset(table)
        has_year = "year" in ds.schema.names
        for c in cols:
            t = ds.to_table(columns=[c] + (["year"] if has_year else []))
            if not has_year:
                t = t.append_column("year", pa.array(["all"] * t.num_rows))
            g = (
                t.filter(pc.is_valid(t.column(c)))  # pyrefly: ignore
                .group_by(["year", c])
                .aggregate([([], "count_all")])
                .to_pandas()
                .rename(columns={c: "v", "count_all": "n"})
            )
            n = int(g["n"].sum())
            known = keys.get((table, c), set())
            g["ok"] = g["v"].isin(known)
            share = g.loc[g["ok"], "n"].sum() / n if n else float("nan")
            d = drift(g.groupby("year")["v"].apply(set).to_dict())
            if d:
                drifted.append((table, c, d))
            if n and share < 1:
                bad = (
                    g[~g["ok"]]
                    .groupby("v")["n"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(8)
                )
                low.append((table, c, n, share, len(known), bad.to_dict()))
            elif not n:
                low.append((table, c, 0, float("nan"), len(known), {}))
    print(f"{len(low)} columns below 100% (or empty):")
    for table, c, n, share, nk, bad in low:
        print(
            f"  {table}.{c}: n={n:,} covered={share:.1%} dict_keys={nk} uncovered={bad}"
        )
    print(f"\n## Zero-padding drift ({len(drifted)} columns)")
    for table, c, d in drifted:
        print(
            f"  {table}.{c}: {d[:4]}{' ...' if len(d) > 4 else ''} ({len(d)} codes)"
        )


def newid_checks():
    """NEWID must stay unique per household table after unpadding."""
    print("\n## NEWID after unpadding")
    for t in ("interview_household", "diary_household"):
        ids = dataset(t).to_table(columns=["newid"]).column("newid")
        n, d = len(ids), len(pc.unique(ids))  # pyrefly: ignore
        print(f"  {t}: rows={n:,} distinct newid={d:,} duplicates={n - d}")


def spot_checks():
    print("\n## Spot checks")
    a = dataset("annual").to_table(
        filter=(pc.field("series_id") == "CXUTOTALEXPLB0101M")
        & (pc.field("year") == "2024")
    )
    print(
        "  annual 2024 CXUTOTALEXPLB0101M:",
        a.select(["mean", "standard_error"]).to_pylist(),
    )
    h = (
        dataset("interview_household")
        .to_table(
            columns=["final_weight", "quarter"],
            filter=pc.field("year") == "2024",
        )
        .to_pandas()
    )
    w = (
        h.assign(w=pd.to_numeric(h.final_weight))
        .groupby("quarter")["w"]
        .agg(["sum", "size"])
    )
    print(
        "  interview_household 2024 sum(final_weight) by quarter:\n"
        + w.to_string()
    )
    e = rows_per_year("interview_expenditure")
    print(
        f"  interview_expenditure rows: 2024={e.get(2024):,} 2025={e.get(2025):,} "
        f"sum={e.get(2024, 0) + e.get(2025, 0):,}"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", default=TABLES)
    args = ap.parse_args()
    print("## Rows per year and size")
    total = 0
    for t in args.tables:
        check_schema(t)
        r = rows_per_year(t)
        n = sum(r.values())
        total += n
        compact = ", ".join(f"{k}:{v:,}" for k, v in r.items())
        print(f"{t}: total={n:,} size={size_mb(t):.1f}MB | {compact}")
    print(f"TOTAL rows {total:,}")
    dic = dataset("dicionario").to_table().to_pandas()
    coverage([t for t in args.tables if t != "dicionario"], dic)
    if {"interview_household", "diary_household"} <= set(args.tables):
        newid_checks()
    if {"annual", "interview_household", "interview_expenditure"} <= set(
        args.tables
    ):
        spot_checks()


if __name__ == "__main__":
    main()
