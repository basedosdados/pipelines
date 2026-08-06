"""
Build partitioned parquet for br_senado_dados_abertos T1 tables.

  uv run python run_onboarding.py --sample        # recent slice, fast validation
  uv run python run_onboarding.py --full          # full history
  uv run python run_onboarding.py --tables votacao,processo --full

All-STRING parquet (staging convention). Partitioned tables → output/<t>/ano=<y>/data.parquet
Dimension tables → output/<t>/data.parquet
"""

from __future__ import annotations

import argparse
import os
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import senado_clean as sc

OUTPUT = os.path.join(os.path.dirname(__file__), "output")

PARTITIONED = {
    "votacao",
    "votacao_parlamentar",
    "votacao_orientacao_bancada",
    "processo",
}
DIMS = {"senador", "partido", "bloco", "lideranca", "comissao", "mesa"}
ALL_TABLES = list(DIMS) + list(PARTITIONED)


def _write_string_parquet(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    tbl = pa.Table.from_pandas(
        df.astype(object), schema=schema, preserve_index=False
    )
    pq.write_table(tbl, path, compression="snappy")


def write_table(name: str, df: pd.DataFrame) -> int:
    out_dir = os.path.join(OUTPUT, name)
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    if name in PARTITIONED:
        df = df[df["ano"].notna()]
        cols = [c for c in df.columns if c != "ano"]
        for ano, g in df.groupby("ano"):
            _write_string_parquet(
                g[cols], os.path.join(out_dir, f"ano={ano}", "data.parquet")
            )
    else:
        _write_string_parquet(df, os.path.join(out_dir, "data.parquet"))
    print(f"  -> wrote {name}: {len(df)} rows")
    return len(df)


def build(tables: list[str], sample: bool) -> None:
    if sample:
        vy, oy, py = range(2024, 2025), range(2024, 2025), range(2024, 2025)
    else:
        cy = pd.Timestamp.today().year
        vy, oy, py = (
            range(sc.VOTACAO_START, cy + 1),
            range(sc.ORIENT_START, cy + 1),
            range(sc.PROCESSO_START, cy + 1),
        )

    counts = {}
    if "partido" in tables:
        counts["partido"] = write_table("partido", sc.clean_partido())
    if "bloco" in tables:
        counts["bloco"] = write_table("bloco", sc.clean_bloco())
    if "lideranca" in tables:
        counts["lideranca"] = write_table("lideranca", sc.clean_lideranca())
    if "comissao" in tables:
        counts["comissao"] = write_table("comissao", sc.clean_comissao())
    if "mesa" in tables:
        counts["mesa"] = write_table("mesa", sc.clean_mesa())
    if "senador" in tables:
        counts["senador"] = write_table("senador", sc.clean_senador())
    if "votacao" in tables or "votacao_parlamentar" in tables:
        vdf, pdf = sc.clean_votacao(vy)
        if "votacao" in tables:
            counts["votacao"] = write_table("votacao", vdf)
        if "votacao_parlamentar" in tables:
            counts["votacao_parlamentar"] = write_table(
                "votacao_parlamentar", pdf
            )
    if "votacao_orientacao_bancada" in tables:
        counts["votacao_orientacao_bancada"] = write_table(
            "votacao_orientacao_bancada", sc.clean_orientacao(oy)
        )
    if "processo" in tables:
        counts["processo"] = write_table("processo", sc.clean_processo(py))

    print("\n=== SUMMARY ===")
    for k, v in counts.items():
        print(f"  {k:32} {v:>8} rows")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true")
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--tables", default=",".join(ALL_TABLES))
    args = ap.parse_args()
    tabs = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = sorted(set(tabs) - set(ALL_TABLES))
    if unknown:
        ap.error(f"unknown table(s): {', '.join(unknown)}")
    build(tabs, sample=not args.full)
