"""
Pure helpers for the br_senado_dados_abertos pipeline (no Prefect imports).

Builds the same all-STRING partitioned parquet the one-shot onboarding produces,
reusing the cleaning transform in `senado_clean`. Shared by the recurring
pipeline (`tasks.py`) and the onboarding bootstrap (`models/.../code/`).
"""

from __future__ import annotations

import os
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos import senado_clean as sc

# Year-partitioned (time-series) vs. dimension (single-file) tables.
PARTITIONED = [
    "votacao",
    "votacao_parlamentar",
    "votacao_orientacao_bancada",
    "processo",
]
DIMS = ["senador", "partido", "bloco", "lideranca", "comissao", "mesa"]
ALL_TABLES = DIMS + PARTITIONED


def _write_string_parquet(df: pd.DataFrame, path: str) -> None:
    """Write `df` as a single all-STRING snappy parquet at `path`."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in df.columns])
    tbl = pa.Table.from_pandas(
        df.astype(object), schema=schema, preserve_index=False
    )
    pq.write_table(tbl, path, compression="snappy")


def write_table(out_root: str, name: str, df: pd.DataFrame) -> str:
    """Write one table under `out_root/<name>/`, hive-partitioned by `ano` when
    it is a time-series table; a single `data.parquet` otherwise. Returns the
    table directory. Partition files are always named `data.parquet` so a
    re-upload replaces the partition rather than accreting duplicates."""
    out_dir = os.path.join(out_root, name)
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
    return out_dir


def recent_window(prior_years: int = 1) -> range:
    """Year range to refresh for the time-series tables on a routine run:
    the current year plus `prior_years` earlier years (to pick up late edits)."""
    cur = pd.Timestamp.today().year
    return range(cur - prior_years, cur + 1)


def clean_all(out_root: str, years: range | None = None) -> dict:
    """Build every table under `out_root/<table>/`.

    Dimensions are always rebuilt in full. Time-series tables use `years`
    (default: full history) — pass `recent_window()` for the routine refresh.

    Returns a mapping `{table: out_dir}` plus `"max_data_sessao"`: the latest
    `data_sessao` in `votacao` (`YYYY-MM-DD`), which anchors the source update.
    """
    if years is None:
        # Full history: votes/orientation start in 1991, processes in 1946
        # (empty earlier years just return nothing).
        cur = pd.Timestamp.today().year
        vote_years = range(sc.VOTACAO_START, cur + 1)
        proc_years = range(sc.PROCESSO_START, cur + 1)
    else:
        vote_years = years
        proc_years = years

    result: dict = {}
    result["partido"] = write_table(out_root, "partido", sc.clean_partido())
    result["bloco"] = write_table(out_root, "bloco", sc.clean_bloco())
    result["lideranca"] = write_table(
        out_root, "lideranca", sc.clean_lideranca()
    )
    result["comissao"] = write_table(out_root, "comissao", sc.clean_comissao())
    result["mesa"] = write_table(out_root, "mesa", sc.clean_mesa())
    result["senador"] = write_table(out_root, "senador", sc.clean_senador())

    vdf, pdf = sc.clean_votacao(vote_years)
    result["votacao"] = write_table(out_root, "votacao", vdf)
    result["votacao_parlamentar"] = write_table(
        out_root, "votacao_parlamentar", pdf
    )
    result["votacao_orientacao_bancada"] = write_table(
        out_root, "votacao_orientacao_bancada", sc.clean_orientacao(vote_years)
    )
    result["processo"] = write_table(
        out_root, "processo", sc.clean_processo(proc_years)
    )

    max_ds = vdf["data_sessao"].dropna().max() if len(vdf) else None
    result["max_data_sessao"] = max_ds
    return result
