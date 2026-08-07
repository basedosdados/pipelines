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
    # T2
    "relatoria",
    "votacao_comissao",
    "votacao_comissao_parlamentar",
    "discurso",
]
DIMS = [
    "senador",
    "partido",
    "bloco",
    "lideranca",
    "comissao",
    "mesa",
    # T2 — per-senator biographical (keyed by senator, not year)
    "senador_mandato",
    "senador_filiacao",
    "senador_comissao",
    "senador_cargo",
]
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


def clean_all(
    out_root: str, years: range | None = None, sample: bool = False
) -> dict:
    """Build every table under `out_root/<table>/`.

    Dimensions are always rebuilt in full. Time-series tables use `years`
    (default: full history) — pass `recent_window()` for the routine refresh.
    The per-senator T2 tables (mandato/filiacao/comissao/cargo) and the
    committee-vote tables iterate the full senator/committee lists regardless of
    `years`, since their grain is the senator/committee, not the calendar year.

    `sample=True` caps the senator and committee iteration for a fast smoke test.

    Returns a mapping `{table: out_dir}` plus `"max_data_sessao"`: the latest
    `data_sessao` in `votacao` (`YYYY-MM-DD`), which anchors the source update.
    """
    cur = pd.Timestamp.today().year
    if years is None:
        # Full history: votes/orientation start in 1991, processes in 1900,
        # relatoria in 2015, speeches in 1997 (empty earlier years return
        # nothing).
        vote_years = range(sc.VOTACAO_START, cur + 1)
        proc_years = range(sc.PROCESSO_START, cur + 1)
        relatoria_years = range(sc.RELATORIA_START, cur + 1)
        discurso_years = range(sc.DISCURSO_START, cur + 1)
    else:
        vote_years = proc_years = relatoria_years = discurso_years = years

    result: dict = {}
    # --- dimensions (T1)
    result["partido"] = write_table(out_root, "partido", sc.clean_partido())
    result["bloco"] = write_table(out_root, "bloco", sc.clean_bloco())
    result["lideranca"] = write_table(
        out_root, "lideranca", sc.clean_lideranca()
    )
    comissao_df = sc.clean_comissao()
    result["comissao"] = write_table(out_root, "comissao", comissao_df)
    result["mesa"] = write_table(out_root, "mesa", sc.clean_mesa())
    senador_df = sc.clean_senador()
    result["senador"] = write_table(out_root, "senador", senador_df)

    # --- per-senator biographical (T2); committee siglas drive committee votes
    codes = [c for c in senador_df["id_senador"].tolist() if c]
    siglas = [
        x
        for x in comissao_df["sigla_comissao"].dropna().unique().tolist()
        if x
    ]
    if sample:
        codes, siglas = codes[:15], siglas[:6]
    result["senador_mandato"] = write_table(
        out_root, "senador_mandato", sc.clean_senador_mandato(codes)
    )
    result["senador_filiacao"] = write_table(
        out_root, "senador_filiacao", sc.clean_senador_filiacao(codes)
    )
    result["senador_comissao"] = write_table(
        out_root, "senador_comissao", sc.clean_senador_comissao(codes)
    )
    result["senador_cargo"] = write_table(
        out_root, "senador_cargo", sc.clean_senador_cargo(codes)
    )

    # --- time-series (T1)
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

    # --- time-series (T2)
    result["relatoria"] = write_table(
        out_root, "relatoria", sc.clean_relatoria(relatoria_years)
    )
    cvh, cvp = sc.clean_votacao_comissao(siglas)
    result["votacao_comissao"] = write_table(out_root, "votacao_comissao", cvh)
    result["votacao_comissao_parlamentar"] = write_table(
        out_root, "votacao_comissao_parlamentar", cvp
    )
    result["discurso"] = write_table(
        out_root, "discurso", sc.clean_discurso(discurso_years)
    )

    max_ds = vdf["data_sessao"].dropna().max() if len(vdf) else None
    result["max_data_sessao"] = max_ds
    return result
