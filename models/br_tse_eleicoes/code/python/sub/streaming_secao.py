"""Streaming, memory-bounded build of the three giant seção families.

`resultados_candidato_secao`, `resultados_partido_secao`, and
`perfil_eleitorado_secao` reach 60-75M rows/year and OOM the in-RAM
builders on a 16 GB host (a single SP file is a 5.5 GB CSV). This module
rebuilds them with `(ano, uf)` as the unit of work, chunking within the
biggest UFs, and writes Hive-partitioned parquet
(`<out_root>/<table>/ano=YYYY/sigla_uf=UF/data.parquet`).

Correctness: the per-row transforms and per-UF aggregations are the SAME
functions the in-RAM builders use (`sub/results_section.py`,
`sub/voter_profile_section.py`) - this module only changes *how much is
held in RAM at once*, never the logic. Candidato dedup (`keep="first"`
across UFs) is reproduced with a running hash set in UFS order; partido
and perfil aggregations are group-sums, which are associative and so
chunk-safe. Validated against the in-RAM build (which is byte-identical to
the March reference) - see `validate_stream_year`.

Run: ``TSE_DATA_DIR=... uv run python -m sub.streaming_secao <ano> [out_root]``
"""

from __future__ import annotations

import gc
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from config import INPUT_DIR, STREAM_SECAO_ROOT
from utils.helpers import iter_raw_csv_chunks, read_raw_csv

from sub import results_section as rsec
from sub import voter_profile_section as vps

CHUNK = 2_000_000
# CSV larger than this is read in chunks; smaller UFs are read whole.
BIG_BYTES = 1_500_000_000


def _size(base: Path) -> int:
    for suf in (".txt", ".csv"):
        p = base.with_suffix(suf)
        if p.exists():
            return p.stat().st_size
    return 0


def _write_partition(df: pd.DataFrame, out_root: Path, table: str, ano, uf):
    d = out_root / table / f"ano={ano}" / f"sigla_uf={uf}"
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "data.parquet", index=False)


def _hash_keys(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    # 64-bit hash of the dup-key tuple per row; stringified for a stable,
    # dtype-independent key. Collisions would show up as a row-count gap in
    # validate_stream_year, so they are detectable, not silent.
    return pd.util.hash_pandas_object(
        df[keys].astype("string").fillna(""), index=False
    )


# ---------------------------------------------------------------------------
# resultados_candidato_secao + resultados_partido_secao
# ---------------------------------------------------------------------------


class _PartitionRouter:
    """Appends rows to per-``sigla_uf`` parquet partitions, keeping one
    ParquetWriter open per partition across the whole year.

    Rows are routed by their DATA ``sigla_uf`` value, not the source-file
    name — the consolidated BR file (federal years) carries presidential
    rows for every state, exactly as the in-RAM build relies on
    ``save_partitioned`` to place them.
    """

    def __init__(self, out_root: Path, table: str, ano: int):
        self.dir = out_root / table
        self.ano = ano
        self.writers: dict[str, pq.ParquetWriter] = {}

    def write(self, df: pd.DataFrame) -> None:
        if not len(df):
            return
        for uf_val, sub in df.groupby("sigla_uf", dropna=False, sort=False):
            if not len(sub):
                continue
            table = pa.Table.from_pandas(sub, preserve_index=False)
            w = self.writers.get(uf_val)
            if w is None:
                d = self.dir / f"ano={self.ano}" / f"sigla_uf={uf_val}"
                d.mkdir(parents=True, exist_ok=True)
                w = pq.ParquetWriter(d / "data.parquet", table.schema)
                self.writers[uf_val] = w
            w.write_table(table)

    def close(self) -> None:
        for w in self.writers.values():
            w.close()


def stream_resultados_secao(ano: int, out_root: Path) -> None:
    mun_uf = rsec._load_mun_uf()
    seen: set[int] = set()  # candidato dup-key hashes, global for this year
    cand_router = _PartitionRouter(out_root, "resultados_candidato_secao", ano)
    part_router = _PartitionRouter(out_root, "resultados_partido_secao", ano)

    for uf in rsec.UFS[ano]:
        base = (
            INPUT_DIR
            / f"votacao_secao/votacao_secao_{ano}_{uf}"
            / f"votacao_secao_{ano}_{uf}"
        )
        big = _size(base) > BIG_BYTES
        frames = (
            iter_raw_csv_chunks(str(base), chunksize=CHUNK)
            if big
            else [read_raw_csv(str(base))]
        )

        nom_parts: list[pd.DataFrame] = []
        leg_parts: list[pd.DataFrame] = []
        for raw in frames:
            df = rsec.clean_secao_frame(raw, ano, uf, mun_uf)
            cand, nominais, legenda = rsec.split_secao_frame(df)

            # candidato: global keep-first dedup, then route by data sigla_uf
            h = _hash_keys(cand, rsec.DUP_KEYS)
            mask = (~h.duplicated(keep="first")) & (~h.isin(seen))
            cand = cand[mask.to_numpy()]
            seen.update(h[mask.to_numpy()].tolist())
            cand = cand[[c for c in rsec.CAND_COLS if c in cand.columns]]
            cand_router.write(cand)

            # partido: reduce each chunk to group-sums so RAM stays bounded
            grp = [c for c in rsec._GROUP_COLS if c in nominais.columns]
            nom_parts.append(
                nominais.groupby(grp, as_index=False, dropna=False)[
                    "votos"
                ].sum()
            )
            lgrp = [c for c in rsec._GROUP_COLS if c in legenda.columns]
            leg_parts.append(
                legenda.groupby(lgrp, as_index=False, dropna=False)[
                    "votos_legenda"
                ].sum()
            )
            del raw, df, cand, nominais, legenda, h, mask
            gc.collect()

        # finalize partido for this source file, then route by data sigla_uf
        partido = _finalize_partido_from_partials(
            pd.concat(nom_parts, ignore_index=True),
            pd.concat(leg_parts, ignore_index=True),
        )
        partido = partido[[c for c in rsec.PART_COLS if c in partido.columns]]
        part_router.write(partido)
        del nom_parts, leg_parts, partido
        gc.collect()
        print(f"    {uf} done", flush=True)

    cand_router.close()
    part_router.close()


def _finalize_partido_from_partials(
    nominais_partial: pd.DataFrame, legenda_partial: pd.DataFrame
) -> pd.DataFrame:
    """Merge pre-aggregated nominais/legenda partials into partido rows.

    Equivalent to ``results_section.finalize_partido`` when legenda is
    unique per group (one legend-vote row per party/section/cargo): both
    re-sum nominais and outer-merge legenda on the group keys. The
    equivalence is asserted by ``validate_stream_year`` on 1996.
    """
    grp = [c for c in rsec._GROUP_COLS if c in nominais_partial.columns]
    nom_agg = (
        nominais_partial.groupby(grp, as_index=False, dropna=False)["votos"]
        .sum()
        .rename(columns={"votos": "votos_nominais"})
    )
    lgrp = [c for c in rsec._GROUP_COLS if c in legenda_partial.columns]
    leg_agg = legenda_partial.groupby(lgrp, as_index=False, dropna=False)[
        "votos_legenda"
    ].sum()
    merge_keys = [c for c in grp if c in leg_agg.columns]
    partido = nom_agg.merge(leg_agg, on=merge_keys, how="outer")
    partido["votos_nominais"] = partido["votos_nominais"].fillna(0).astype(int)
    partido["votos_legenda"] = partido["votos_legenda"].fillna(0).astype(int)
    return partido


# ---------------------------------------------------------------------------
# perfil_eleitorado_secao
# ---------------------------------------------------------------------------


def _iter_perfil_chunks(path: Path, chunksize: int):
    """Chunked twin of ``_read_perfil_secao`` (header row parsed once)."""
    for chunk in pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
        chunksize=chunksize,
    ):
        chunk.columns = chunk.columns.str.lower().str.strip('"')
        yield chunk


def stream_perfil_secao(ano: int, out_root: Path) -> None:
    for uf in vps.UFS[ano]:
        path = vps.perfil_secao_path(ano, uf)
        big = path.stat().st_size > BIG_BYTES
        frames = (
            _iter_perfil_chunks(path, CHUNK)
            if big
            else [vps._read_perfil_secao(ano, uf)]
        )
        agg_parts: list[pd.DataFrame] = []
        for raw in frames:
            df = vps.clean_perfil_frame(raw, ano, uf)
            agg_parts.append(vps._perfil_groupby(df))
            del raw, df
            gc.collect()
        agg = vps._perfil_groupby(pd.concat(agg_parts, ignore_index=True))
        out = vps.finalize_perfil_uf(agg)
        out = out[[c for c in vps.PERFIL_COL_ORDER if c in out.columns]]
        _write_partition(out, out_root, "perfil_eleitorado_secao", ano, uf)
        del agg_parts, agg, out
        gc.collect()
        print(f"    {uf} done", flush=True)


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def validate_stream_year(ano: int, out_root: Path) -> None:
    """Assert the streamed per-(ano,uf) output equals the in-RAM build for a
    year small enough to build whole (use 1996 for secao). Order-independent
    multiset comparison per table."""
    cand, part = rsec.build_resultados_secao(ano)
    for table, ref in (
        ("resultados_candidato_secao", cand),
        ("resultados_partido_secao", part),
    ):
        parts = sorted(
            (out_root / table).glob(f"ano={ano}/sigla_uf=*/data.parquet")
        )
        got = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        cols = list(ref.columns)
        a = ref.sort_values(cols).reset_index(drop=True)
        b = got[cols].sort_values(cols).reset_index(drop=True)
        print(f"{table} {ano}: stream==build {a.equals(b)} ({a.shape})")


def build_all_resultados_secao(out_root: Path = STREAM_SECAO_ROOT) -> None:
    """Stream every year of the two resultados_secao families to out_root."""
    for ano in sorted(rsec.UFS.keys()):
        print(f"  streaming resultados_secao {ano}", flush=True)
        stream_resultados_secao(ano, out_root)


def build_all_perfil_secao(out_root: Path = STREAM_SECAO_ROOT) -> None:
    """Stream every year of perfil_eleitorado_secao to out_root."""
    for ano in sorted(vps.UFS.keys()):
        print(f"  streaming perfil_eleitorado_secao {ano}", flush=True)
        stream_perfil_secao(ano, out_root)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        build_all_resultados_secao()
        build_all_perfil_secao()
    else:
        ano = int(sys.argv[1])
        out_root = (
            Path(sys.argv[2]) if len(sys.argv) > 2 else STREAM_SECAO_ROOT
        )
        print(f"streaming resultados_secao {ano}")
        stream_resultados_secao(ano, out_root)
        if ano >= 2008 and ano in vps.UFS:
            print(f"streaming perfil_secao {ano}")
            stream_perfil_secao(ano, out_root)
