"""One-shot bootstrap: build beneficio_mantido_municipio_mes month by month.

Imports the shared transform from ``pipelines.datasets.br_mps_beneficios.utils``.

This table is far larger than its concedido sibling: one month of ATIVOS is
about 12 GB uncompressed (876 MB zipped), so the whole ~56-month series is
roughly 700 GB of input. Each month is therefore downloaded, streamed straight
out of the zip without ever being written to disk uncompressed, aggregated,
staged, and the archive deleted before the next month starts. Peak disk stays
near one compressed archive.

Requires ``models/br_mps_beneficios/code/municipio_gex_lookup.csv``, which
concedido_clean.py produces. Benefícios mantidos truncates the município field
to 20 characters, and only the GEX prefix that concedido publishes makes that
truncated key unambiguous.

Usage:
    python models/br_mps_beneficios/code/mantido_clean.py [--limit N] [--from YYYYMM]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.br_mps_beneficios import utils as u

TABLE = "beneficio_mantido_municipio_mes"
BASE = Path(
    os.environ.get(
        "BR_MPS_BENEFICIOS_DATA",
        Path.home() / "Library/Caches/br_mps_beneficios_data",
    )
)
INPUT = BASE / "input"
STAGING = BASE / "staging" / TABLE
OUTPUT = BASE / "output"
REPORT = BASE / "reports"


def stage_path(competencia: int) -> Path:
    return STAGING / f"comp={competencia}.parquet"


def run(
    limit: int | None = None,
    start: int | None = None,
    keep_raw: bool = False,
    situacao: str = "ativos",
) -> None:
    u.validate_reference_tables()
    if not u.constants.MUNICIPIO_GEX_LOOKUP.value.exists():
        raise SystemExit(
            "municipio_gex_lookup.csv is missing. Run concedido_clean.py first — "
            "it derives the lookup from the GEX prefixes that only benefícios "
            "concedidos publishes."
        )
    # fails fast if a truncated espécie prefix ever spans two categorias
    u.assert_truncation_is_categoria_stable()
    for d in (INPUT, STAGING, OUTPUT, REPORT):
        d.mkdir(parents=True, exist_ok=True)

    resources = u.resolve_mantido_resources(situacao)
    if start:
        resources = [r for r in resources if r["competencia"] >= start]
    if limit:
        resources = resources[:limit]

    diagnostics: list[dict] = []
    print(f"{len(resources)} monthly files to process\n", flush=True)

    for i, res in enumerate(resources, 1):
        comp = res["competencia"]
        if stage_path(comp).exists():
            print(
                f"[{i}/{len(resources)}] {comp}: already staged, skipping",
                flush=True,
            )
            continue
        dest = INPUT / f"mantido_{situacao}_{comp}.zip"
        t0 = time.time()
        try:
            u.download(res["url"], dest)
            size_mb = dest.stat().st_size / 1e6
            df, diag = u.aggregate_mantido(dest, comp)
        except Exception as exc:
            print(
                f"[{i}/{len(resources)}] {comp}: FAILED {type(exc).__name__}: "
                f"{str(exc)[:300]}",
                flush=True,
            )
            diagnostics.append(
                {
                    "competencia": comp,
                    "erro": f"{type(exc).__name__}: {exc}"[:400],
                }
            )
            continue

        if not df.empty:
            df.to_parquet(stage_path(comp), index=False)
        diag["competencia"] = comp
        diag["arquivo_mb"] = round(size_mb, 1)
        diag["segundos"] = round(time.time() - t0, 1)
        diagnostics.append(diag)
        amb = diag["especie_ambigua"]
        print(
            f"[{i}/{len(resources)}] {comp}: {diag['linhas']:,} rows -> "
            f"{len(df):,} cells, sem_mun={diag['sem_municipio']:,} "
            f"nao_achado={diag['municipio_nao_encontrado']:,} "
            f"especie_ambigua={amb:,} "
            f"({100 * amb / max(diag['linhas'], 1):.1f}%) "
            f"({diag['segundos']}s, {size_mb:.0f}MB)",
            flush=True,
        )
        if not keep_raw:
            dest.unlink(missing_ok=True)
        (REPORT / "mantido_diagnostics.json").write_text(
            json.dumps(diagnostics, ensure_ascii=False, indent=2, default=str)
        )

    consolidate()


def consolidate() -> None:
    """Merge the per-competência staging files into the partitioned output."""
    files = sorted(STAGING.glob("comp=*.parquet"))
    if not files:
        print("nothing staged")
        return
    by_year: dict[int, list[Path]] = defaultdict(list)
    for f in files:
        by_year[int(f.stem.split("=")[1]) // 100].append(f)

    out = OUTPUT / TABLE
    if out.exists():
        shutil.rmtree(out)
    total = 0
    for ano in sorted(by_year):
        df = pd.concat(
            [pd.read_parquet(f) for f in by_year[ano]], ignore_index=True
        )
        u.write_partitioned(df, OUTPUT, TABLE, partition_cols=["ano"])
        total += len(df)
        print(
            f"  ano {ano}: {len(by_year[ano])} competência(s), {len(df):,} rows",
            flush=True,
        )
    print(f"\nwrote {total:,} rows across {len(by_year)} years to {out}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--from", dest="start", type=int)
    ap.add_argument("--keep-raw", action="store_true")
    ap.add_argument("--situacao", default="ativos")
    ap.add_argument("--consolidate-only", action="store_true")
    args = ap.parse_args()
    if args.consolidate_only:
        consolidate()
    else:
        run(args.limit, args.start, args.keep_raw, args.situacao)


if __name__ == "__main__":
    main()
