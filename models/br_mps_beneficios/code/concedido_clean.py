"""One-shot bootstrap: build beneficio_concedido_municipio_mes for the whole series.

Imports the shared transform from ``pipelines.datasets.br_mps_beneficios.utils``
rather than duplicating it, so the recurring Prefect pipeline and this bootstrap
can never drift apart.

Scratch data goes to ``$BR_MPS_BENEFICIOS_DATA`` (default
``~/Library/Caches/br_mps_beneficios_data``) — deliberately NOT under
``~/Downloads``, which on this machine is a symlink into Dropbox and would sync
every gigabyte.

Each source file is downloaded, aggregated, staged as one parquet per
competência and then deleted, so peak disk stays near a single archive. Staging
per competência also makes the run resumable and idempotent: the annual archives
and the monthly packages could in principle both cover a month, and staging by
competência means the later era overwrites rather than double-counts.

Usage:
    python models/br_mps_beneficios/code/concedido_clean.py [--limit N] [--from YYYYMM]
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

TABLE = "beneficio_concedido_municipio_mes"
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
    limit: int | None = None, start: int | None = None, keep_raw: bool = False
) -> None:
    u.validate_reference_tables()
    for d in (INPUT, STAGING, OUTPUT, REPORT):
        d.mkdir(parents=True, exist_ok=True)

    resources = u.resolve_concedido_resources()
    if start:
        resources = [
            r
            for r in resources
            if (r["competencia"] or (r["ano"] or 0) * 100 + 12) >= start
        ]
    if limit:
        resources = resources[:limit]

    gex: dict[tuple[str, str, str], str] = {}
    diagnostics: list[dict] = []
    print(f"{len(resources)} resources to process\n", flush=True)

    for i, res in enumerate(resources, 1):
        label = res["competencia"] or f"ano {res['ano']}"
        # A monthly file maps to one staged competência; an annual archive maps
        # to up to twelve, so it is only skippable once the whole year is there.
        if res["competencia"]:
            already = stage_path(res["competencia"]).exists()
        else:
            staged = {
                int(f.stem.split("=")[1])
                for f in STAGING.glob("comp=*.parquet")
            }
            already = sum(1 for c in staged if c // 100 == res["ano"]) >= 12
        if already:
            print(
                f"[{i}/{len(resources)}] {label}: already staged, skipping",
                flush=True,
            )
            continue

        ext = (
            ".xlsx"
            if res["url"].lower().endswith((".xlsx", ".xls"))
            else ".zip"
            if res["url"].lower().endswith(".zip")
            else ".csv"
        )
        dest = INPUT / f"conc_{res['competencia'] or res['ano']}{ext}"
        t0 = time.time()
        try:
            u.download(res["url"], dest)
            size_mb = dest.stat().st_size / 1e6
            df, diag = u.aggregate_concedido(
                dest, res["competencia"], gex_sink=gex
            )
        except Exception as exc:
            print(
                f"[{i}/{len(resources)}] {label}: FAILED {type(exc).__name__}: "
                f"{str(exc)[:300]}",
                flush=True,
            )
            diagnostics.append(
                {
                    "recurso": label,
                    "erro": f"{type(exc).__name__}: {exc}"[:400],
                }
            )
            continue

        if df.empty:
            print(f"[{i}/{len(resources)}] {label}: no rows", flush=True)
        for comp, part in df.groupby(df["ano"] * 100 + df["mes"]):
            part.drop(columns=[]).to_parquet(
                stage_path(int(comp)), index=False
            )
        diag["recurso"] = label
        diag["era"] = res["era"]
        diag["competencias"] = (
            sorted({int(c) for c in (df["ano"] * 100 + df["mes"]).unique()})
            if not df.empty
            else []
        )
        diag["arquivo_mb"] = round(size_mb, 1)
        diag["segundos"] = round(time.time() - t0, 1)
        diagnostics.append(diag)
        print(
            f"[{i}/{len(resources)}] {label}: {diag['linhas']:,} rows -> "
            f"{len(df):,} cells, {len(diag['competencias'])} competência(s), "
            f"sem_mun={diag['sem_municipio']:,} "
            f"nao_achado={diag['municipio_nao_encontrado']:,} "
            f"({diag['segundos']}s, {size_mb:.0f}MB)",
            flush=True,
        )

        if not keep_raw:
            dest.unlink(missing_ok=True)

        (REPORT / "concedido_diagnostics.json").write_text(
            json.dumps(diagnostics, ensure_ascii=False, indent=2, default=str)
        )
        if gex:
            u.build_gex_lookup(gex).to_csv(
                REPORT / "municipio_gex_lookup.csv", index=False
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

    dic = u.build_dicionario_especie()
    (OUTPUT / "dicionario_especie").mkdir(parents=True, exist_ok=True)
    dic.astype("string").to_parquet(
        OUTPUT / "dicionario_especie" / "data.parquet", index=False
    )
    print(f"wrote dicionario_especie: {len(dic)} rows")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--from", dest="start", type=int)
    ap.add_argument("--keep-raw", action="store_true")
    ap.add_argument("--consolidate-only", action="store_true")
    args = ap.parse_args()
    if args.consolidate_only:
        consolidate()
    else:
        run(args.limit, args.start, args.keep_raw)


if __name__ == "__main__":
    main()
