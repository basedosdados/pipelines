"""
Full SICONFI pipeline: download (optional) + build all tables.

Each table module in tables_final/ handles its full year range internally:
  - municipio tables with legacy data: 1989/1996/1998-2026 (Excel ≤2012, JSON ≥2013)
  - all other tables:                  2013-2026 (JSON only)

All output goes to output/{table}/ano={ano}/sigla_uf={uf}/{table}.csv
(Brasil tables: output/{table}/ano={ano}/{table}.csv)

The build runs years in parallel using ProcessPoolExecutor.
Use --workers to control parallelism (default: 1 = sequential).

Usage:
    python build.py --path_dados /path/to/dados_SICONFI
    python build.py --path_dados ... --workers 4
    python build.py --path_dados ... --download --download-workers 5
    python build.py --path_dados ... --table municipio_receitas_orcamentarias
    python build.py --path_dados ... --ano 2022
"""

import argparse
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)

from tables_final.shared import _init_worker, process_year_task  # noqa: E402

# ---------------------------------------------------------------------------
# Table registry
# (first_year, last_year, comp_file)
# comp_file: compatibilizacao key used for missing-row error reports;
#            empty string means no unmatched-row tracking for that table.
# ---------------------------------------------------------------------------

BUILDERS = {
    # municipio tables — legacy (≤2012) + API (≥2013)
    "municipio_receitas_orcamentarias": (1989, 2026, "receitas_orcamentarias"),
    "municipio_despesas_orcamentarias": (1989, 2026, "despesas_orcamentarias"),
    "municipio_despesas_funcao": (1996, 2026, "despesas_funcao"),
    "municipio_balanco_patrimonial": (1998, 2026, "balanco_patrimonial"),
    # uf / brasil tables — API only (2013+)
    "uf_receitas_orcamentarias": (2013, 2026, "receitas_orcamentarias"),
    "uf_despesas_orcamentarias": (2013, 2026, "despesas_orcamentarias"),
    "uf_despesas_funcao": (2013, 2026, "despesas_funcao"),
    "brasil_receitas_orcamentarias": (2013, 2026, "receitas_orcamentarias"),
    "brasil_despesas_orcamentarias": (2013, 2026, "despesas_orcamentarias"),
    "brasil_despesas_funcao": (2013, 2026, "despesas_funcao"),
    "municipio_execucao_restos_pagar": (2013, 2026, ""),
    "uf_execucao_restos_pagar": (2013, 2026, ""),
    "brasil_execucao_restos_pagar": (2013, 2026, ""),
    "municipio_execucao_restos_pagar_funcao": (2013, 2026, ""),
    "uf_execucao_restos_pagar_funcao": (2013, 2026, ""),
    "brasil_execucao_restos_pagar_funcao": (2013, 2026, ""),
    "municipio_variacoes_patrimoniais": (2013, 2026, ""),
    "uf_variacoes_patrimoniais": (2013, 2026, ""),
    "brasil_variacoes_patrimoniais": (2013, 2026, ""),
}

ALL_TABLES = sorted(BUILDERS)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def run_download(ano, workers):
    cmd = [sys.executable, os.path.join(_here, "download_api.py")]
    if workers > 1:
        cmd += ["--workers", str(workers)]
    if ano is not None:
        cmd += ["--start-year", str(ano), "--end-year", str(ano)]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _aggregate_unmatched(all_unmatched, ano_result):
    for comp_file, df in ano_result.items():
        if comp_file and not df.empty:
            all_unmatched.setdefault(comp_file, []).append(df)


def run_build(tables, path_dados, path_queries, workers, ano_filter):
    api_dir = os.path.join(_here, "input", "api")
    if not os.path.isdir(api_dir):
        print(f"ERROR: API directory not found: {api_dir}")
        sys.exit(1)

    # Build config list for process_year_task.
    table_configs = [
        (name, first, last, comp_file)
        for name, (first, last, comp_file) in BUILDERS.items()
        if name in tables
    ]

    first_year = min(first for _, first, _, _ in table_configs)
    last_year = max(last for _, _, last, _ in table_configs)
    if ano_filter is not None:
        first_year = last_year = ano_filter

    years = list(range(first_year, last_year + 1))
    all_unmatched = {}

    print(f"\n{'=' * 60}")
    print(f"BUILD  years {first_year}-{last_year}  workers={workers}")
    print(f"{'=' * 60}")

    if workers == 1:
        _init_worker(_here, path_queries)
        for ano in years:
            print(f"\n=== {ano} ===")
            _, ano_result = process_year_task(
                (ano, api_dir, path_dados, path_queries, table_configs)
            )
            _aggregate_unmatched(all_unmatched, ano_result)
    else:
        tasks = [
            (ano, api_dir, path_dados, path_queries, table_configs)
            for ano in years
        ]
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(_here, path_queries),
        ) as pool:
            future_to_ano = {
                pool.submit(process_year_task, t): t[0] for t in tasks
            }
            for future in as_completed(future_to_ano):
                ano, ano_result = future.result()
                print(f"  year {ano} done")
                _aggregate_unmatched(all_unmatched, ano_result)

    if all_unmatched:
        print("\nERROR: unmatched rows — add these to compatibilizacao files:")
        comp_dir = os.path.join(path_queries, "code", "compatibilizacao")
        for comp_file, dfs in sorted(all_unmatched.items()):
            combined = (
                pd.concat(dfs, ignore_index=True)
                .drop_duplicates()
                .sort_values(list(dfs[0].columns))
                .reset_index(drop=True)
            )
            out_path = os.path.join(comp_dir, f"missing_{comp_file}.xlsx")
            combined.to_excel(out_path, index=False)
            print(
                f"  {comp_file}.xlsx: {len(combined)} missing rows → {out_path}"
            )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Full SICONFI pipeline")
    parser.add_argument(
        "--path_dados",
        default=(
            "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis"
            "/Mac/Downloads/dados_SICONFI"
        ),
        help="Root data directory (contains input/ and will write output/)",
    )
    parser.add_argument(
        "--table",
        default=None,
        choices=ALL_TABLES,
        help="Build only this table (default: all)",
    )
    parser.add_argument(
        "--ano",
        type=int,
        default=None,
        help="Build only this year (default: full range)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Worker processes for parallel build (default: 1 = sequential)",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download API data before building (runs download_api.py)",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=1,
        dest="download_workers",
        help="Parallel workers for download_api.py (default: 1)",
    )
    args = parser.parse_args()

    path_dados = args.path_dados
    path_queries = os.path.dirname(_here)
    tables = [args.table] if args.table else ALL_TABLES

    print(f"path_dados:   {path_dados}")
    print(f"path_queries: {path_queries}")
    if args.ano:
        print(f"year filter:  {args.ano}")
    if args.table:
        print(f"table filter: {args.table}")

    if args.download:
        print("\n=== Downloading API data ===")
        run_download(args.ano, args.download_workers)

    run_build(tables, path_dados, path_queries, args.workers, args.ano)

    print("\nAll done.")


if __name__ == "__main__":
    main()
