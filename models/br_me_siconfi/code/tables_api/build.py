"""
Build cleaned output CSVs from per-entity API JSON files.

Input:  input/api/dca_{year}_{cod_ibge}.json
Output: output_API/{table}/ano={ano}/sigla_uf={uf}/{table}.csv  (municipio/UF)
        output_API/{table}/ano={ano}/{table}.csv                 (brasil)

Usage:
    python build.py --path_dados /path/to/dados_SICONFI
    python build.py --path_dados ... --table municipio_receitas_orcamentarias
    python build.py --path_dados ... --ano 2015
    python build.py --path_dados ... --ano 2015 --table municipio_balanco_patrimonial
"""

import argparse
import os
import sys

import pandas as pd

# Allow running as a script from any directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tables_api import (
    brasil_despesas_funcao,
    brasil_despesas_orcamentarias,
    brasil_receitas_orcamentarias,
    municipio_balanco_patrimonial,
    municipio_despesas_funcao,
    municipio_despesas_orcamentarias,
    municipio_receitas_orcamentarias,
    uf_despesas_funcao,
    uf_despesas_orcamentarias,
    uf_receitas_orcamentarias,
)
from tables_api.shared import (
    load_compatibilizacao,
    load_year_data,
    setup_output_dirs,
)

# Maps each table to the compatibilizacao file it draws from.
COMP_FILE = {
    "municipio_receitas_orcamentarias": "receitas_orcamentarias",
    "uf_receitas_orcamentarias": "receitas_orcamentarias",
    "brasil_receitas_orcamentarias": "receitas_orcamentarias",
    "municipio_despesas_orcamentarias": "despesas_orcamentarias",
    "uf_despesas_orcamentarias": "despesas_orcamentarias",
    "brasil_despesas_orcamentarias": "despesas_orcamentarias",
    "municipio_despesas_funcao": "despesas_funcao",
    "uf_despesas_funcao": "despesas_funcao",
    "brasil_despesas_funcao": "despesas_funcao",
    "municipio_balanco_patrimonial": "balanco_patrimonial",
}

# (table_module, first_year, last_year)
TABLE_BUILDERS = {
    "municipio_receitas_orcamentarias": (
        municipio_receitas_orcamentarias,
        2013,
        2026,
    ),
    "municipio_despesas_orcamentarias": (
        municipio_despesas_orcamentarias,
        2013,
        2026,
    ),
    "municipio_despesas_funcao": (municipio_despesas_funcao, 2013, 2026),
    "municipio_balanco_patrimonial": (
        municipio_balanco_patrimonial,
        2013,
        2026,
    ),
    "uf_receitas_orcamentarias": (uf_receitas_orcamentarias, 2013, 2026),
    "uf_despesas_orcamentarias": (uf_despesas_orcamentarias, 2013, 2026),
    "uf_despesas_funcao": (uf_despesas_funcao, 2013, 2026),
    "brasil_receitas_orcamentarias": (
        brasil_receitas_orcamentarias,
        2013,
        2026,
    ),
    "brasil_despesas_orcamentarias": (
        brasil_despesas_orcamentarias,
        2013,
        2026,
    ),
    "brasil_despesas_funcao": (brasil_despesas_funcao, 2013, 2026),
}


def main():
    parser = argparse.ArgumentParser(
        description="Build SICONFI tables from API JSON files"
    )
    parser.add_argument(
        "--path_dados",
        default="/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/Mac/Downloads/dados_SICONFI",
        help="Root data directory (contains input/api/ and will write output_API/)",
    )
    parser.add_argument(
        "--api_dir",
        default=None,
        help="Directory containing API JSON files (default: code/input/api)",
    )
    parser.add_argument("--table", default=None, help="Build only this table")
    parser.add_argument(
        "--ano", type=int, default=None, help="Build only this year"
    )
    args = parser.parse_args()

    path_dados = args.path_dados
    # build.py lives at code/tables_api/build.py; path_queries = br_me_siconfi/
    _here = os.path.dirname(os.path.abspath(__file__))
    code_dir = os.path.dirname(_here)
    path_queries = os.path.dirname(code_dir)

    tables = [args.table] if args.table else list(TABLE_BUILDERS.keys())

    comp = load_compatibilizacao(path_queries)

    first_year = min(TABLE_BUILDERS[t][1] for t in tables)
    last_year = max(TABLE_BUILDERS[t][2] for t in tables)
    if args.ano:
        first_year = last_year = args.ano
    setup_output_dirs(path_dados, first_year, last_year)

    api_dir = args.api_dir or os.path.join(code_dir, "input", "api")
    if not os.path.isdir(api_dir):
        print(f"ERROR: API directory not found: {api_dir}")
        sys.exit(1)

    all_unmatched = {}  # comp_file -> [df, ...]

    for ano in range(first_year, last_year + 1):
        print(f"\n=== {ano} ===")
        year_data = load_year_data(ano, api_dir)

        for table in tables:
            module, tbl_first, tbl_last = TABLE_BUILDERS[table]
            if not (tbl_first <= ano <= tbl_last):
                continue
            unmatched = module.build(
                path_dados, path_queries, comp, year_data, ano
            )
            if unmatched is not None and not unmatched.empty:
                all_unmatched.setdefault(COMP_FILE[table], []).append(
                    unmatched
                )

    if all_unmatched:
        print(
            "\nERROR: unmatched rows detected — add these to compatibilizacao files:"
        )
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

    print("\nAll done.")


if __name__ == "__main__":
    main()
