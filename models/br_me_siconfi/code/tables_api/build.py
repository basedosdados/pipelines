"""
Build cleaned output CSVs from per-municipality API JSON files.

Input:  input/api/dca_{year}_{cod_ibge}.json
Output: output_API/{table}/ano={ano}/sigla_uf={uf}/{table}.csv

Usage:
    python build.py --path_dados /path/to/dados_SICONFI
    python build.py --path_dados ... --table municipio_receitas_orcamentarias
    python build.py --path_dados ... --ano 2015
    python build.py --path_dados ... --ano 2015 --table municipio_balanco_patrimonial
"""

import argparse
import os
import sys

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
from tables_api.shared import load_compatibilizacao, setup_output_dirs

# (table_module, first_year, last_year)
# All tables read from input/api/ — entity level distinguished by cod_ibge length
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

    # Determine year range across selected tables for directory setup
    first_year = min(TABLE_BUILDERS[t][1] for t in tables)
    last_year = max(TABLE_BUILDERS[t][2] for t in tables)
    if args.ano:
        first_year = last_year = args.ano
    setup_output_dirs(path_dados, first_year, last_year)

    api_dir = args.api_dir or os.path.join(code_dir, "input", "api")
    if not os.path.isdir(api_dir):
        print(f"ERROR: API directory not found: {api_dir}")
        sys.exit(1)

    for table in tables:
        module, tbl_first, tbl_last = TABLE_BUILDERS[table]
        first = args.ano if args.ano else tbl_first
        last = args.ano if args.ano else tbl_last
        print(f"Building: {table} ({first}-{last})")
        module.build(path_dados, path_queries, comp, api_dir, first, last)
        print("  Done.")

    print("All done.")


if __name__ == "__main__":
    main()
