"""
SICONFI data build script.

Usage:
    python build.py --path_dados /path/to/dados_SICONFI
    python build.py --path_dados /path/to/dados_SICONFI --table municipio_balanco_patrimonial
    python build.py --path_dados /path/to/dados_SICONFI --table uf_despesas_funcao --ano 2016
"""

import argparse
import os
import sys

# Allow running from the code/ directory directly
sys.path.insert(0, os.path.dirname(__file__))

from tables import (
    municipio_balanco_patrimonial,
    municipio_despesas_funcao,
    municipio_despesas_orcamentarias,
    municipio_receitas_orcamentarias,
    uf_despesas_funcao,
    uf_despesas_orcamentarias,
    uf_receitas_orcamentarias,
)
from tables.shared import load_compatibilizacao, setup_output_dirs

LAST_YEAR = 2023

TABLE_BUILDERS = {
    "municipio_receitas_orcamentarias": (
        municipio_receitas_orcamentarias,
        1989,
        LAST_YEAR,
    ),
    "municipio_despesas_orcamentarias": (
        municipio_despesas_orcamentarias,
        1989,
        LAST_YEAR,
    ),
    "municipio_despesas_funcao": (municipio_despesas_funcao, 1996, LAST_YEAR),
    "municipio_balanco_patrimonial": (
        municipio_balanco_patrimonial,
        1998,
        LAST_YEAR,
    ),
    "uf_receitas_orcamentarias": (uf_receitas_orcamentarias, 2013, LAST_YEAR),
    "uf_despesas_orcamentarias": (uf_despesas_orcamentarias, 2013, LAST_YEAR),
    "uf_despesas_funcao": (uf_despesas_funcao, 2013, LAST_YEAR),
}


def main():
    parser = argparse.ArgumentParser(description="Build SICONFI output tables")
    parser.add_argument(
        "--path_dados",
        required=True,
        help="Path to dados_SICONFI root (must contain input/ and output/)",
    )
    parser.add_argument(
        "--path_queries",
        default=None,
        help="Path to queries repo root (default: two levels up from this script)",
    )
    parser.add_argument(
        "--table",
        default=None,
        choices=list(TABLE_BUILDERS.keys()),
        help="Run only this table (default: all tables)",
    )
    parser.add_argument(
        "--ano",
        type=int,
        default=None,
        help="Run only this year (default: full range for each table)",
    )
    args = parser.parse_args()

    path_dados = args.path_dados
    path_queries = args.path_queries or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )

    print(f"path_dados:   {path_dados}")
    print(f"path_queries: {path_queries}")

    print("Loading compatibilizacao tables...")
    comp = load_compatibilizacao(path_queries)

    print("Creating output directories...")
    setup_output_dirs(path_dados, LAST_YEAR)

    tables_to_run = [args.table] if args.table else list(TABLE_BUILDERS.keys())

    for table_name in tables_to_run:
        module, default_first, default_last = TABLE_BUILDERS[table_name]
        first_year = args.ano if args.ano else default_first
        last_year = args.ano if args.ano else default_last

        print(f"\n{'=' * 60}")
        print(f"Building: {table_name} ({first_year}-{last_year})")
        print(f"{'=' * 60}")
        module.build(path_dados, path_queries, comp, first_year, last_year)

    print("\nDone.")


if __name__ == "__main__":
    main()
