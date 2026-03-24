"""
Validate new output against reference files.

Usage:
    python validate_output.py --ref_path /path/to/reference/output --new_path /path/to/new/output
    python validate_output.py  # uses default paths
"""

import argparse
import os

import pandas as pd

DEFAULT_PATH = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/Mac/Downloads/dados_SICONFI/output"

EXPECTED_COLUMNS = {
    "municipio_receitas_orcamentarias": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "municipio_despesas_orcamentarias": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "municipio_despesas_funcao": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "municipio_balanco_patrimonial": [
        "id_municipio",
        "portaria",
        "conta",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "uf_receitas_orcamentarias": [
        "id_uf",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "uf_despesas_orcamentarias": [
        "id_uf",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
    "uf_despesas_funcao": [
        "id_uf",
        "estagio",
        "portaria",
        "conta",
        "estagio_bd",
        "id_conta_bd",
        "conta_bd",
        "valor",
    ],
}


def validate_partition(ref_path, new_path, table, ano, uf, strict_rows=True):
    fname = f"{table}.csv"
    ref_file = os.path.join(
        ref_path, table, f"ano={ano}", f"sigla_uf={uf}", fname
    )
    new_file = os.path.join(
        new_path, table, f"ano={ano}", f"sigla_uf={uf}", fname
    )

    issues = []

    if not os.path.exists(ref_file):
        return []  # no reference to compare against
    if not os.path.exists(new_file):
        issues.append(f"MISSING output: {table}/ano={ano}/sigla_uf={uf}")
        return issues

    try:
        ref = pd.read_csv(ref_file, low_memory=False)
    except Exception:
        return []  # empty or unreadable reference — skip

    try:
        new = pd.read_csv(new_file, low_memory=False)
    except Exception:
        issues.append(f"UNREADABLE output: {table}/ano={ano}/sigla_uf={uf}")
        return issues

    if list(ref.columns) != list(new.columns):
        issues.append(
            f"COLUMN MISMATCH {table}/ano={ano}/sigla_uf={uf}: ref={list(ref.columns)} new={list(new.columns)}"
        )

    if strict_rows and len(ref) != len(new):
        issues.append(
            f"ROW COUNT {table}/ano={ano}/sigla_uf={uf}: ref={len(ref)} new={len(new)}"
        )
    elif (
        not strict_rows and abs(len(ref) - len(new)) / max(len(ref), 1) > 0.01
    ):
        issues.append(
            f"ROW COUNT >1% diff {table}/ano={ano}/sigla_uf={uf}: ref={len(ref)} new={len(new)}"
        )

    return issues


def find_available_partitions(base_path, table):
    """Return list of (ano, uf) tuples that exist under base_path/table."""
    table_dir = os.path.join(base_path, table)
    if not os.path.isdir(table_dir):
        return []
    partitions = []
    for ano_dir in sorted(os.listdir(table_dir)):
        if not ano_dir.startswith("ano="):
            continue
        ano = ano_dir[4:]
        ano_path = os.path.join(table_dir, ano_dir)
        for uf_dir in sorted(os.listdir(ano_path)):
            if not uf_dir.startswith("sigla_uf="):
                continue
            uf = uf_dir[9:]
            csv = os.path.join(ano_path, uf_dir, f"{table}.csv")
            if os.path.exists(csv):
                partitions.append((ano, uf))
    return partitions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref_path", default=DEFAULT_PATH)
    parser.add_argument("--new_path", default=DEFAULT_PATH)
    parser.add_argument("--table", default=None)
    args = parser.parse_args()

    tables = [args.table] if args.table else list(EXPECTED_COLUMNS.keys())

    total_checked = 0
    total_issues = []

    for table in tables:
        partitions = find_available_partitions(args.ref_path, table)
        if not partitions:
            print(f"  {table}: no reference partitions found, skipping")
            continue

        table_issues = []
        for ano, uf in partitions:
            issues = validate_partition(
                args.ref_path, args.new_path, table, ano, uf
            )
            table_issues.extend(issues)
            total_checked += 1

        status = "OK" if not table_issues else f"{len(table_issues)} ISSUES"
        print(f"  {table}: {len(partitions)} partitions checked — {status}")
        total_issues.extend(table_issues)

    print(f"\nTotal partitions checked: {total_checked}")
    if total_issues:
        print(f"ISSUES ({len(total_issues)}):")
        for issue in total_issues:
            print(f"  {issue}")
    else:
        print("All checks passed!")


if __name__ == "__main__":
    main()
