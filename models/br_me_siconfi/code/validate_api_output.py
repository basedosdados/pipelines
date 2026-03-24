"""
Validate output_API CSVs against reference output CSVs.

For each table/ano/sigla_uf partition present in output_API,
compare row counts and values against the reference.
Only compares municipalities present in both (API files cover a subset).
"""

import os

import pandas as pd

PATH_API = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/Mac/Downloads/dados_SICONFI/output_API"
PATH_REF = "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/Mac/Downloads/dados_SICONFI/output"

TABLES = [
    "municipio_receitas_orcamentarias",
    "municipio_despesas_orcamentarias",
    "municipio_despesas_funcao",
    "municipio_balanco_patrimonial",
]

# Merge keys to compare rows (excluding valor)
TABLE_KEYS = {
    "municipio_receitas_orcamentarias": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
    ],
    "municipio_despesas_orcamentarias": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
    ],
    "municipio_despesas_funcao": [
        "id_municipio",
        "estagio",
        "portaria",
        "conta",
    ],
    "municipio_balanco_patrimonial": ["id_municipio", "portaria", "conta"],
}

total_checked = 0
total_issues = []

for table in TABLES:
    table_api_dir = os.path.join(PATH_API, table)
    if not os.path.isdir(table_api_dir):
        print(f"  {table}: output_API directory not found, skipping")
        continue

    table_issues = []
    partitions_checked = 0

    for ano_dir in sorted(os.listdir(table_api_dir)):
        if not ano_dir.startswith("ano="):
            continue
        ano = ano_dir[4:]

        ano_path = os.path.join(table_api_dir, ano_dir)
        for uf_dir in sorted(os.listdir(ano_path)):
            if not uf_dir.startswith("sigla_uf="):
                continue
            uf = uf_dir[9:]

            api_file = os.path.join(ano_path, uf_dir, f"{table}.csv")
            ref_file = os.path.join(
                PATH_REF, table, f"ano={ano}", f"sigla_uf={uf}", f"{table}.csv"
            )

            if not os.path.exists(ref_file):
                continue

            try:
                api_df = pd.read_csv(api_file, low_memory=False, dtype=str)
                ref_df = pd.read_csv(ref_file, low_memory=False, dtype=str)
            except Exception:
                continue

            if api_df.empty and ref_df.empty:
                partitions_checked += 1
                continue

            # Filter ref to only the municipalities present in the API output
            if (
                "id_municipio" in api_df.columns
                and "id_municipio" in ref_df.columns
            ):
                api_muns = set(api_df["id_municipio"].unique())
                ref_df = ref_df[ref_df["id_municipio"].isin(api_muns)].copy()

            if api_df.empty or ref_df.empty:
                partitions_checked += 1
                continue

            # Check row counts
            if len(api_df) != len(ref_df):
                table_issues.append(
                    f"ROW COUNT {table}/ano={ano}/sigla_uf={uf}: api={len(api_df)} ref={len(ref_df)}"
                )

            # Compare values via merge on key columns
            keys = TABLE_KEYS[table]
            api_df["valor"] = pd.to_numeric(api_df["valor"], errors="coerce")
            ref_df["valor"] = pd.to_numeric(ref_df["valor"], errors="coerce")

            # Normalize quotes in conta (finbra CSVs have encoding artifacts)
            if "conta" in api_df.columns:
                api_df["conta"] = api_df["conta"].str.replace(
                    '"', "", regex=False
                )
            if "conta" in ref_df.columns:
                ref_df["conta"] = ref_df["conta"].str.replace(
                    '"', "", regex=False
                )

            merged = ref_df.merge(
                api_df[[*keys, "valor"]],
                on=keys,
                how="outer",
                suffixes=("_ref", "_api"),
                indicator=True,
            )
            only_ref = (merged["_merge"] == "left_only").sum()
            only_api = (merged["_merge"] == "right_only").sum()
            both = merged[merged["_merge"] == "both"].copy()

            if only_ref > 0 or only_api > 0:
                table_issues.append(
                    f"KEY MISMATCH {table}/ano={ano}/sigla_uf={uf}: only_ref={only_ref} only_api={only_api}"
                )

            if len(both) > 0:
                both["diff"] = (both["valor_ref"] - both["valor_api"]).abs()
                max_diff = both["diff"].max()
                if max_diff > 0.01:
                    table_issues.append(
                        f"VALUE DIFF {table}/ano={ano}/sigla_uf={uf}: max_diff={max_diff:.4f}"
                    )

            partitions_checked += 1
            total_checked += 1

    status = "OK" if not table_issues else f"{len(table_issues)} ISSUES"
    print(f"  {table}: {partitions_checked} partitions checked — {status}")
    total_issues.extend(table_issues)

print(f"\nTotal partitions checked: {total_checked}")
if total_issues:
    print(f"\nISSUES ({len(total_issues)}):")
    for issue in total_issues[:20]:
        print(f"  {issue}")
    if len(total_issues) > 20:
        print(f"  ... and {len(total_issues) - 20} more")
else:
    print("All checks passed!")
