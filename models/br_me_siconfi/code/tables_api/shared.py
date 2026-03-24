"""Shared utilities for tables_API — reads per-municipality API JSON files."""

import json
import os

import pandas as pd

UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "SC",
    "SE",
    "RS",
    "SP",
    "TO",
]

ORDEM_MUNICIPIO = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "estagio",
    "portaria",
    "conta",
    "estagio_bd",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_MUNICIPIO_BALANCO = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "portaria",
    "conta",
    "id_conta_bd",
    "conta_bd",
    "valor",
]


def load_compatibilizacao(path_queries):
    comp_dir = os.path.join(path_queries, "code", "compatibilizacao")
    df_municipio = pd.read_excel(
        os.path.join(comp_dir, "municipio.xlsx"), dtype="string"
    )
    df_receitas = pd.read_excel(
        os.path.join(comp_dir, "receitas_orcamentarias.xlsx"), dtype="string"
    ).fillna("")
    df_despesas = pd.read_excel(
        os.path.join(comp_dir, "despesas_orcamentarias.xlsx"), dtype="string"
    ).fillna("")
    df_despesas_funcao = pd.read_excel(
        os.path.join(comp_dir, "despesas_funcao.xlsx"), dtype="string"
    ).fillna("")
    df_balanco = pd.read_excel(
        os.path.join(comp_dir, "balanco_patrimonial.xlsx"), dtype="string"
    ).fillna("")
    return {
        "municipio": df_municipio,
        "receitas": df_receitas,
        "despesas": df_despesas,
        "despesas_funcao": df_despesas_funcao,
        "balanco": df_balanco,
    }


def load_api_json(json_path):
    """Load one per-municipality API JSON file and return a flat DataFrame."""
    with open(json_path) as f:
        d = json.load(f)
    items = d["data"].get("items", [])
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    df["id_municipio"] = df["cod_ibge"].astype(str)
    df["sigla_uf"] = df["uf"].astype(str)
    df["estagio"] = df["coluna"].astype(str)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    return df


def apply_conta_split(df, ano):
    """Split 'portaria - conta_name' strings into separate portaria and conta columns."""
    split_fn = _split_2013_2017 if ano <= 2017 else _split_2018_plus

    portarias, contas = [], []
    for val in df["conta"]:
        p, c = split_fn(str(val) if val else "")
        portarias.append(p)
        contas.append(c)

    df["portaria"] = pd.array(portarias, dtype="string")
    df["conta"] = pd.array(contas, dtype="string")
    df["conta"] = df["conta"].str.replace("\ufffd", "-", regex=False)
    return df


def _split_2013_2017(s):
    if s and s[0].isnumeric():
        parts = s.split(" -", 1)
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""
    return "", s


def _split_2018_plus(s):
    if s and s[0].isnumeric():
        portaria = s[:14].strip()
        conta = s[16:].strip() if "0 -" in s else s[15:].strip()
        return portaria, conta
    return "", s


def partition_and_save(df, table_name, ano, path_dados):
    """Save per-UF partition CSVs, dropping sigla_uf and ano columns."""
    drop_cols = ["sigla_uf", "ano"]
    for uf in UFS:
        part = df.loc[df["sigla_uf"] == uf].drop(drop_cols, axis=1)
        out_path = os.path.join(
            path_dados,
            f"output_API/{table_name}/ano={ano}/sigla_uf={uf}/{table_name}.csv",
        )
        part.to_csv(out_path, index=False, encoding="utf-8", na_rep="")


def setup_output_dirs(path_dados, first_year, last_year):
    """Create all output_API partition directories for municipal tables."""
    tables_municipal = [
        ("municipio_receitas_orcamentarias", first_year),
        ("municipio_despesas_orcamentarias", first_year),
        ("municipio_despesas_funcao", first_year),
        ("municipio_balanco_patrimonial", first_year),
    ]
    for table, start in tables_municipal:
        for ano in range(start, last_year + 1):
            for uf in UFS:
                os.makedirs(
                    os.path.join(
                        path_dados,
                        f"output_API/{table}/ano={ano}/sigla_uf={uf}",
                    ),
                    exist_ok=True,
                )
