"""Shared utilities for tables_API."""

import glob
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

ORDEM_BRASIL = [
    "ano",
    "estagio",
    "portaria",
    "conta",
    "estagio_bd",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_UF = [
    "ano",
    "sigla_uf",
    "id_uf",
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
    for df in [df_receitas, df_despesas, df_despesas_funcao, df_balanco]:
        df["_in_comp"] = True
    return {
        "municipio": df_municipio,
        "receitas": df_receitas,
        "despesas": df_despesas,
        "despesas_funcao": df_despesas_funcao,
        "balanco": df_balanco,
    }


def load_year_data(ano, api_dir):
    """Single-pass loader: read every file for a year once and partition by level/anexo.

    Returns:
        {
            "municipio": {anexo: DataFrame, ...},
            "uf":        {anexo: DataFrame, ...},
            "brasil":    {anexo: DataFrame, ...},
        }

    Entity columns set per level:
        municipio → id_municipio, sigla_uf
        uf        → id_uf, sigla_uf
        brasil    → (none extra)
    All levels get: estagio (= coluna), valor (numeric).
    """
    json_files = sorted(glob.glob(os.path.join(api_dir, f"dca_{ano}_*.json")))
    buckets = {}  # (level, anexo) -> list[DataFrame]

    for jpath in json_files:
        fname = os.path.basename(jpath)
        cod_str = fname[len(f"dca_{ano}_") : -5]  # strip prefix + ".json"
        n = len(cod_str)
        if n == 1:
            level = "brasil"
        elif n == 2:
            level = "uf"
        else:
            level = "municipio"

        with open(jpath) as f:
            d = json.load(f)
        items = d["data"].get("items", [])
        if not items:
            continue

        df = pd.DataFrame(items)
        df["estagio"] = df["coluna"].astype(str)
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        if level == "municipio":
            df["id_municipio"] = df["cod_ibge"].astype(str)
            df["sigla_uf"] = df["uf"].astype(str)
            keep = [
                "estagio",
                "valor",
                "conta",
                "anexo",
                "id_municipio",
                "sigla_uf",
            ]
        elif level == "uf":
            df["id_uf"] = df["cod_ibge"].astype(str)
            df["sigla_uf"] = df["uf"].astype(str)
            keep = ["estagio", "valor", "conta", "anexo", "id_uf", "sigla_uf"]
        else:
            keep = ["estagio", "valor", "conta", "anexo"]

        df = df[keep]

        for anexo_val, grp in df.groupby("anexo"):
            buckets.setdefault((level, anexo_val), []).append(grp)

    result = {}
    for (level, anexo_val), dfs in buckets.items():
        result.setdefault(level, {})[anexo_val] = pd.concat(
            dfs, ignore_index=True
        )

    return result


def apply_conta_split(df):
    """Vectorized split of 'CODE - NAME' conta strings into portaria and conta columns.

    Extracts portaria by matching the numeric code layout pattern (digits separated by dots)
    at the start of the string, then takes everything after it as the conta name.
    Handles all separator variants: 'CODE - Name', 'CODE- Name', 'CODE    Name', 'CODE Name'.
    """
    s = df["conta"].fillna("").astype(str)
    extracted = s.str.extract(r"^(\d+(?:\.\d+)*)\s*-?\s*(.*)", expand=True)
    matched = extracted[0].notna()

    portaria = extracted[0].where(matched, "").str.strip()
    conta = (
        extracted[1]
        .fillna("")
        .where(matched, s)
        .str.strip()
        .str.replace("\ufffd", "-", regex=False)
        .str.replace("\u00bf", "-", regex=False)
    )

    df = df.copy()
    df["portaria"] = portaria.astype("string")
    df["conta"] = conta.astype("string")
    return df


def apply_conta_split_funcao(df):
    """Alias for apply_conta_split — identical logic for despesas_funcao tables."""
    return apply_conta_split(df)


def get_unmatched(df, keys=("ano", "estagio", "portaria", "conta")):
    """Return unique key combinations where compatibilizacao join found no match.

    Checks for NaN in id_conta_bd — must be called before fillna("").
    """
    mask = df["_in_comp"].isna()
    if not mask.any():
        return pd.DataFrame()
    return (
        df.loc[mask, list(keys)]
        .drop_duplicates()
        .sort_values(list(keys))
        .reset_index(drop=True)
    )


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


def partition_and_save_brasil(df, table_name, ano, path_dados):
    """Save brasil-level CSV (no UF partition), dropping ano column."""
    out_path = os.path.join(
        path_dados,
        f"output_API/{table_name}/ano={ano}/{table_name}.csv",
    )
    df.drop("ano", axis=1).to_csv(
        out_path, index=False, encoding="utf-8", na_rep=""
    )


def setup_output_dirs(path_dados, first_year, last_year):
    """Create all output_API partition directories for municipal and UF tables."""
    uf_tables = [
        "municipio_receitas_orcamentarias",
        "municipio_despesas_orcamentarias",
        "municipio_despesas_funcao",
        "municipio_balanco_patrimonial",
        "uf_receitas_orcamentarias",
        "uf_despesas_orcamentarias",
        "uf_despesas_funcao",
    ]
    brasil_tables = [
        "brasil_receitas_orcamentarias",
        "brasil_despesas_orcamentarias",
        "brasil_despesas_funcao",
    ]
    for table in uf_tables:
        for ano in range(first_year, last_year + 1):
            for uf in UFS:
                os.makedirs(
                    os.path.join(
                        path_dados,
                        f"output_API/{table}/ano={ano}/sigla_uf={uf}",
                    ),
                    exist_ok=True,
                )
    for table in brasil_tables:
        for ano in range(first_year, last_year + 1):
            os.makedirs(
                os.path.join(path_dados, f"output_API/{table}/ano={ano}"),
                exist_ok=True,
            )
