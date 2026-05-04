"""Shared utilities for tables_final."""

import glob
import importlib
import json
import os
import sys
import zipfile

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

ORDEM_MUNICIPIO_SIMPLES = [
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

ORDEM_UF_SIMPLES = [
    "ano",
    "sigla_uf",
    "estagio",
    "portaria",
    "conta",
    "estagio_bd",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_BRASIL_SIMPLES = [
    "ano",
    "estagio",
    "portaria",
    "conta",
    "estagio_bd",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_MUNICIPIO_VARIACOES = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "portaria",
    "conta",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_UF_VARIACOES = [
    "ano",
    "sigla_uf",
    "portaria",
    "conta",
    "id_conta_bd",
    "conta_bd",
    "valor",
]

ORDEM_BRASIL_VARIACOES = [
    "ano",
    "portaria",
    "conta",
    "id_conta_bd",
    "conta_bd",
    "valor",
]


# ---------------------------------------------------------------------------
# Compatibilizacao loader
# ---------------------------------------------------------------------------


def load_crosswalk(path_queries):
    comp_dir = os.path.join(path_queries, "code", "crosswalk")
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


# ---------------------------------------------------------------------------
# API data loader
# ---------------------------------------------------------------------------


def load_year_data(ano, api_dir, levels=None):
    """Single-pass loader: read every JSON file for a year, partition by level/anexo.

    Args:
        levels: iterable of level names to load, e.g. {"uf", "brasil"}.
                Defaults to all three: {"brasil", "uf", "municipio"}.

    Returns:
        {
            "municipio": {anexo: DataFrame, ...},
            "uf":        {anexo: DataFrame, ...},
            "brasil":    {anexo: DataFrame, ...},
        }

    Returns {} if no JSON files exist for the year (e.g., pre-2013).
    """
    if levels is None:
        levels = {"brasil", "uf", "municipio"}
    json_files = sorted(
        f
        for lvl in levels
        for f in glob.glob(os.path.join(api_dir, lvl, f"dca_{ano}_*.json"))
    )
    buckets = {}

    for jpath in json_files:
        fname = os.path.basename(jpath)
        cod_str = fname[len(f"dca_{ano}_") : -5]
        n = len(cod_str)
        if n == 1:
            level = "brasil"
        elif n == 2:
            level = "uf"
        else:
            level = "municipio"

        try:
            with open(jpath) as f:
                d = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
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
            df["sigla_uf"] = df["uf"].astype(str)
            keep = ["estagio", "valor", "conta", "anexo", "sigla_uf"]
        else:
            keep = ["estagio", "valor", "conta", "anexo"]

        df = df[keep]
        df["anexo"] = df["anexo"].str.replace(r"^(?!DCA-)", "DCA-", regex=True)

        for anexo_val, grp in df.groupby("anexo"):
            buckets.setdefault((level, anexo_val), []).append(grp)

    result = {}
    for (level, anexo_val), dfs in buckets.items():
        result.setdefault(level, {})[anexo_val] = pd.concat(
            dfs, ignore_index=True
        )

    return result


# ---------------------------------------------------------------------------
# Conta split (API path, 2013+)
# ---------------------------------------------------------------------------


def apply_conta_split(df):
    """Vectorized split of 'CODE - NAME' conta strings into portaria and conta columns."""
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
    """Return unique key combinations where crosswalk join found no match."""
    mask = df["_in_comp"].isna()
    if not mask.any():
        return pd.DataFrame()
    return (
        df.loc[mask, list(keys)]
        .drop_duplicates()
        .sort_values(list(keys))
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Legacy utilities (Excel/FINBRA path, 1989-2012)
# ---------------------------------------------------------------------------


def build_ibge_id_1998_2012(df, df_comp_municipio):
    """Construct 7-digit id_municipio from CD_UF + CD_MUN, then merge sigla_uf."""
    df = df.copy()
    df["length"] = df["CD_MUN"].str.len()
    df["id_municipio_6"] = "0"
    df.loc[df["length"] == 1, "id_municipio_6"] = (
        df["CD_UF"] + "000" + df["CD_MUN"]
    )
    df.loc[df["length"] == 2, "id_municipio_6"] = (
        df["CD_UF"] + "00" + df["CD_MUN"]
    )
    df.loc[df["length"] == 3, "id_municipio_6"] = (
        df["CD_UF"] + "0" + df["CD_MUN"]
    )
    df.loc[df["length"] == 4, "id_municipio_6"] = df["CD_UF"] + df["CD_MUN"]

    comp_reduced = df_comp_municipio[["id_municipio", "sigla_uf"]].copy()
    comp_reduced["id_municipio_6"] = comp_reduced["id_municipio"].str[:6]

    df = df.merge(comp_reduced, how="left", on="id_municipio_6")
    df = df.drop(["length", "id_municipio_6"], axis=1)
    return df


def unzip_finbra(zip_path, folder):
    """Extract finbra.csv from zip into folder. Returns path to extracted CSV."""
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extract("finbra.csv", folder)
    return os.path.join(folder, "finbra.csv")


def read_finbra_csv(csv_path):
    """Read a finbra.csv file with standard encoding/separator."""
    return pd.read_csv(
        csv_path, skiprows=3, encoding="latin1", sep=";", dtype="string"
    )


def clean_finbra_municipio(df):
    """Drop metadata cols, rename to standard municipio fields, fix valor."""
    df = df.drop(
        ["Instituição", "População", "Identificador da Conta"], axis=1
    )
    df.columns = ["id_municipio", "sigla_uf", "estagio", "conta", "valor"]
    df["valor"] = df["valor"].str.replace(",", ".", regex=False)
    return df


def clean_finbra_uf(df):
    """Drop metadata cols, rename to standard UF fields, fix valor."""
    df = df.drop(
        ["Instituição", "População", "Identificador da Conta"], axis=1
    )
    df.columns = ["id_uf", "sigla_uf", "estagio", "conta", "valor"]
    df["valor"] = df["valor"].str.replace(",", ".", regex=False)
    return df


def _apply_currency_conversion(df, ano):
    """Convert pre-1994 values from cruzeiro/cruzado to real."""
    if 1990 <= ano <= 1993:
        df["valor"] *= 1000
    if ano <= 1992:
        df["valor"] /= (1000**2) * 2.75
    elif ano == 1993:
        df["valor"] /= 1000 * 2.75
    return df


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

_created_dirs: set = set()


def _ensure_dir(path):
    if path not in _created_dirs:
        os.makedirs(path, exist_ok=True)
        _created_dirs.add(path)


def partition_and_save(df, table_name, ano, path_dados):
    """Save per-UF partition CSVs to output/, dropping sigla_uf and ano columns."""
    drop_cols = ["sigla_uf", "ano"]
    for uf in UFS:
        part = df.loc[df["sigla_uf"] == uf].drop(drop_cols, axis=1)
        out_path = os.path.join(
            path_dados,
            f"output/{table_name}/ano={ano}/sigla_uf={uf}/{table_name}.csv",
        )
        _ensure_dir(os.path.dirname(out_path))
        part.to_csv(out_path, index=False, encoding="utf-8", na_rep="")


def partition_and_save_brasil(df, table_name, ano, path_dados):
    """Save brasil-level CSV to output/ (no UF partition), dropping ano column."""
    out_path = os.path.join(
        path_dados,
        f"output/{table_name}/ano={ano}/{table_name}.csv",
    )
    _ensure_dir(os.path.dirname(out_path))
    df.drop("ano", axis=1).to_csv(
        out_path, index=False, encoding="utf-8", na_rep=""
    )


# ---------------------------------------------------------------------------
# Parallel worker infrastructure (used by code/build.py)
# ---------------------------------------------------------------------------

# Module-level comp cache — populated once per worker process by _init_worker.
_comp = None


def _init_worker(code_dir, path_queries):
    """Initializer for ProcessPoolExecutor workers.

    Runs once per worker process. Inserts code_dir into sys.path (needed when
    the OS uses 'spawn' to create worker processes, e.g. macOS Python 3.8+)
    and loads the crosswalk tables into the module-level _comp cache.
    """
    if code_dir not in sys.path:
        sys.path.insert(0, code_dir)
    global _comp, _created_dirs
    _comp = load_crosswalk(path_queries)
    _created_dirs = set()


def process_year_task(args):
    """Process one year: load data, run all table builders, return unmatched rows.

    This is a top-level function so it is pickleable by multiprocessing.

    Args:
        args: (ano, api_dir, path_dados, path_queries, table_configs)
            table_configs: list of (table_name, first_year, last_year, comp_file)

    Returns:
        (ano, {comp_file: combined_unmatched_DataFrame})
    """
    ano, api_dir, path_dados, path_queries, table_configs = args

    needed_levels = set()
    for table_name, _, _, _ in table_configs:
        if table_name.startswith("municipio_"):
            needed_levels.add("municipio")
        elif table_name.startswith("uf_"):
            needed_levels.add("uf")
        elif table_name.startswith("brasil_"):
            needed_levels.add("brasil")

    year_data = load_year_data(ano, api_dir, levels=needed_levels or None)
    unmatched_by_comp = {}

    for table_name, first_year, last_year, comp_file in table_configs:
        if not (first_year <= ano <= last_year):
            continue
        mod = importlib.import_module(f"tables_final.{table_name}")
        unmatched = mod.build(path_dados, path_queries, _comp, year_data, ano)
        if unmatched is not None and not unmatched.empty:
            unmatched_by_comp.setdefault(comp_file, []).append(unmatched)

    consolidated = {
        cf: pd.concat(dfs, ignore_index=True).drop_duplicates()
        for cf, dfs in unmatched_by_comp.items()
    }
    return ano, consolidated
