"""
municipio_balanco_patrimonial builder.

Input sources:
  1998-2012: input/municipio/quadro{ano}_6.xlsx (ativo) + quadro{ano}_7.xlsx (passivo)
  2013+:     input/finbra_MUN_BalancoPatrimonialDCA(AnexoI-AB)/{ano}.zip → finbra.csv

Output schema (partitioned by ano + sigla_uf, those cols dropped from CSV):
  id_municipio, portaria, conta, id_conta_bd, conta_bd, valor

Key differences from other municipio tables:
  - No estagio / estagio_bd columns
  - Merge key for 1998-2012: ["ano", "conta"]
  - Merge key for 2013+:     ["ano", "portaria"]
  - No currency adjustments (data starts 1998)
"""

import glob
import os

import pandas as pd

from .shared import (
    build_ibge_id_1998_2012,
    partition_and_save,
    unzip_finbra,
)

ORDEM_BALANCO = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "portaria",
    "conta",
    "id_conta_bd",
    "conta_bd",
    "valor",
]


def _validate_coluna(df, ano):
    """Assert Coluna field is always end-of-year; raise informative error if not."""
    expected = f"31/12/{ano}"
    unique_vals = df["Coluna"].dropna().unique().tolist()
    unexpected = [v for v in unique_vals if v != expected]
    if unexpected:
        raise ValueError(
            f"Unexpected Coluna values for ano={ano}: {unexpected}. "
            "Expected only end-of-year dates."
        )


def _read_finbra_balanco(zip_path, folder, ano):
    """Unzip, validate dates, return cleaned df with portaria+conta split."""
    csv_path = unzip_finbra(zip_path, folder)
    try:
        df = pd.read_csv(
            csv_path, skiprows=3, encoding="latin1", sep=";", dtype="string"
        )
    finally:
        os.remove(csv_path)

    # Validate that Coluna is always end-of-year
    _validate_coluna(df, ano)

    # Keep: Cod.IBGE, UF, Conta, Valor — drop the rest
    df = df.drop(
        ["Instituição", "População", "Coluna", "Identificador da Conta"],
        axis=1,
    )
    df.columns = ["id_municipio", "sigla_uf", "conta_raw", "valor"]

    df["valor"] = df["valor"].str.replace(",", ".", regex=False)

    # Split "1.0.0.0.0.00.00 - Ativo" → portaria="1.0.0.0.0.00.00", conta="Ativo"
    portarias = []
    contas = []
    for val in df["conta_raw"]:
        s = str(val) if val else ""
        if s and s[0].isnumeric() and " - " in s:
            idx = s.index(" - ")
            portarias.append(s[:idx].strip())
            contas.append(s[idx + 3 :].strip())
        else:
            portarias.append("")
            contas.append(s)
    df["portaria"] = pd.array(portarias, dtype="string")
    df["conta"] = pd.array(contas, dtype="string")
    df["conta"] = df["conta"].str.replace("\ufffd", "-", regex=False)
    df = df.drop("conta_raw", axis=1)

    return df


def build(path_dados, path_queries, comp, first_year, last_year):
    pd.options.mode.chained_assignment = None

    df_comp_municipio = comp["municipio"]
    df_comp_balanco = comp["balanco"]

    for ano in range(max(first_year, 1998), last_year + 1):
        df = pd.DataFrame(columns=ORDEM_BALANCO)

        # ------------------------------------------------------------------ #
        # 1998-2012: wide-format Excel files (ativo = file 6, passivo = file 7)
        # ------------------------------------------------------------------ #
        if ano <= 2012:
            arquivos = [
                arq
                for arq in glob.iglob(
                    os.path.join(path_dados, "input/municipio/quadro*")
                )
                if int(arq[-11:-7]) == ano and 6 <= int(arq[-6:-5]) <= 7
            ]

            # file 6 = ativo, file 7 = passivo — track categoria to resolve "Diferido" overlap
            file_categoria = {6: "ativo", 7: "passivo"}

            for arquivo in sorted(arquivos):
                file_num = int(arquivo[-6:-5])
                categoria = file_categoria.get(file_num, "")

                df_dados = pd.read_excel(arquivo, dtype="string", na_values="")

                # 2009-2012 files use CdUF/CdMun/Populacao and also have trailing
                # sigla_uf/municipio_original cols — normalize and drop metadata
                if "CdUF" in df_dados.columns:
                    df_dados = df_dados.rename(
                        columns={"CdUF": "CD_UF", "CdMun": "CD_MUN"}
                    )
                    df_dados = df_dados.drop(
                        columns=[
                            "Populacao",
                            "sigla_uf",
                            "municipio_original",
                        ],
                        errors="ignore",
                    )

                df_dados = build_ibge_id_1998_2012(df_dados, df_comp_municipio)
                df_dados = df_dados.drop_duplicates(
                    subset=["id_municipio"], keep="first"
                )

                id_vars = ["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]
                value_vars = df_dados.drop(id_vars, axis=1).columns
                df_dados = pd.melt(
                    df_dados,
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name="conta",
                    value_name="valor",
                )
                df_dados = df_dados.drop(["CD_UF", "CD_MUN"], axis=1)

                df_dados["ano"] = str(ano)
                df_dados["categoria"] = categoria

                df_dados = df_dados.merge(
                    df_comp_balanco,
                    how="left",
                    on=["ano", "categoria", "conta"],
                )
                df_dados = df_dados.drop("categoria", axis=1)
                df_dados["conta"] = df_dados["conta"].astype("string")
                df_dados = df_dados.fillna("")
                df_dados["valor"] = pd.to_numeric(
                    df_dados["valor"], errors="coerce"
                ).astype("float")

                # Keep only output columns; drops extra comp cols (n)
                df_dados = df_dados[ORDEM_BALANCO]
                df = pd.concat([df, df_dados], ignore_index=True)

        # ------------------------------------------------------------------ #
        # 2013+: finbra CSV inside zip
        # ------------------------------------------------------------------ #
        else:
            anexo = "finbra_MUN_BalancoPatrimonialDCA(AnexoI-AB)"
            zip_path = os.path.join(path_dados, f"input/{anexo}/{ano}.zip")
            folder = os.path.join(path_dados, f"input/{anexo}")

            df_dados = _read_finbra_balanco(zip_path, folder, ano)

            df_dados["ano"] = str(ano)

            df_dados = df_dados.merge(
                df_comp_balanco, how="left", on=["ano", "portaria"]
            )

            # After merge, conta may be duplicated (from df_dados and comp); keep df_dados version
            if "conta_x" in df_dados.columns:
                df_dados["conta"] = df_dados["conta_x"]
                df_dados = df_dados.drop(["conta_x", "conta_y"], axis=1)

            df_dados["conta"] = df_dados["conta"].astype("string")
            df_dados = df_dados.fillna("")
            df_dados["valor"] = pd.to_numeric(
                df_dados["valor"], errors="coerce"
            ).astype("float")

            df_dados = df_dados[ORDEM_BALANCO]
            df = pd.concat([df, df_dados], ignore_index=True)

        print(f"Particionando municipio_balanco_patrimonial {ano}")
        partition_and_save(
            df, "municipio_balanco_patrimonial", ano, path_dados
        )
