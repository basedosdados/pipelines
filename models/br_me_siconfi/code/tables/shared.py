import os
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


def split_conta_portaria_2013_2017(conta_str):
    """Split '1.2.3.4.5.00.00 - Conta Name' into (portaria, conta)."""
    if conta_str and conta_str[0].isnumeric():
        parts = conta_str.split(" -", 1)
        portaria = parts[0].strip()
        conta = parts[1].strip() if len(parts) > 1 else ""
    else:
        portaria = ""
        conta = conta_str
    return portaria, conta


def split_conta_portaria_2018_plus(conta_str):
    """Split fixed-width format used from 2018 onward."""
    if conta_str and conta_str[0].isnumeric():
        portaria = conta_str[:14].strip()
        if "0 -" in conta_str:
            conta = conta_str[16:].strip()
        else:
            conta = conta_str[15:].strip()
    else:
        portaria = ""
        conta = conta_str
    return portaria, conta


def apply_conta_split(df, ano):
    """Add portaria column by splitting conta, in-place on df."""
    if ano <= 2017:
        split_fn = split_conta_portaria_2013_2017
    else:
        split_fn = split_conta_portaria_2018_plus

    portarias = []
    contas = []
    for val in df["conta"]:
        p, c = split_fn(str(val) if val else "")
        portarias.append(p)
        contas.append(c)
    df["portaria"] = pd.array(portarias, dtype="string")
    df["conta"] = pd.array(contas, dtype="string")
    df["conta"] = df["conta"].str.replace("\ufffd", "-", regex=False)
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


def partition_and_save(df, table_name, ano, path_dados, id_col="municipio"):
    """Save per-UF partition CSVs, dropping sigla_uf and ano."""
    drop_cols = ["sigla_uf", "ano"]
    for uf in UFS:
        part = df.loc[df["sigla_uf"] == uf].drop(drop_cols, axis=1)
        out_path = os.path.join(
            path_dados,
            f"output/{table_name}/ano={ano}/sigla_uf={uf}/{table_name}.csv",
        )
        part.to_csv(out_path, index=False, encoding="utf-8", na_rep="")


def setup_output_dirs(path_dados, last_year):
    """Create all output partition directories."""
    for ano in range(1989, last_year + 1):
        for uf in UFS:
            for table in [
                "municipio_receitas_orcamentarias",
                "municipio_despesas_orcamentarias",
            ]:
                os.makedirs(
                    os.path.join(
                        path_dados, f"output/{table}/ano={ano}/sigla_uf={uf}"
                    ),
                    exist_ok=True,
                )

    for ano in range(1996, last_year + 1):
        for uf in UFS:
            os.makedirs(
                os.path.join(
                    path_dados,
                    f"output/municipio_despesas_funcao/ano={ano}/sigla_uf={uf}",
                ),
                exist_ok=True,
            )

    for ano in range(1998, last_year + 1):
        for uf in UFS:
            os.makedirs(
                os.path.join(
                    path_dados,
                    f"output/municipio_balanco_patrimonial/ano={ano}/sigla_uf={uf}",
                ),
                exist_ok=True,
            )

    for ano in range(2013, last_year + 1):
        for uf in UFS:
            for table in [
                "uf_receitas_orcamentarias",
                "uf_despesas_orcamentarias",
                "uf_despesas_funcao",
            ]:
                os.makedirs(
                    os.path.join(
                        path_dados, f"output/{table}/ano={ano}/sigla_uf={uf}"
                    ),
                    exist_ok=True,
                )
