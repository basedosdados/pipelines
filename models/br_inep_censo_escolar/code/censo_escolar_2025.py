# %% [markdown]
# Script para subir os dados do censo escolar de 2025
#
# Apenas a tabela `escola`
#

# %%
import csv
import zipfile
from io import StringIO
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
import requests

# %%
INPUT = Path("input") / "br_inep_censo_escolar_2025"
OUTPUT = Path("output") / "br_inep_censo_escolar_2025"

# %%
INPUT.mkdir(exist_ok=True, parents=True)
OUTPUT.mkdir(exist_ok=True, parents=True)

# %%
ZIP_FILE = INPUT / "microdados_censo_escolar_2025.zip"

# %%
with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
    zip_ref.extractall(INPUT)

# %%
csv_path = (
    INPUT
    / "microdados_censo_escolar_2025"
    / "dados"
    / "microdados_ed_basica_2025.csv"
)


# %%
def detect_delimiter(file_path: str | Path) -> str:
    with open(file_path, encoding="iso-8859-1") as file:
        sample = file.read(1024)  # Read a sample of the file
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
        return delimiter


detected_delimiter = detect_delimiter(csv_path)
print(f"The detected delimiter is: {detected_delimiter}")

# %%
censo = pd.read_csv(
    csv_path,
    delimiter=";",
    dtype="string",
    encoding="iso-8859-1",
)

# %%
censo_raw_columns = censo.columns.to_list()

# %%
print(censo.shape)

# %%
censo.head()


# %%
def apply_architecture_to_dataframe(
    df: pd.DataFrame,
    url_architecture: str,
    apply_rename_columns: bool = True,
    apply_column_order_and_selection: bool = True,
    apply_include_missing_columns: bool = True,
):
    """
    Transforms a DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        url_architecture (str): The URL of the architecture.
        apply_rename_columns (bool, optional): Flag to apply column renaming. Defaults to True.
        apply_column_order_and_selection (bool, optional): Flag to apply column order and selection. Defaults to True.
        apply_include_missing_columns (bool, optional): Flag to include missing columns. Defaults to True.

    Returns:
        pandas DataFrame: The transformed DataFrame.

    Raises:
        Exception: If an error occurs during the transformation process.
    """
    architecture = read_architecture_table(url_architecture=url_architecture)

    if apply_rename_columns:
        df = rename_columns(df, architecture)

    if apply_include_missing_columns:
        df = include_missing_columns(df, architecture)

    if apply_column_order_and_selection:
        df = column_order_and_selection(df, architecture)

    return df


def read_architecture_table(url_architecture: str) -> pd.DataFrame:
    """URL contendo a tabela de arquitetura no formato da base dos dados
    Args:
        url_architecture (str): url de tabela de arquitetura no padrão da base dos dados
    Returns:
        df: um df com a tabela de arquitetura
    """
    # Converte a URL de edição para um link de exportação em formato csv
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    df_architecture = df_architecture.query("name != '(excluido)'")

    return df_architecture.replace(np.nan, "", regex=True)


def get_order(architecture: pd.DataFrame) -> list:
    """
    Retrieves the column order from an architecture table.
    Args:
        architecture (pd.DataFrame): The architecture table containing column information.
    Returns:
        list: The list of column names representing the order.
    """

    # Return the list of column names from the 'name' column of the architecture table
    return list(architecture["name"])


def rename_columns(
    df: pd.DataFrame, architecture: pd.DataFrame
) -> pd.DataFrame:
    """
    Renames the columns of a DataFrame based on an architecture table.
    Args:
        df (pd.DataFrame): The DataFrame to rename columns.
        architecture (pd.DataFrame): The architecture table containing column mappings.
    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """

    # Create a DataFrame 'aux' with unique mappings of column names from the architecture table
    aux = architecture[["name", "original_name"]].drop_duplicates(
        subset=["original_name"], keep=False
    )

    # Create a dictionary 'dict_columns' with column name mappings
    dict_columns = dict(zip(aux.original_name, aux.name, strict=False))

    # Rename columns of the DataFrame 'df' based on the dictionary 'dict_columns'
    return df.rename(columns=dict_columns)


def include_missing_columns(df, architecture):
    """
    Includes missing columns in the DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        architecture (str): The specified architecture.

    Returns:
        pandas DataFrame: The modified DataFrame with missing columns included.
    """
    df_missing_columns = missing_columns(df.columns, get_order(architecture))
    if df_missing_columns:
        df[df_missing_columns] = ""
        print(
            f"The following columns were included into the df: {df_missing_columns}"
        )
    else:
        print("No columns were included into the df")
    return df


def missing_columns(current_columns, specified_columns):
    """
    Determines the missing columns between the current columns and the specified columns.

    Args:
        current_columns (list): The list of current columns.
        specified_columns (list): The list of specified columns.

    Returns:
        list: The list of missing columns.
    """
    missing_columns = []
    for col in specified_columns:
        if col not in current_columns:
            missing_columns.append(col)

    return missing_columns


def column_order_and_selection(df, architecture):
    """
    Performs column order and selection on the DataFrame based on the specified architecture.

    Args:
        df (pandas DataFrame): The input DataFrame.
        architecture (str): The specified architecture.

    Returns:
        pandas DataFrame: The DataFrame with columns ordered and selected according to the architecture.
    """
    architecture_columns = get_order(architecture)
    list_missing_columns = missing_columns(
        current_columns=architecture_columns, specified_columns=df
    )
    if list_missing_columns:
        print(
            f"The following columns were discarded from the df: {list_missing_columns}"
        )
    else:
        print("No columns were discarded from the df")
    return df[architecture_columns]


# %%
url_architecture = "https://docs.google.com/spreadsheets/d/1WmKRJjOmcG9uFL0LaBx4EwZUA_o2VpZK2MO3hFfTnmM/edit#gid=0"

# %%
censo = apply_architecture_to_dataframe(
    df=censo,
    url_architecture=url_architecture,
    apply_rename_columns=True,
    apply_column_order_and_selection=True,
    apply_include_missing_columns=True,
)

# %%
censo.head()

# %%
bq_censo_cols = bd.read_sql(
    "select * from `basedosdados.br_inep_censo_escolar.escola` limit 0",
    billing_project_id="basedosdados-dev",
).columns.to_list()

# %%
# Colunas que devemos remover porque não estão na tabela no BQ
cols_to_drop = [i for i in censo.columns if i not in bq_censo_cols]

# %%
print(cols_to_drop)

# %%
censo: pd.DataFrame = censo.drop(columns=cols_to_drop)

# %%
assert len(bq_censo_cols) == len(censo.columns)

# %%
# Colunas nulas para adicionar porque elas não existem mais no censo de 2025
cols_to_add = [i for i in bq_censo_cols if i not in censo.columns]

if len(cols_to_add) > 0:
    censo[cols_to_add] = None

censo: pd.DataFrame = censo[bq_censo_cols]

# %%
output_dir = OUTPUT / "escola" / "ano=2025"
output_dir.mkdir(exist_ok=True, parents=True)
# %%
for sigla_uf, df_uf in censo.groupby("sigla_uf"):
    path = output_dir / f"sigla_uf={sigla_uf}"
    path.mkdir(exist_ok=True)
    df_uf.drop(columns=["ano", "sigla_uf"]).to_csv(
        path / "escola.csv", index=False
    )

# %%
tb = bd.Table(dataset_id="br_inep_censo_escolar", table_id="escola")

# %%
tb.create(OUTPUT / "escola")
