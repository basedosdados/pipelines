# ruff: noqa: RUF001
# %%
import os

import basedosdados as bd
import pandas as pd

INPUT = os.path.join("models", "br_inep_educacao_especial", "data")
OUTPUT = os.path.join("models", "br_inep_educacao_especial", "output")

os.makedirs(INPUT, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


# %%
def read_sheet(
    df: pd.ExcelFile, sheet_name: str, skiprows: int
) -> pd.DataFrame:
    return pd.read_excel(
        df,
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


# %%
# Load the Excel file into a pandas ExcelFile object
excel_data = pd.ExcelFile(os.path.join(INPUT, "txa-21-22-23.xlsx"))

# Get the sheet names
print(excel_data.sheet_names)

# %%
df = excel_data.parse()

# %%
# Print the column names of the DataFrame to see what was read from the Excel sheet
print(df.columns)

# %%
# -----------------------------
# Rename and filter columns
# -----------------------------
# This block renames the DataFrame columns according to the RENAME_COLUMNS dictionary
# and keeps only the renamed columns. It overwrites the original df variable, so
# df will contain only the columns specified in RENAME_COLUMNS.

RENAME_COLUMNS = {
    "NU_ANO_CENSO": "ano",
    "NO_REGIAO": "regiao",
    "1_CAT_FUN_AI": "taxaaprovacao_anosiniciais",
    "1_CAT_FUN_AF": "taxaaprovacao_anosfinais",
    "1_CAT_MED": "taxaaprovacao_ensinomedio",
    "2_CAT_FUN_AI": "taxareprovacao_anosiniciais",
    "2_CAT_FUN_AF": "taxareprovacao_anosfinais",
    "2_CAT_MED": "taxareprovacao_ensinomedio",
    "3_CAT_FUN_AI": "taxaabandono_anosiniciais",
    "3_CAT_FUN_AF": "taxaabandono_anosfinais",
    "3_CAT_MED": "taxaabandono_ensinomedio",
}


def keep_only_renamed(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME_COLUMNS)

    cols_keep = list(RENAME_COLUMNS.values())

    cols_existentes = [col for col in cols_keep if col in df.columns]

    return df[cols_existentes]


df = keep_only_renamed(df)
print(df.columns)

# %%
# Filters only years equal to or greater than 2022
df = df[df["ano"] >= 2022]

# %%
# Filters the DataFrame to keep only rows where 'regiao' is "Brasil"
# and melts the DataFrame from wide to long format (one row per metric)
# Each row will have: 'ano', 'regiao', 'metrica' (original metric name), and 'valor' (corresponding value)
melted_dataframe = pd.concat(
    [
        df.pipe(lambda d: d.loc[(d["regiao"] == "Brasil")]).pipe(
            lambda d: pd.melt(
                d,
                id_vars=["ano", "regiao"],
                value_vars=d.columns.difference(
                    ["ano", "regiao"]
                ).tolist(),  # Convert to list
                var_name="metrica",
                value_name="valor",
            )
        )
    ]
)


# %%
melted_dataframe["etapa_ensino"] = melted_dataframe["metrica"].apply(
    lambda v: v.split("_")[-1]
)  # Extracts 'anosiniciais', 'anosfinais', or 'ensinomedio'
melted_dataframe["tipo_metrica"] = melted_dataframe["metrica"].apply(
    lambda v: v.split("_")[0]
)  # Extracts 'taxaaprovacao', 'taxareprovacao', 'taxaabandono'
melted_dataframe["valor"] = pd.to_numeric(
    melted_dataframe["valor"], errors="coerce"
)

# Pivoting the melted DataFrame to get desired columns
df_final = melted_dataframe.pivot_table(
    index=["ano", "regiao", "etapa_ensino"],
    columns="tipo_metrica",
    values="valor",
).reset_index()

# %%
# Dictionary used to rename columns in the melted DataFrame
# to a more standardized format.
RENAME_COLUMNS_MELTED = {
    "taxaabandono": "taxa_abandono",
    "taxaaprovacao": "taxa_aprovacao",
    "taxareprovacao": "taxa_reprovacao",
}
# Dictionary mapping shorthand stage names of education
# to their full descriptive names.
etapa_ensino = {
    "anosiniciais": "Ensino Fundamental – Anos Iniciais",
    "anosfinais": "Ensino Fundamental – Anos Finais",
    "ensinomedio": "Ensino Médio Regular",
}

# %%
# Rename columns in df_final using the mapping defined in RENAME_COLUMNS_MELTED
df_final = df_final.rename(columns=RENAME_COLUMNS_MELTED)
# Replace shorthand values in the 'etapa_ensino' column
# with their full descriptive names using the etapa_ensino dictionary
df_final["etapa_ensino"] = df_final["etapa_ensino"].replace(etapa_ensino)

# %%
# Rename the 'sigla' column to 'regiao' and drop the 'nome' column
df_final = df_final.drop(columns=["regiao"])

# %%
# Select and keep only the specified columns from df_final
# This ensures the DataFrame contains only the relevant variables for analysis
df_final = df_final[
    [
        "ano",
        "etapa_ensino",
        "taxa_aprovacao",
        "taxa_reprovacao",
        "taxa_abandono",
    ]
]

# %%
# Define the output file path by joining the OUTPUT directory with a subfolder name
path = os.path.join(OUTPUT, "educacao_especial_brasil_taxa_rendimento")
# Create the directory if it does not already exist
os.makedirs(path, exist_ok=True)
# Convert all values in df_final to string (astype(str)),
# then save it as a CSV file inside the specified folder.
df_final.astype(str).to_csv(os.path.join(path, "2022-2023.csv"), index=False)

# %%
# Read a table directly from BigQuery into a pandas DataFrame using the basedosdados library.
# The SQL query selects all columns from the table:
#   basedosdados.br_inep_educacao_especial.uf_taxa_rendimento
# The parameter billing_project_id specifies which GCP project will be billed for the query.
df_bq = bd.read_sql(
    "select * from basedosdados.br_inep_educacao_especial.brasil_taxa_rendimento",
    billing_project_id="basedosdados-dev",
)

# %%
# Concatenate two DataFrames.
df_updated = pd.concat([df_bq, df_final])

# %%
# Convert all values in df_updated to strings and save as a CSV file.
df_updated.astype(str).to_csv(
    os.path.join(path, "brasil_taxa_rendimento.csv"), index=False
)

# %%
# Create a Table object representing a BigQuery table in the specified dataset.
tb_brasil = bd.Table(
    dataset_id="br_inep_educacao_especial", table_id="brasil_taxa_rendimento"
)
# Upload the local CSV file to the BigQuery table.
# Parameters:
# - if_storage_data_exists='replace': replace the data in storage if it already exists
# - if_table_exists='replace': replace the table if it already exists
# - source_format='csv': specify that the source file is a CSV
tb_brasil.create(
    os.path.join(path, "brasil_taxa_rendimento.csv"),
    if_storage_data_exists="replace",
    if_table_exists="replace",
    source_format="csv",
)
