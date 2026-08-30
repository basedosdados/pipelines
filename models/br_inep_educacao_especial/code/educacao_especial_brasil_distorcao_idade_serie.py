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
def read_sheet(sheet_name: str, skiprows: int = 3) -> pd.DataFrame:
    return pd.read_excel(
        os.path.join(INPUT, "TDI_ANO_2020_21_22_23_24.xlsx"),
        skiprows=skiprows,
        sheet_name=sheet_name,
    )


# %%
# Load the Excel file into a pandas ExcelFile object
excel_data = pd.ExcelFile(os.path.join(INPUT, "TDI_ANO_2020_21_22_23_24.xlsx"))

# Get the sheet names
print(excel_data.sheet_names)

# %%
# Parse the Excel file into a DataFrame.
# If no sheet name is specified, it loads the first sheet by default.
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
    "NO_CATEGORIA": "categoria",
    "NO_REGIAO": "regiao",
    "TP_TIPO_CLASSE": "classe",
    "NO_DEPENDENCIA": "dependencia",
    "FUN_AI_CAT_0": "Ensino Fundamental – Anos Iniciais",
    "FUN_AF_CAT_0": "Ensino Fundamental – Anos Finais",
    "MED_CAT_0": "Ensino Médio Regular",
}


def keep_only_renamed(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME_COLUMNS)

    cols_keep = list(RENAME_COLUMNS.values())

    cols_existentes = [col for col in cols_keep if col in df.columns]

    return df[cols_existentes]


df = keep_only_renamed(df)
print(df.columns)

# %%
# Filter the DataFrame 'df' to keep only rows that meet all of the following conditions:
df = df[
    (df["ano"] >= 2022)  # Year is 2022 or later
    & (
        df["classe"] != "0 - Todas as turmas"
    )  # Exclude rows where 'classe' equals "0 - Todas as turmas"
    & (
        df["categoria"] == "Modalidade: educação especial"
    )  # Keep only rows for special education
    & (
        df["dependencia"] == "Total"
    )  # Include only rows where 'dependencia' is "Total"
    & (df["regiao"] == "Brasil")  # Include only rows for the whole country
]

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
                value_name="tdi",
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
)  # Extracts 'tdi'
melted_dataframe["tdi"] = pd.to_numeric(
    melted_dataframe["tdi"], errors="coerce"
)

# Pivoting the melted DataFrame to get desired columns
df_final = melted_dataframe.pivot_table(
    index=["ano", "regiao", "etapa_ensino"],
    columns="tipo_metrica",
    values="tdi",
).reset_index()

# %%
# Remove all rows where the column 'valor' has missing (NaN) values.
melted_dataframe = melted_dataframe.dropna(subset=["tdi"])

# %%
# Dictionary used to rename columns in the melted DataFrame
# to a more standardized format.
RENAME_COLUMNS_MELTED = {"tdi": "tdi", "metrica": "etapa_ensino"}

# %%
# Select only the relevant columns for analysis
melted_dataframe = melted_dataframe[["ano", "etapa_ensino", "tdi"]]

# %%
# Define the output path by joining the OUTPUT directory with a subfolder
path = os.path.join(OUTPUT, "educacao_especial_brasil_distorcao_idade_serie")

# Create the directory if it doesn't exist (exist_ok=True avoids errors if it already exists)
os.makedirs(path, exist_ok=True)

# Convert all values in melted_dataframe to strings and save as a CSV file
# The file is named "brasil_tdi_2022_2024.csv" and will not include the DataFrame index
melted_dataframe.astype(str).to_csv(
    os.path.join(path, "brasil_tdi_2022_2024.csv"), index=False
)

# %%
# Read a table directly from BigQuery into a pandas DataFrame using the basedosdados library.
# The SQL query selects all columns from the table:
#   basedosdados.br_inep_educacao_especial.uf_taxa_rendimento
# The parameter billing_project_id specifies which GCP project will be billed for the query.
df_bq = bd.read_sql(
    "select * from basedosdados.br_inep_educacao_especial.brasil_distorcao_idade_serie",
    billing_project_id="basedosdados-dev",
)

# %%
# Concatenate two DataFrames:
df_updated = pd.concat([df_bq, melted_dataframe])

# %%
# Convert all values in df_updated to strings and save as a CSV file.
df_updated.astype(str).to_csv(
    os.path.join(path, "brasil_distorcao_idade_serie.csv"), index=False
)

# %%
# Create a Table object representing a BigQuery table in the specified dataset.
tb_brasil = bd.Table(
    dataset_id="br_inep_educacao_especial",
    table_id="brasil_distorcao_idade_serie",
)
# Upload the local CSV file to the BigQuery table.
# Parameters:
# - if_storage_data_exists='replace': replace the data in storage if it already exists
# - if_table_exists='replace': replace the table if it already exists
# - source_format='csv': specify that the source file is a CSV
tb_brasil.create(
    os.path.join(path, "brasil_distorcao_idade_serie.csv"),
    if_storage_data_exists="replace",
    if_table_exists="replace",
    source_format="csv",
)
