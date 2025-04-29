# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
from pathlib import Path
import basedosdados as bd

from pirls_utils import LABELS_FROM_CONTEXT_QUESTIONNAIRES, COUNTRY_CODES, RENAMES

CWD = os.path.dirname(os.getcwd())
INPUT = os.path.join(CWD, "input")
TMP = os.path.join(CWD, "tmp")
OUTPUT = os.path.join(CWD, "output")

PIRLS_ASSETS_URLS = {
    "data": "https://pirls2021.org/data/downloads/P21_Data_SAS.zip",
    "item_information": "https://pirls2021.org/data/downloads/P21_ItemInformation.xlsx",
    "codebooks": "https://pirls2021.org/data/downloads/P21_Codebooks.zip",
}

PIRLS_TABLES_DESC = {
    "acg": "school_context",
    "asa": "student_achievement",
    "asg": "student_context",
    "ash": "home_context",
    "asr": "within_country_scoring_reliability",
    "ast": "student_teacher_link",
    "atg": "teacher_context",
}

TABLES_TO_PROCESS = list(PIRLS_TABLES_DESC.keys())

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

if not os.path.exists(TMP):
    os.mkdir(TMP)

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

r = requests.get(PIRLS_ASSETS_URLS["data"])
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(INPUT)

r = requests.get(PIRLS_ASSETS_URLS["codebooks"])
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(INPUT)

r = requests.get(PIRLS_ASSETS_URLS["item_information"], stream=True)
with open(f"{INPUT}/item_information.xlsx", "wb") as file:
    file.write(r.content)

# Convert sas files to parquet
for file in os.listdir(f"{INPUT}/Data"):
    parquet_filename = file.replace(".sas7bdat", ".parquet")
    parquet_output_filename = f"{TMP}/data/{parquet_filename}"
    pd.read_sas(f"{INPUT}/Data/{file}").to_parquet(parquet_output_filename)  # type: ignore


def read_pirls_files(tables: list[str]):
    files = os.listdir(f"{TMP}/data")

    result = {k: [] for k in tables}

    for file in files:
        is_normal_pirls = file.endswith("r5.parquet")
        table = file[0:3]
        df = pd.read_parquet(f"{TMP}/data/{file}")

        df["pirls_type"] = "Normal" if is_normal_pirls else "Bridge"

        if table in result:
            result[table].append(df)

    return {
        table: pd.concat(result[table], ignore_index=True) for table in result.keys()
    }


dfs = read_pirls_files(TABLES_TO_PROCESS)


def read_codebooks() -> dict[str, pd.DataFrame]:
    cols = [
        "Variable",
        "Label",
        "Level",
        "Value Scheme Detailed",
        "Decimals",
        "Comment",
    ]

    restrict_use = "In restricted-use IDB only"

    def read_sheets(table_prefix: str) -> pd.DataFrame:
        sheet_name_normal = f"{table_prefix.upper()}R5"
        sheet_name_bridge = f"{table_prefix.upper()}A5"
        normal = pd.read_excel(
            f"{INPUT}/P21_Codebook.xlsx", sheet_name=sheet_name_normal
        )[cols]
        bridge = pd.read_excel(
            f"{INPUT}/P21Br_Codebook.xlsx", sheet_name=sheet_name_bridge
        )[cols]
        normal["pirls_type"] = "Normal"
        bridge["pirls_type"] = "Bridge"
        df = pd.concat([normal, bridge])
        return (
            # ITDEV is available only in the restricted use version
            df[(df["Comment"] != restrict_use) & (df["Variable"] != "ITDEV")]
            .drop(columns=["Comment"])
            .drop_duplicates(subset=["Variable"])
        )

    return {k: read_sheets(k) for k in PIRLS_TABLES_DESC.keys()}


def get_item_informations() -> pd.DataFrame:
    sheets = ["P21 Paper", "P21 Digital", "P21Br (Paper)"]
    cols = ["Item ID", "Label"]

    dfs: list[pd.DataFrame] = [
        pd.read_excel(f"{INPUT}/item_information.xlsx", sheet_name=sheet_name)[cols]
        for sheet_name in sheets
    ]

    result = pd.concat(dfs).drop_duplicates(subset=["Item ID"])
    assert result is not None
    return result


codebooks = read_codebooks()

item_informations = get_item_informations()


# Generate architecture
def gen_archs(codebooks: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    def find_label(table, variable, label_original, label_from_item):
        if variable == "IDCNTRY":
            return (
                "Six-digit country identification code based on the ISO classification"
            )

        if pd.isnull(label_from_item):
            if (
                table in LABELS_FROM_CONTEXT_QUESTIONNAIRES
                and variable in LABELS_FROM_CONTEXT_QUESTIONNAIRES[table]
            ):
                return LABELS_FROM_CONTEXT_QUESTIONNAIRES[table][variable]
            else:
                return (
                    label_original[2:-4]
                    if label_original[1] == "\\"
                    else label_original
                )
        else:
            return label_from_item

    def get_bigquery_type(variable, level, decimals, scheme):
        if variable in [
            "IDPOP",
            "IDGRADER",
            "IDGRADE",
            "WAVE",
            "IDSCHOOL",
            "IDCLASS",
            "IDSTUD",
            "ITSEX",
            "ITADMINI",
            "ITLANG_SA",
            "LCID_SA",
            "IDBOOK",
            "ITDEV",
            "VERSION",
            "SCOPE",
        ]:
            return "string"

        if not pd.isnull(scheme) and scheme.strip() in [
            "1: Yes; 2: No",
            "0: False; 1: True",
        ]:
            return "bool"

        if decimals > 0:
            assert pd.isnull(scheme)
            return "float64"
        else:
            return "int64" if level == "Scale" else "string"

    def process_table(df: pd.DataFrame, table: str):
        order = [
            "name",
            "bigquery_type",
            "description",
            "temporal_coverage",
            "covered_by_dictionary",
            "directory_column",
            "measurement_unit",
            "has_sensitive_data",
            "observations",
            "original_name",
        ]

        df["name"] = df["Variable"].apply(
            lambda var: RENAMES[table][var] if var in RENAMES[table] else var.lower()
        )
        df = df.merge(
            item_informations, how="left", left_on="Variable", right_on="Item ID"
        )

        df["description"] = df[["Variable", "Label_x", "Label_y"]].apply(
            lambda cols: find_label(table, cols[0], cols[1], cols[2]).title().strip(),
            axis=1,
        )

        df["bigquery_type"] = df[
            ["Variable", "Level", "Decimals", "Value Scheme Detailed"]
        ].apply(lambda cols: get_bigquery_type(*cols), axis=1)

        def covered_by_dictionary(bigquery_type, scheme, variable):
            if variable == "IDCNTRY":
                return "yes"
            return "no" if bigquery_type == "bool" or pd.isnull(scheme) else "yes"

        df["covered_by_dictionary"] = df[
            ["bigquery_type", "Value Scheme Detailed", "Variable"]
        ].apply(lambda cols: covered_by_dictionary(*cols), axis=1)

        df["observations"] = None

        df["directory_column"] = None

        df["measurement_unit"] = df["Variable"].apply(
            lambda var: "person" if var == "NTEACH" else None
        )

        df["temporal_coverage"] = None
        df["has_sensitive_data"] = "no"

        df = df[df["Variable"] != "isDummy"]

        country_iso3_code = {
            "name": "country_iso3_code",
            "bigquery_type": "string",
            "description": "Valid Country ISO3 Code",
            "temporal_coverage": None,
            "covered_by_dictionary": "no",
            "directory_column": "br_bd_diretorios_mundo_pais:id_pais_m49",
            "measurement_unit": None,
            "has_sensitive_data": "no",
            "observations": None,
            "original_name": None,
        }

        pirls_type_row = {
            "name": "pirls_type",
            "bigquery_type": "string",
            "description": "Indicates if the record is from PIRLS Bridge or PIRLS Normal",
            "temporal_coverage": None,
            "covered_by_dictionary": "no",
            "directory_column": None,
            "measurement_unit": None,
            "has_sensitive_data": "no",
            "observations": None,
            "original_name": None,
        }

        df = (
            df.drop(columns=["Label_x", "Item ID", "Label_y", "Decimals"])
            .rename(columns={"Variable": "original_name"})
            .filter(order)
        )

        return pd.concat(
            [pd.DataFrame([country_iso3_code]), df, pd.DataFrame([pirls_type_row])]
        )

    result = dict(acg=[], asa=[], asg=[], ash=[], asr=[], ast=[], atg=[])

    for table in codebooks.keys():
        df = codebooks[table]

        result[table].append(process_table(df, table))

    return {
        table: pd.concat(result[table]).drop_duplicates(subset=["name"])
        for table in result.keys()
    }  # type: ignore


tables_archs = gen_archs(codebooks)

# Save architecture tables
for table in tables_archs.keys():
    df = tables_archs[table]
    table_name = f"{CWD}/extra/architecture/{PIRLS_TABLES_DESC[table]}_{table}.xlsx"
    df.to_excel(table_name, index=False)


def get_types_cols(table: str) -> dict[str, str]:
    types = tables_archs[table][["original_name", "bigquery_type"]].to_dict(
        orient="split", index=False
    )["data"]
    return {key: typ for (key, typ) in types if key is not None}


def clean_data(df: pd.DataFrame, table: str):
    types_cols = {k: v for (k, v) in get_types_cols(table).items() if k in df.columns}

    cols_bool_type = [k for (k, v) in types_cols.items() if v == "bool"]

    if table == "atg":
        df = df.drop(columns=["isDummy"])

    def to_bool(value):
        return True if value == 1 else False

    for col in cols_bool_type:
        df[col] = np.vectorize(to_bool)(df[col])

    def to_string(value):
        return str(int(value)) if not np.isnan(value) else np.nan

    for col, typ in types_cols.items():
        if typ == "string":
            df[col] = np.vectorize(to_string, otypes=[np.ndarray])(df[col])

    df.insert(0, "country_iso3_code", np.nan)

    def valid_country_code(code: str):
        return code if len(code) == 3 else np.nan

    df["country_iso3_code"] = np.vectorize(valid_country_code, otypes=[np.ndarray])(
        df["IDCNTRY"]
    )

    return df.rename(columns=RENAMES[table]).rename(columns=str.lower)


# Clean data and save
for table in TABLES_TO_PROCESS:
    table_name = f"{OUTPUT}/{PIRLS_TABLES_DESC[table]}_{table}.parquet"
    df = clean_data(dfs[table], table)
    df.to_parquet(table_name, index=False)


def parse_scheme(table: str, variable: str) -> list[tuple[str, str]]:
    # O valor na celula ta em outro formato
    if variable == "IDPOP":
        return [("1", "1")]

    if variable == "IDCNTRY":
        return [(k, v) for k, v in COUNTRY_CODES.items()]

    df = codebooks[table]
    value = df[df["Variable"] == variable]["Value Scheme Detailed"].values[0].strip()

    def sanitize(value: str) -> tuple[str, str]:
        head, *tail = value.split(":")
        head = head.strip()
        rest = "".join(tail).strip()
        return head, rest

    if not ";" in value:
        return [sanitize(value)]

    return [sanitize(i) for i in value.split(";") if ":" in i]


# Build dictionary table
def build_dict(table: str) -> pd.DataFrame:
    table_arch = pd.DataFrame.copy(
        tables_archs[table][["name", "covered_by_dictionary", "original_name"]]
    )
    cols_covered_by_dict = table_arch[table_arch["covered_by_dictionary"] == "yes"]

    cols_covered_by_dict["table_id"] = PIRLS_TABLES_DESC[table]
    cols_covered_by_dict["temporal_coverage"] = 2021
    cols_covered_by_dict["schema_values"] = cols_covered_by_dict["original_name"].apply(
        lambda variable: parse_scheme(table, variable)
    )
    cols_covered_by_dict = cols_covered_by_dict.explode("schema_values")
    cols_covered_by_dict["key"] = cols_covered_by_dict["schema_values"].apply(
        lambda schema: schema[0]
    )
    cols_covered_by_dict["value"] = cols_covered_by_dict["schema_values"].apply(
        lambda schema: schema[1]
    )

    return cols_covered_by_dict.drop(
        ["covered_by_dictionary", "schema_values", "original_name"], axis=1
    ).rename(columns={"name": "column_name"})[
        ["table_id", "column_name", "key", "temporal_coverage", "value"]
    ]


# Save dictionary table
dictionary = pd.concat(
    [build_dict(table_name) for table_name in PIRLS_TABLES_DESC.keys()]
)
dictionary.to_parquet(f"{OUTPUT}/dictionary.parquet", index=False)

dictionary.to_excel(f"{CWD}/extra/architecture/dictionary.xlsx", index=False)

ds = bd.Dataset(dataset_id="world_iea_pirls")

if not ds.exists():
    ds.create(mode="staging")

# Upload to BQ
for table_suffix_id, table_name in PIRLS_TABLES_DESC.items():
    path = f"{OUTPUT}/{table_name}_{table_suffix_id}.parquet"

    tb = bd.Table(dataset_id="world_iea_pirls", table_id=table_name)

    tb.create(
        path=path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        source_format="parquet",
    )


# Upload dictionary
tb = bd.Table(dataset_id="world_iea_pirls", table_id="dictionary")

tb.create(
    path=f"{OUTPUT}/dictionary.parquet",
    if_table_exists="replace",
    if_storage_data_exists="replace",
    source_format="parquet",
)

# Upload auxiliares files
st = bd.Storage(dataset_id="world_iea_pirls", table_id="")

if not os.path.exists(f"{CWD}/extra/auxiliary_files/"):
    os.mkdir(f"{CWD}/extra/auxiliary_files/")

with zipfile.ZipFile(
    f"{CWD}/extra/auxiliary_files/auxiliary_files.zip", "w", zipfile.ZIP_DEFLATED
) as zipf:
    for file in [
        Path(f"{INPUT}/{file}")
        for file in [
            "item_information.xlsx",
            "P21_Codebook.xlsx",
            "P21Br_Codebook.xlsx",
        ]
    ]:
        zipf.write(file, file.relative_to(Path(INPUT)))

st.upload(
    f"{CWD}/extra/auxiliary_files/auxiliary_files.zip",
    mode="auxiliary_files",
)
