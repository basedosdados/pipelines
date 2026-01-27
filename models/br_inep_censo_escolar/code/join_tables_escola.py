"""
Esse script juntar as tabelas `escola_2023` e `escola_2024` com a tabela `escola`
"""

import shutil
from pathlib import Path

import basedosdados as bd
import pandas as pd

input = Path("input")
output = Path("output") / "br_inep_censo_escolar" / "escola"

tb_escola = bd.Table("br_inep_censo_escolar", "escola")
tb_escola_2023 = bd.Table("br_inep_censo_escolar", "escola_2023")
tb_escola_2024 = bd.Table("br_inep_censo_escolar", "escola_2024")

bq_cols_escola = tb_escola._get_columns_from_bq()
bq_cols_escola_2023 = tb_escola_2023._get_columns_from_bq()
bq_cols_escola_2024 = tb_escola_2024._get_columns_from_bq()

len(bq_cols_escola["columns"])
len(bq_cols_escola_2023["columns"])
len(bq_cols_escola_2024["columns"])

st_escola = bd.Storage("br_inep_censo_escolar", "escola")

st_escola.download(filename="*", savepath=input)

cols_2023: list[str] = [i["name"] for i in bq_cols_escola_2023["columns"]]
cols = [i["name"] for i in bq_cols_escola["columns"]]

len(cols)
len(cols_2023)

cols_to_add = set(cols_2023) - set(cols)

output.mkdir(exist_ok=True, parents=True)

for dir_year in (
    input / "staging" / "br_inep_censo_escolar" / "escola"
).iterdir():
    for uf_dir in dir_year.iterdir():
        df: pd.DataFrame = pd.read_csv(uf_dir / "escola.csv")
        for col_to_add in cols_to_add:
            if col_to_add in df.columns:
                raise Exception(
                    f"File {uf_dir} already has the column {col_to_add}"
                )
        df[list(cols_to_add)] = None
        (output / dir_year.stem / uf_dir.stem).mkdir(
            exist_ok=True, parents=True
        )
        df[cols_2023].to_csv(
            output / dir_year.stem / uf_dir.stem / "escola.csv", index=False
        )

bd.Storage("br_inep_censo_escolar", "escola_2023").download(
    filename="*", savepath=input
)
bd.Storage("br_inep_censo_escolar", "escola_2024").download(
    filename="*", savepath=input
)

for file in (
    input / "staging" / "br_inep_censo_escolar" / "escola_2023" / "ano=2023"
).iterdir():
    dest_path = output / "ano=2023" / file.stem / "escola.csv"
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.move(file / "escola_2023.csv", dest_path)

for file in (
    input / "staging" / "br_inep_censo_escolar" / "escola_2024" / "ano=2024"
).iterdir():
    dest_path = output / "ano=2024" / file.stem / "escola.csv"
    dest_path.parent.mkdir(exist_ok=True, parents=True)
    shutil.move(file / "data.csv", dest_path)


assert len(
    pd.read_csv(
        output / "ano=2022" / "sigla_uf=AC" / "escola.csv", nrows=10
    ).columns
) == len(
    pd.read_csv(
        output / "ano=2024" / "sigla_uf=AC" / "escola.csv", nrows=10
    ).columns
)

tb_escola.create(
    output, if_storage_data_exists="replace", if_table_exists="replace"
)
