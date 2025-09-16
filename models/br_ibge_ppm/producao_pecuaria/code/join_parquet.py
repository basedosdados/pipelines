import os

import pandas as pd
import pyarrow as pa

ANOS = range(1974, 2022 + 1)

for temp_ano in ANOS:
    df_ovinos = pd.read_parquet(
        f"../source/ovinos_tosquiados/parquet/ano={temp_ano}/data.parquet"
    )
    df_vacas = pd.read_parquet(
        f"../source/vacas_ordenhadas/parquet/ano={temp_ano}/data.parquet"
    )

    print(
        f"Criando o DataFrame com os dados consolidados, referente ao ano de {temp_ano}..."
    )
    df_join = df_ovinos.merge(df_vacas, on=["sigla_uf", "id_municipio"])

    temp_export_file_path = f"../parquet/ano={temp_ano}/data.parquet"
    print(f"Exportando o DataFrame particionado em {temp_export_file_path}...")
    os.makedirs(os.path.dirname(temp_export_file_path), exist_ok=True)
    temp_schema = pa.schema(
        [
            ("sigla_uf", pa.string()),
            ("id_municipio", pa.string()),
            ("ovinos_tosquiados", pa.int64()),
            ("vacas_ordenhadas", pa.int64()),
        ]
    )
    df_join.to_parquet(temp_export_file_path, schema=temp_schema, index=False)
    print()
