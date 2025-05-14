# -*- coding: utf-8 -*-
import logging
import re

import basedosdados as bd
import pandas as pd
import requests
from constants import constants
from tqdm import tqdm
from unidecode import unidecode


def municipalities_as_chunks(chunk_size: int = 50):
    query = """
    SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    df_municipios = bd.read_sql(query, billing_project_id="basedosdados-dev")
    df_municipios = bd.read_sql(query, billing_project_id="basedosdados-dev")
    input_list = list(df_municipios.id_municipio.unique())

    return [
        input_list[i : i + chunk_size]
        for i in range(0, len(input_list), chunk_size)
    ]


def sidra_to_dataframe(url: str) -> pd.DataFrame:
    try:
        response = requests.get(url=url)
        if response.status_code >= 400 and response.status_code <= 599:
            logging.info(f"Tabela grande demais: {url}")
            raise Exception(
                f"Erro de requisição: status code {response.raise_for_status()}"
            )
            raise Exception(
                f"Erro de requisição: status code {response.raise_for_status()}"
            )
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
    return pd.json_normalize(response.json())


def rename_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.loc[0, :].values.flatten().tolist()
    return df.iloc[1:, :]


def prepare_columns_for_bigquery(df):
    df.columns = [
        re.sub(r"\W+", "_", unidecode(col)).lower() for col in df.columns
    ]

    return df


if __name__ == "__main__":
    tables = pd.read_csv(
        "/home/laura/Documents/conjuntos/br_ibge_censo_2022/tabelas.csv",
        encoding="utf-16",
    )
    dataset_id = "br_ibge_censo_2022"

    selected_tables = tables[tables["Status"] == "arquitetura"]

    for _, table in selected_tables.iterrows():
        print(table)
        table_id = table["novo_nome"]
        table_url = constants.URLS.value[table_id]
        logging.info(f"Baixando dados da tabela: {table_id}")
        df_final = pd.DataFrame()
        try:
            df = sidra_to_dataframe(table_url)
            df = prepare_columns_for_bigquery(df)
            df.to_parquet(
                path=f"conjuntos/br_ibge_censo_2022/favela_comunidade_urbana/{table_id}.parquet",
                compression="gzip",
            )
        except:  # noqa: E722
            output_list = municipalities_as_chunks()
            logging.info(f"Baixando dados em chunks da tabela: {table_id}")
            for n in tqdm(range(len(output_list))):
                munis = ""
                munis += "".join(
                    f"{value}" if i == 0 else f",{value}"
                    for i, value in enumerate(output_list[n])
                )
                url_nova = re.split(r"all(?=/v/)", table_url)
                df = sidra_to_dataframe(
                    url=f"{url_nova[0]}{munis}{url_nova[1]}"
                )
                df = prepare_columns_for_bigquery(df)
                df = prepare_columns_for_bigquery(df)
                df_final = pd.concat([df_final, df])
                break
            df.to_parquet(path=f"{table_id}.parquet", compression="gzip")
            print(df.columns)

        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        tb.create(
            path=f"{table_id}.parquet",
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="replace",
        )
        break
        df.to_parquet(path=f"{table_id}.parquet", compression="gzip")
        print(df.columns)

        tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
        tb.create(
            path=f"{table_id}.parquet",
            source_format="parquet",
            if_table_exists="replace",
            if_storage_data_exists="replace",
        )
