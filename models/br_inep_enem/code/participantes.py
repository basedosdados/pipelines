import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


def to_partitions(
    data: pd.DataFrame,
    partition_columns: list[str],
    savepath: str,
    file_type: str = "csv",
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions.
        file_type (str): default to csv. Accepts parquet.
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/',
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)
        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            # .astype(str)
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)

            if file_type == "csv":
                # append data to csv
                file_filter_save_path = Path(filter_save_path) / "data.csv"
                df_filter.to_csv(
                    file_filter_save_path,
                    sep=",",
                    encoding="utf-8",
                    na_rep="",
                    index=False,
                    mode="a",
                    header=not file_filter_save_path.exists(),
                )
            elif file_type == "parquet":
                # append data to parquet
                file_filter_save_path = Path(filter_save_path) / "data.parquet"
                df_filter.to_parquet(
                    file_filter_save_path, index=False, compression="gzip"
                )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


valor = 0

caminho_leitura = (
    "/content/drive/MyDrive/conjuntos/dados_brutos/br_inep_enem/microdados/"
)


def read_csv_enem():
    global valor
    for df in pd.read_csv(
        caminho_leitura + "PARTICIPANTES_2024.csv",
        sep=";",
        encoding="latin1",
        chunksize=100000,
    ):
        valor = valor + 1
        print(valor)
        rename = {
            "NU_INSCRICAO": "id_inscricao",
            "NU_ANO": "ano",
            "TP_FAIXA_ETARIA": "faixa_etaria",
            "TP_SEXO": "sexo",
            "TP_ESTADO_CIVIL": "estado_civil",
            "TP_COR_RACA": "cor_raca",
            "TP_NACIONALIDADE": "nacionalidade",
            "TP_ST_CONCLUSAO": "situacao_conclusao",
            "TP_ANO_CONCLUIU": "ano_conclusao",
            "TP_ENSINO": "ensino",
            "IN_TREINEIRO": "indicador_treineiro",
            "CO_MUNICIPIO_PROVA": "id_municipio_prova",
            "SG_UF_PROVA": "sigla_uf_prova",
        }
        df_lista = df.rename(columns=rename)

        lista = [
            "id_inscricao",
            "ano",
            "faixa_etaria",
            "sexo",
            "estado_civil",
            "cor_raca",
            "nacionalidade",
            "situacao_conclusao",
            "ano_conclusao",
            "ensino",
            "indicador_treineiro",
            "id_municipio_prova",
            "sigla_uf_prova",
        ]
        for col in lista:
            if col not in df_lista.columns:
                df_lista[col] = str(np.nan)

        # todas string com exceção de indicador_treineiro, que é boolean
        for x in df_lista.columns:
            if x != "indicador_treineiro":
                df_lista[x] = df_lista[x].apply(
                    lambda v: str(v).replace(".0", "").replace("nan", "")
                )

        df_lista["indicador_treineiro"] = (
            df_lista["indicador_treineiro"]
            .replace("", np.nan)
            .astype("float", errors="ignore")
            .astype("boolean")
        )

        df_lista = df_lista[
            [
                "ano",
                "id_inscricao",
                "faixa_etaria",
                "sexo",
                "estado_civil",
                "cor_raca",
                "nacionalidade",
                "situacao_conclusao",
                "ano_conclusao",
                "ensino",
                "indicador_treineiro",
                "id_municipio_prova",
                "sigla_uf_prova",
            ]
        ]

        to_partitions(
            data=df_lista,
            partition_columns=["ano"],
            savepath="/content/drive/MyDrive/conjuntos/br_inep_enem/participantes/",
            file_type="csv",
        )


read_csv_enem()
