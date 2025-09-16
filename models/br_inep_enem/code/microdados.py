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


def read_csv_enem():
    global valor
    for df in pd.read_csv(
        "/home/tricktx/dados/br_inep_enem/Microdados ENEM 2023/DADOS/MICRODADOS_ENEM_2023.csv",
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
            "TP_ESCOLA": "tipo_escola",
            "TP_ENSINO": "ensino",
            "IN_TREINEIRO": "indicador_treineiro",
            "CO_MUNICIPIO_ESC": "id_municipio_escola",
            "SG_UF_ESC": "sigla_uf_escola",
            "TP_DEPENDENCIA_ADM_ESC": "dependencia_administrativa_escola",
            "TP_LOCALIZACAO_ESC": "localizacao_escola",
            "TP_SIT_FUNC_ESC": "situacao_funcionamento_escola",
            "CO_MUNICIPIO_PROVA": "id_municipio_prova",
            "SG_UF_PROVA": "sigla_uf_prova",
            "TP_PRESENCA_CN": "presenca_ciencias_natureza",
            "TP_PRESENCA_CH": "presenca_ciencias_humanas",
            "TP_PRESENCA_LC": "presenca_linguagens_codigos",
            "TP_PRESENCA_MT": "presenca_matematica",
            "CO_PROVA_CN": "tipo_prova_ciencias_natureza",
            "CO_PROVA_CH": "tipo_prova_ciencias_humanas",
            "CO_PROVA_LC": "tipo_prova_linguagens_codigos",
            "CO_PROVA_MT": "tipo_prova_matematica",
            "NU_NOTA_CN": "nota_ciencias_natureza",
            "NU_NOTA_CH": "nota_ciencias_humanas",
            "NU_NOTA_LC": "nota_linguagens_codigos",
            "NU_NOTA_MT": "nota_matematica",
            "TX_RESPOSTAS_CN": "respostas_ciencias_natureza",
            "TX_RESPOSTAS_CH": "respostas_ciencias_humanas",
            "TX_RESPOSTAS_LC": "respostas_linguagens_codigos",
            "TX_RESPOSTAS_MT": "respostas_matematica",
            "TX_GABARITO_CN": "gabarito_ciencias_natureza",
            "TX_GABARITO_CH": "gabarito_ciencias_humanas",
            "TX_GABARITO_LC": "gabarito_linguagens_codigos",
            "TX_GABARITO_MT": "gabarito_matematica",
            "TP_LINGUA": "lingua_estrangeira",
            "TP_STATUS_REDACAO": "presenca_redacao",
            "NU_NOTA_COMP1": "nota_redacao_competencia_1",
            "NU_NOTA_COMP2": "nota_redacao_competencia_2",
            "NU_NOTA_COMP3": "nota_redacao_competencia_3",
            "NU_NOTA_COMP4": "nota_redacao_competencia_4",
            "NU_NOTA_COMP5": "nota_redacao_competencia_5",
            "NU_NOTA_REDACAO": "nota_redacao",
        }
        df = df.rename(columns=rename)

        lista = [
            "id_inscricao",
            "ano",
            "faixa_etaria",
            "sexo",
            "id_municipio_residencia",
            "sigla_uf_residencia",
            "estado_civil",
            "cor_raca",
            "nacionalidade",
            "situacao_conclusao",
            "ano_conclusao",
            "tipo_escola",
            "ensino",
            "indicador_treineiro",
            "id_municipio_escola",
            "sigla_uf_escola",
            "dependencia_administrativa_escola",
            "localizacao_escola",
            "situacao_funcionamento_escola",
            "indicador_certificado",
            "nome_certificadora",
            "sigla_uf_certificadora",
            "id_municipio_prova",
            "sigla_uf_prova",
            "presenca_objetiva",
            "tipo_prova_objetiva",
            "nota_objetiva_competencia_1",
            "nota_objetiva_competencia_2",
            "nota_objetiva_competencia_3",
            "nota_objetiva_competencia_4",
            "nota_objetiva_competencia_5",
            "nota_objetiva",
            "respostas_objetiva",
            "gabarito_objetiva",
            "presenca_ciencias_natureza",
            "presenca_ciencias_humanas",
            "presenca_linguagens_codigos",
            "presenca_matematica",
            "tipo_prova_ciencias_natureza",
            "tipo_prova_ciencias_humanas",
            "tipo_prova_linguagens_codigos",
            "tipo_prova_matematica",
            "nota_ciencias_natureza",
            "nota_ciencias_humanas",
            "nota_linguagens_codigos",
            "nota_matematica",
            "respostas_ciencias_natureza",
            "respostas_ciencias_humanas",
            "respostas_linguagens_codigos",
            "respostas_matematica",
            "gabarito_ciencias_natureza",
            "gabarito_ciencias_humanas",
            "gabarito_linguagens_codigos",
            "gabarito_matematica",
            "lingua_estrangeira",
            "presenca_redacao",
            "nota_redacao_competencia_1",
            "nota_redacao_competencia_2",
            "nota_redacao_competencia_3",
            "nota_redacao_competencia_4",
            "nota_redacao_competencia_5",
            "nota_redacao",
            "indicador_questionario_socioeconomico",
        ]
        for col in lista:
            if col not in df_lista.columns:  # noqa: F821
                df_lista[col] = str(np.nan)  # noqa: F821

        for x in df_lista.columns:  # noqa: F821
            df_lista[x] = df_lista[x].apply(  # noqa: F821
                lambda x: str(x).replace(".0", "").replace("nan", "")
            )

        df_lista = df_lista[  # noqa: F821
            [
                "ano",
                "id_inscricao",
                "faixa_etaria",
                "sexo",
                "id_municipio_residencia",
                "sigla_uf_residencia",
                "estado_civil",
                "cor_raca",
                "nacionalidade",
                "situacao_conclusao",
                "ano_conclusao",
                "tipo_escola",
                "ensino",
                "indicador_treineiro",
                "id_municipio_escola",
                "sigla_uf_escola",
                "dependencia_administrativa_escola",
                "localizacao_escola",
                "situacao_funcionamento_escola",
                "indicador_certificado",
                "nome_certificadora",
                "sigla_uf_certificadora",
                "id_municipio_prova",
                "sigla_uf_prova",
                "presenca_objetiva",
                "tipo_prova_objetiva",
                "nota_objetiva_competencia_1",
                "nota_objetiva_competencia_2",
                "nota_objetiva_competencia_3",
                "nota_objetiva_competencia_4",
                "nota_objetiva_competencia_5",
                "nota_objetiva",
                "respostas_objetiva",
                "gabarito_objetiva",
                "presenca_ciencias_natureza",
                "presenca_ciencias_humanas",
                "presenca_linguagens_codigos",
                "presenca_matematica",
                "tipo_prova_ciencias_natureza",
                "tipo_prova_ciencias_humanas",
                "tipo_prova_linguagens_codigos",
                "tipo_prova_matematica",
                "nota_ciencias_natureza",
                "nota_ciencias_humanas",
                "nota_linguagens_codigos",
                "nota_matematica",
                "respostas_ciencias_natureza",
                "respostas_ciencias_humanas",
                "respostas_linguagens_codigos",
                "respostas_matematica",
                "gabarito_ciencias_natureza",
                "gabarito_ciencias_humanas",
                "gabarito_linguagens_codigos",
                "gabarito_matematica",
                "lingua_estrangeira",
                "presenca_redacao",
                "nota_redacao_competencia_1",
                "nota_redacao_competencia_2",
                "nota_redacao_competencia_3",
                "nota_redacao_competencia_4",
                "nota_redacao_competencia_5",
                "nota_redacao",
                "indicador_questionario_socioeconomico",
            ]
        ]

        to_partitions(
            data=df_lista,
            partition_columns=["ano"],
            savepath="/home/tricktx/dados/br_inep_enem/main/",
            file_type="csv",
        )


read_csv_enem()
