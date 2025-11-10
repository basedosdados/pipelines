import gc
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
import tqdm

pd.set_option("display.max_columns", None)


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


def load_and_process_rais_estabelecimento(
    input,
    ano,
    partition_columns,
    savepath="tmp/output",
    file_type="csv",
):
    for df in tqdm.tqdm(
        pd.read_csv(
            input,
            encoding="latin1",
            sep=";",
            dtype=str,
            chunksize=1000000,
        )
    ):
        df = df.rename(
            columns={
                "Bairros SP": "bairros_sp",
                "Bairros Fortaleza": "bairros_fortaleza",
                "Bairros RJ": "bairros_rj",
                "CNAE 2.0 Classe": "cnae_2",
                "CNAE 95 Classe": "cnae_1",
                "Distritos SP": "distritos_sp",
                "Qtd Vínculos CLT": "quantidade_vinculos_clt",
                "Qtd Vínculos Ativos": "quantidade_vinculos_ativos",
                "Qtd Vínculos Estatutários": "quantidade_vinculos_estatutarios",
                "Ind Atividade Ano": "indicador_atividade_ano",
                "Ind CEI Vinculado": "indicador_cei_vinculado",
                "Ind Estab Participa PAT": "indicador_pat",
                "Ind Rais Negativa": "indicador_rais_negativa",
                "Ind Simples": "indicador_simples",
                "Município": "municipio",
                "Natureza Jurídica": "natureza_juridica",
                "Regiões Adm DF": "regioes_administrativas_df",
                "CNAE 2.0 Subclasse": "cnae_2_subclasse",
                "Tamanho Estabelecimento": "tamanho",
                "Tipo Estab": "tipo",
                "UF": "uf",
                "IBGE Subsetor": "subsetor_ibge",
                "CEP Estab": "cep",
            },
        )

        df["ano"] = ano

        df["municipio"] = df["municipio"].astype(str)

        # Carregar os arquivos

        df_municipio = bd.read_sql(
            "SELECT id_municipio, id_municipio_6 FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
            billing_project_id="basedosdados",
            reauth=False,
        )
        df_uf = bd.read_sql(
            "SELECT id_uf, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
            billing_project_id="basedosdados",
            reauth=False,
        )

        # Mescla com o arquivo de municípios
        df = df.merge(
            df_municipio,
            left_on="municipio",
            right_on="id_municipio_6",
            how="left",
        )
        df = df.drop(["id_municipio_6", "municipio"], axis=1)

        # Gerar a sigla_uf

        # Mescla com o arquivo de UFs
        df["uf"] = df["uf"].astype(str)
        df = df.merge(df_uf, left_on="uf", right_on="id_uf", how="left")
        df = df.drop(["id_uf", "uf"], axis=1)
        df = df.rename(columns={"sigla": "sigla_uf"})

        # Substitui sigla_uf vazia por "IGNORADO"
        df["sigla_uf"] = df["sigla_uf"].replace(np.nan, "IGNORADO")

        # Padronização das variáveis e dados
        for col in df.columns:
            if df[col].dtype == "str":
                df[col] = df[col].str.strip()
                df[col] = df[col].replace(["{ñ", "{ñ class}", "{ñ c"], "")

        # Lista de variáveis
        vars_list = [
            "ano",
            "sigla_uf",
            "id_municipio",
            "quantidade_vinculos_ativos",
            "quantidade_vinculos_clt",
            "quantidade_vinculos_estatutarios",
            "natureza",
            "natureza_juridica",
            "tamanho",
            "tipo",
            "indicador_cei_vinculado",
            "indicador_pat",
            "indicador_simples",
            "indicador_rais_negativa",
            "indicador_atividade_ano",
            "cnae_1",
            "cnae_2",
            "cnae_2_subclasse",
            "subsetor_ibge",
            "subatividade_ibge",
            "cep",
            "bairros_sp",
            "distritos_sp",
            "bairros_fortaleza",
            "bairros_rj",
            "regioes_administrativas_df",
        ]

        # Gera as variáveis não confirmadas
        for var in vars_list:
            if var not in df.columns:
                print(var)
                df[var] = ""

        # Limpeza adicional de variáveis
        for col in df.columns:
            if df[col].dtype == "str":
                print(col)
                df[col] = df[col].str.strip()
                df[col] = df[col].replace(
                    ["{ñ", "{ñ class}", "{ñ c", "{ñ clas}"], ""
                )

        # Limpeza para variáveis específicas
        for col in [
            "bairros_sp",
            "distritos_sp",
            "bairros_fortaleza",
            "bairros_rj",
            "distritos_sp",
            "regioes_administrativas_df",
            "cnae_2",
            "cnae_2_subclasse",
            "subsetor_ibge",
            "subatividade_ibge",
        ]:
            df[col] = df[col].replace(
                [
                    "0000",
                    "00000",
                    "000000",
                    "0000000",
                    "0000-1",
                    "000-1",
                    "998",
                    "999",
                    "9999",
                    "9997",
                    "00",
                    "-1",
                ],
                "",
            )

        # Limpeza de natureza_juridica e cep
        df["natureza_juridica"] = df["natureza_juridica"].replace(
            ["9990", "9999"], ""
        )
        df["cep"] = df["cep"].replace("0", "")

        # Ajuste na variável tipo
        df["tipo"] = df["tipo"].replace(["CNPJ", "Cnpj", "01", "1"], "1")
        df["tipo"] = df["tipo"].replace(["CAEPF", "Caepf"], "2")
        df["tipo"] = df["tipo"].replace(
            ["CEI", "Cei", "CEI/CNO", "Cei/Cno", "CNO", "Cno", "03", "3"],
            "3",
        )

        # Converte colunas para numérico
        cols_to_numeric = [
            "id_municipio",
            "quantidade_vinculos_ativos",
            "quantidade_vinculos_clt",
            "quantidade_vinculos_estatutarios",
            "tamanho",
            "indicador_cei_vinculado",
            "indicador_pat",
            "indicador_simples",
            "indicador_rais_negativa",
            "indicador_atividade_ano",
        ]
        df[cols_to_numeric] = df[cols_to_numeric].apply(
            pd.to_numeric, errors="coerce"
        )

        # Reordena as colunas
        df = df[vars_list]

        to_partitions(
            data=df,
            partition_columns=partition_columns,
            savepath=savepath,
            file_type=file_type,
        )
        del df
        gc.collect()


load_and_process_rais_estabelecimento(
    input="tmp/input/RAIS_ESTAB_PUB/RAIS_ESTAB_PUB.txt",
    partition_columns=["ano", "sigla_uf"],
    ano=2024,
)
