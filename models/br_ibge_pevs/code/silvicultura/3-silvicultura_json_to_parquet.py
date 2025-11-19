import json
import os
from collections import OrderedDict
from pathlib import Path

import pandas as pd


def parse_file(file_path):
    dict_list = []

    with open(file_path) as f:
        json_list = json.load(f)

    for json_obj in json_list:
        for id_json in json_obj:
            temp_od = OrderedDict()
            temp_variavel = id_json["id"]
            temp_unidade = id_json["unidade"]

            for resultado in id_json["resultados"]:
                temp_id_categoria = list(  # noqa: RUF015
                    resultado["classificacoes"][0]["categoria"].keys()
                )[0]

                for serie in resultado["series"]:
                    temp_id_municipio = serie["localidade"]["id"]
                    temp_valor = list(serie["serie"].values())[0]  # noqa: RUF015

                    temp_od["id_municipio"] = temp_id_municipio
                    temp_od["categoria_produto"] = (
                        df_metadados_enriquecidos.loc[
                            df_metadados_enriquecidos["id"]
                            == temp_id_categoria,
                            "categoria_produto",
                        ].to_numpy()[0]
                    )
                    temp_od["tipo_produto"] = df_metadados_enriquecidos.loc[
                        df_metadados_enriquecidos["id"] == temp_id_categoria,
                        "tipo_produto",
                    ].to_numpy()[0]
                    temp_od["subtipo_produto"] = df_metadados_enriquecidos.loc[
                        df_metadados_enriquecidos["id"] == temp_id_categoria,
                        "subtipo_produto",
                    ].to_numpy()[0]
                    temp_od["produto"] = df_metadados_enriquecidos.loc[
                        df_metadados_enriquecidos["id"] == temp_id_categoria,
                        "produto",
                    ].to_numpy()[0]
                    temp_od["variavel"] = temp_variavel
                    temp_od["unidade"] = temp_unidade
                    temp_od["valor"] = temp_valor

                    dict_list.append(dict(temp_od))
                    temp_od.clear()
    return dict_list


def split_df(df, column, filters):
    df_list = []
    for filter in filters:
        temp_df = df[df[column] == filter].copy()
        temp_df = temp_df.drop(columns=[column])
        df_list.append(temp_df)
    return df_list


def currency_fix(row):
    if row["unidade"] == "Mil Cruzados":
        return row["valor"] / (1000**2 * 2750)
    elif (
        row["unidade"] == "Mil Cruzados Novos"
        or row["unidade"] == "Mil Cruzeiros"
    ):
        return row["valor"] / (1000 * 2750)
    elif row["unidade"] == "Mil Cruzeiros Reais":
        return row["valor"] / 2750
    elif row["unidade"] == "Mil Reais":
        return row["valor"]


def transform_df(df):
    df_quantidade, df_valor = split_df(df, "variavel", ["142", "143"])
    del df

    df_quantidade = df_quantidade.rename(columns={"valor": "quantidade"})
    df_quantidade["quantidade"] = df_quantidade["quantidade"].apply(
        lambda x: x if x not in ("..", "...", "-") else None
    )
    df_quantidade["quantidade"] = df_quantidade["quantidade"].astype("Int64")

    df_valor["valor"] = df_valor["valor"].apply(
        lambda x: x if x not in ("..", "...", "-") else None
    )
    df_valor["valor"] = df_valor["valor"].astype("Float64")
    df_valor["valor"] = df_valor.apply(currency_fix, axis=1)
    df_valor["valor"] = df_valor["valor"].astype("Float64")
    df_valor = df_valor.drop(columns=["unidade"])

    temp_df = df_quantidade.merge(
        df_valor,
        left_on=[
            "id_municipio",
            "categoria_produto",
            "tipo_produto",
            "subtipo_produto",
            "produto",
        ],
        right_on=[
            "id_municipio",
            "categoria_produto",
            "tipo_produto",
            "subtipo_produto",
            "produto",
        ],
    )
    del df_quantidade
    del df_valor

    temp_df = temp_df[
        [
            "id_municipio",
            "categoria_produto",
            "tipo_produto",
            "subtipo_produto",
            "produto",
            "unidade",
            "quantidade",
            "valor",
        ]
    ]
    return temp_df


if __name__ == "__main__":
    os.makedirs("./output/silvicultura/parquet/", exist_ok=True)
    SUBPASTAS = Path("./output/silvicultura/parquet/")
    ANOS_TRANSFORMADOS = [
        int(p.name.split("=")[-1]) for p in SUBPASTAS.glob("ano=*")
    ]
    ARQUIVOS_JSON = list(Path("./output/silvicultura/json/").glob("*.json"))
    JSON_FALTANTES = [
        arquivo
        for arquivo in ARQUIVOS_JSON
        if int(arquivo.stem) not in ANOS_TRANSFORMADOS
    ]

    df_metadados_enriquecidos = pd.read_csv(
        "silvicultura_metadados_enriquecidos.csv", dtype=str
    )

    for json_path in JSON_FALTANTES:
        print(f"ANO = {json_path.stem}")
        print("Criando DataFrame com os dados do arquivo...")
        df = pd.DataFrame.from_dict(parse_file(json_path), dtype=str)
        print("Transformando o DataFrame...")
        df = transform_df(df)
        print("Exportando o DataFrame para .parquet...")
        output_path = Path(
            f"./output/silvicultura/parquet/ano={json_path.stem}"
        )
        output_path.mkdir(parents=True, exist_ok=True)
        print(df.info())
        df.to_parquet(f"{output_path}/data.parquet", index=False)
        del df
        print("Exportação concluída.")
        print()
