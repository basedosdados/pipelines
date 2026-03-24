import glob
import os

import pandas as pd

from .shared import (
    ORDEM_MUNICIPIO,
    apply_conta_split,
    build_ibge_id_1998_2012,
    clean_finbra_municipio,
    partition_and_save,
    read_finbra_csv,
    unzip_finbra,
)


def build(path_dados, path_queries, comp, first_year, last_year):
    pd.options.mode.chained_assignment = None

    df_comp_municipio = comp["municipio"]
    df_comp_receitas = comp["receitas"]

    for ano in range(first_year, last_year + 1):
        df = pd.DataFrame(columns=ORDEM_MUNICIPIO)

        arquivos = [
            arq
            for arq in glob.iglob(
                os.path.join(path_dados, "input/municipio/quadro*")
            )
            if int(arq[-11:-7]) == ano
            and (
                (ano >= 1989 and ano <= 1996 and int(arq[-6:-5]) <= 3)
                or (ano >= 1997 and ano <= 2012 and int(arq[-6:-5]) == 1)
            )
        ]

        csv_to_delete = None
        if ano >= 2013:
            anexo = "finbra_MUN_ReceitasOrcamentarias(AnexoI-C)"
            zip_path = os.path.join(path_dados, f"input/{anexo}/{ano}.zip")
            folder = os.path.join(path_dados, f"input/{anexo}")
            csv_path = unzip_finbra(zip_path, folder)
            arquivos.append(csv_path)
            csv_to_delete = csv_path

        for arquivo in arquivos:
            if ano <= 2012:
                df_dados = pd.read_excel(arquivo, dtype="string", na_values="")

                if ano >= 1989 and ano <= 1997:
                    df_dados["municipio_original"] = df_dados[
                        "municipio_original"
                    ].str.strip()
                    df_dados = df_dados.merge(
                        df_comp_municipio,
                        how="left",
                        on=["municipio_original", "sigla_uf"],
                    )

                if ano >= 1998 and ano <= 2012:
                    df_dados = build_ibge_id_1998_2012(
                        df_dados, df_comp_municipio
                    )

                df_dados = df_dados.drop_duplicates(
                    subset=["id_municipio"], keep="first"
                )

                if ano >= 1989 and ano <= 1997:
                    id_vars = [
                        "id_municipio",
                        "sigla_uf",
                        "municipio_auxiliar",
                        "municipio_original",
                    ]
                    to_drop = ["municipio_auxiliar", "municipio_original"]
                else:
                    id_vars = ["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]
                    to_drop = ["CD_UF", "CD_MUN"]

                value_vars = df_dados.drop(id_vars, axis=1).columns
                df_dados = pd.melt(
                    df_dados,
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name="conta",
                    value_name="valor",
                )
                df_dados = df_dados.drop(to_drop, axis=1)

            if ano >= 2013:
                df_dados = read_finbra_csv(arquivo)
                df_dados = clean_finbra_municipio(df_dados)
                df_dados = apply_conta_split(df_dados, ano)

            df_dados["ano"] = str(ano)

            if ano <= 2012:
                chaves = ["ano", "conta"]
            else:
                chaves = ["ano", "estagio", "portaria", "conta"]

            df_dados = df_dados.merge(df_comp_receitas, how="left", on=chaves)
            df_dados["conta"] = df_dados["conta"].astype("string")
            df_dados = df_dados.fillna("")
            df_dados["valor"] = pd.to_numeric(
                df_dados["valor"], errors="coerce"
            ).astype("float")

            if ano >= 1990 and ano <= 1993:
                df_dados["valor"] *= 1000

            if ano <= 1992:
                df_dados["valor"] /= (1000**2) * 2.75
            elif ano == 1993:
                df_dados["valor"] /= 1000 * 2.75

            df_dados = df_dados[ORDEM_MUNICIPIO]
            df = pd.concat([df, df_dados], ignore_index=True)

        if csv_to_delete and os.path.exists(csv_to_delete):
            os.remove(csv_to_delete)

        print(f"Particionando municipio_receitas_orcamentarias {ano}")
        partition_and_save(
            df, "municipio_receitas_orcamentarias", ano, path_dados
        )
