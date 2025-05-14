# -*- coding: utf-8 -*-
# Importing
import datetime
import json
import os

import pandas as pd
import requests

output = "transferencia/output/"
# Download
todays_year = datetime.date.today().year
year_range = list(range(2015, todays_year + 1))
year_range = [str(x) for x in year_range]
first_time_flag = True
# API retorna no máximo 250 observações, utilizando a segmentação abaixo a maior categoria (ano, mês,
# natureza jurídica) tem 194 observações. É uma margem segura? Se não repensar como fazer
api_response = requests.get(
    "https://apidatalake.tesouro.gov.br/ords/custos/tt/transferencias?ano=2020&mes=10&natureza_juridica=3"
).text

transf_dict = json.loads(api_response)
transferencia_esp = pd.json_normalize(transf_dict["items"])
for ano in [str(x) for x in list(range(2015, todays_year + 1))]:
    for mes in [str(x) for x in list(range(1, 13))]:
        for natur in [str(x) for x in list(range(1, 7))]:
            api_response = requests.get(
                "https://apidatalake.tesouro.gov.br/ords/custos/tt/transferencias?ano="
                + ano
                + "&mes="
                + mes
                + "&natureza_juridica="
                + natur
            ).text

            transf_dict = json.loads(api_response)
            transferencia_esp = pd.json_normalize(transf_dict["items"])

            # Renomear variáveis com nomes inadequados
            transferencia_esp = transferencia_esp.rename(
                columns={
                    "an_lanc": "ano_lancamento",
                    "me_lanc": "mes_lancamento",
                    "ds_organizacao_n0": "nome_unidade_organizacional_nivel_0",
                    "ds_organizacao_n1": "nome_unidade_organizacional_nivel_1",
                    "ds_organizacao_n2": "nome_unidade_organizacional_nivel_2",
                    "ds_organizacao_n3": "nome_unidade_organizacional_nivel_3",
                    "co_organizacao_n0": "id_unidade_organizacional_nivel_0",
                    "co_organizacao_n1": "id_unidade_organizacional_nivel_1",
                    "co_organizacao_n2": "id_unidade_organizacional_nivel_2",
                    "co_organizacao_n3": "id_unidade_organizacional_nivel_3",
                    "co_natureza_juridica": "id_natureza_juridica",
                    "ds_natureza_juridica": "nome_natureza_juridica",
                    "co_esfera_orcamentaria": "id_esfera_orcamentaria",
                    "ds_esfera_orcamentaria": "nome_esfera_orcamentaria",
                    "co_modalidade_aplicacao": "id_modalidade_aplicacao",
                    "ds_modalidade_aplicacao": "nome_modalidade_aplicacao",
                    "co_resultado_eof": "id_resultado_primario",
                    "ds_resultado_eof": "nome_resultado_primario",
                    "va_custo_transferencias": "valor_custo_transferencia",
                }
            )
            if len(transferencia_esp) == 250:
                raise Exception(
                    "Single request reached 250 observations, the maximum allowed by the Tesouro API. Probably missing data, code fix needed"
                )

            if first_time_flag:
                transferencia = transferencia_esp
                first_time_flag = False
            else:
                transferencia = pd.concat(
                    [transferencia, transferencia_esp], axis=0
                )

# Remover 0s iniciais

id_idx = [col.startswith("id") for col in list(transferencia.columns.values)]

transferencia.loc[:, id_idx] = (
    transferencia.loc[:, id_idx].astype(int).astype(str)
)

transferencia = transferencia[
    [
        "ano_lancamento",
        "mes_lancamento",
        "id_unidade_organizacional_nivel_0",
        "nome_unidade_organizacional_nivel_0",
        "id_unidade_organizacional_nivel_1",
        "nome_unidade_organizacional_nivel_1",
        "id_unidade_organizacional_nivel_2",
        "nome_unidade_organizacional_nivel_2",
        "id_unidade_organizacional_nivel_3",
        "nome_unidade_organizacional_nivel_3",
        "id_natureza_juridica",
        "nome_natureza_juridica",
        "id_esfera_orcamentaria",
        "nome_esfera_orcamentaria",
        "id_resultado_primario",
        "nome_resultado_primario",
        "valor_custo_transferencia",
    ]
]

# Cria as partições e coloca os arquivos respectivos dentro delas

for ano in [*range(2015, todays_year + 1)]:
    for mes in [*range(1, 13)]:
        particao = output + f"transferencia/ano={ano}/mes={mes}"
        if not os.path.exists(particao):
            os.makedirs(particao)
for ano in [*range(2015, todays_year + 1)]:
    for mes in [*range(1, 13)]:
        df_particao = transferencia[
            transferencia["ano_lancamento"] == ano
        ].copy()  # O .copy não é necessário é apenas uma boa prática
        df_particao = df_particao[df_particao["mes_lancamento"] == mes]
        df_particao.drop(
            ["ano_lancamento", "mes_lancamento"], axis=1, inplace=True
        )  # É preciso excluir as colunas utilizadas para partição
        particao = (
            output + f"transferencia/ano={ano}/mes={mes}/transferencia.csv"
        )
        df_particao.to_csv(particao, index=False, encoding="utf-8", na_rep="")
