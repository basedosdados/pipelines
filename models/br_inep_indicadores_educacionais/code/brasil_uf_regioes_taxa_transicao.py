# -*- coding: utf-8 -*-
import os
import zipfile

import basedosdados as bd
import pandas as pd

URLS = [
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_brasil_regioes_ufs_2019_2020.zip",
    "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/taxa_transicao/tx_transicao_brasil_regioes_ufs_2020_2021.zip",
]


INPUT = os.path.join(os.getcwd(), "tmp")

if not os.path.exists(INPUT):
    os.mkdir(INPUT)

INPUT_TX_TRAN = os.path.join(INPUT, "taxa_transicao_br_regioes_uf")

OUTPUT = os.path.join(os.getcwd(), "output")

if not os.path.exists(OUTPUT):
    os.mkdir(OUTPUT)

os.mkdir(INPUT_TX_TRAN)

for url in URLS:
    os.system(f"cd {INPUT_TX_TRAN}; curl -O -k {url}")


for file in os.listdir(INPUT_TX_TRAN):
    with zipfile.ZipFile(os.path.join(INPUT_TX_TRAN, file)) as z:
        z.extractall(INPUT_TX_TRAN)

tx_trans_2019_2020 = pd.read_excel(
    os.path.join(
        INPUT_TX_TRAN,
        "TX_TRANSICAO_BRASIL_REGIOES_UFS_2019_2020",
        "TX_TRANSICAO_BRASIL_REGIOES_UFS_2019_2020.xlsx",
    ),
    skiprows=8,
)

tx_trans_2020_2021 = pd.read_excel(
    os.path.join(
        INPUT_TX_TRAN,
        "TX_TRANSICAO_BRASIL_REGIOES_UFS_2020_2021",
        "TX_TRANSICAO_BRASIL_REGIOES_UFS_2020_2021.xlsx",
    ),
    skiprows=8,
)

tx_trans_updated = pd.concat([tx_trans_2019_2020, tx_trans_2020_2021])

bq_uf_tx_trans = bd.read_sql(
    "SELECT * FROM `basedosdados-dev.br_inep_indicadores_educacionais.uf_taxa_transicao`",
    billing_project_id="basedosdados-dev",
)

bq_brasil_tx_trans = bd.read_sql(
    "SELECT * FROM `basedosdados-dev.br_inep_indicadores_educacionais.brasil_taxa_transicao`",
    billing_project_id="basedosdados-dev",
)

bq_regiao_tx_trans = bd.read_sql(
    "SELECT * FROM `basedosdados-dev.br_inep_indicadores_educacionais.regiao_taxa_transicao`",
    billing_project_id="basedosdados-dev",
)

renames = {
    "NO_LOCALIZACAO": "localizacao",
    "NO_DEPENDENCIA": "rede",
    "1_CAT1_CATFUN": "taxa_promocao_ef",
    "1_CAT1_CATFUN_AI": "taxa_promocao_ef_anos_iniciais",
    "1_CAT1_CATFUN_AF": "taxa_promocao_ef_anos_finais",
    "1_CAT1_CATFUN_01": "taxa_promocao_ef_1_ano",
    "1_CAT1_CATFUN_02": "taxa_promocao_ef_2_ano",
    "1_CAT1_CATFUN_03": "taxa_promocao_ef_3_ano",
    "1_CAT1_CATFUN_04": "taxa_promocao_ef_4_ano",
    "1_CAT1_CATFUN_05": "taxa_promocao_ef_5_ano",
    "1_CAT1_CATFUN_06": "taxa_promocao_ef_6_ano",
    "1_CAT1_CATFUN_07": "taxa_promocao_ef_7_ano",
    "1_CAT1_CATFUN_08": "taxa_promocao_ef_8_ano",
    "1_CAT1_CATFUN_09": "taxa_promocao_ef_9_ano",
    "1_CAT1_CATMED": "taxa_promocao_em",
    "1_CAT1_CATMED_01": "taxa_promocao_em_1_ano",
    "1_CAT1_CATMED_02": "taxa_promocao_em_2_ano",
    "1_CAT1_CATMED_03": "taxa_promocao_em_3_ano",
    "1_CAT2_CATFUN": "taxa_repetencia_ef",
    "1_CAT2_CATFUN_AI": "taxa_repetencia_ef_anos_iniciais",
    "1_CAT2_CATFUN_AF": "taxa_repetencia_ef_anos_finais",
    "1_CAT2_CATFUN_01": "taxa_repetencia_ef_1_ano",
    "1_CAT2_CATFUN_02": "taxa_repetencia_ef_2_ano",
    "1_CAT2_CATFUN_03": "taxa_repetencia_ef_3_ano",
    "1_CAT2_CATFUN_04": "taxa_repetencia_ef_4_ano",
    "1_CAT2_CATFUN_05": "taxa_repetencia_ef_5_ano",
    "1_CAT2_CATFUN_06": "taxa_repetencia_ef_6_ano",
    "1_CAT2_CATFUN_07": "taxa_repetencia_ef_7_ano",
    "1_CAT2_CATFUN_08": "taxa_repetencia_ef_8_ano",
    "1_CAT2_CATFUN_09": "taxa_repetencia_ef_9_ano",
    "1_CAT2_CATMED": "taxa_repetencia_em",
    "1_CAT2_CATMED_01": "taxa_repetencia_em_1_ano",
    "1_CAT2_CATMED_02": "taxa_repetencia_em_2_ano",
    "1_CAT2_CATMED_03": "taxa_repetencia_em_3_ano",
    "1_CAT3_CATFUN": "taxa_evasao_ef",
    "1_CAT3_CATFUN_AI": "taxa_evasao_ef_anos_iniciais",
    "1_CAT3_CATFUN_AF": "taxa_evasao_ef_anos_finais",
    "1_CAT3_CATFUN_01": "taxa_evasao_ef_1_ano",
    "1_CAT3_CATFUN_02": "taxa_evasao_ef_2_ano",
    "1_CAT3_CATFUN_03": "taxa_evasao_ef_3_ano",
    "1_CAT3_CATFUN_04": "taxa_evasao_ef_4_ano",
    "1_CAT3_CATFUN_05": "taxa_evasao_ef_5_ano",
    "1_CAT3_CATFUN_06": "taxa_evasao_ef_6_ano",
    "1_CAT3_CATFUN_07": "taxa_evasao_ef_7_ano",
    "1_CAT3_CATFUN_08": "taxa_evasao_ef_8_ano",
    "1_CAT3_CATFUN_09": "taxa_evasao_ef_9_ano",
    "1_CAT3_CATMED": "taxa_evasao_em",
    "1_CAT3_CATMED_01": "taxa_evasao_em_1_ano",
    "1_CAT3_CATMED_02": "taxa_evasao_em_2_ano",
    "1_CAT3_CATMED_03": "taxa_evasao_em_3_ano",
    "1_CAT4_CATFUN": "taxa_migracao_eja_ef",
    "1_CAT4_CATFUN_AI": "taxa_migracao_eja_ef_anos_iniciais",
    "1_CAT4_CATFUN_AF": "taxa_migracao_eja_ef_anos_finais",
    "1_CAT4_CATFUN_01": "taxa_migracao_eja_ef_1_ano",
    "1_CAT4_CATFUN_02": "taxa_migracao_eja_ef_2_ano",
    "1_CAT4_CATFUN_03": "taxa_migracao_eja_ef_3_ano",
    "1_CAT4_CATFUN_04": "taxa_migracao_eja_ef_4_ano",
    "1_CAT4_CATFUN_05": "taxa_migracao_eja_ef_5_ano",
    "1_CAT4_CATFUN_06": "taxa_migracao_eja_ef_6_ano",
    "1_CAT4_CATFUN_07": "taxa_migracao_eja_ef_7_ano",
    "1_CAT4_CATFUN_08": "taxa_migracao_eja_ef_8_ano",
    "1_CAT4_CATFUN_09": "taxa_migracao_eja_ef_9_ano",
    "1_CAT4_CATMED": "taxa_migracao_eja_em",
    "1_CAT4_CATMED_01": "taxa_migracao_eja_em_1_ano",
    "1_CAT4_CATMED_02": "taxa_migracao_eja_em_2_ano",
    "1_CAT4_CATMED_03": "taxa_migracao_eja_em_3_ano",
}

tx_trans_updated = tx_trans_updated.rename(columns=renames, errors="raise")

tx_trans_updated = tx_trans_updated.loc[
    tx_trans_updated["NU_ANO_CENSO"].isin(["2019/2020", "2020/2021"]),
]

tx_trans_updated["NU_ANO_CENSO"] = tx_trans_updated["NU_ANO_CENSO"].str.split(
    "/"
)

# tx_trans_updated["ano_de"] = tx_trans_updated["NU_ANO_CENSO"].apply(lambda l: int(l[0]))
tx_trans_updated["ano"] = tx_trans_updated["NU_ANO_CENSO"].apply(
    lambda l: int(l[1])
)

tx_trans_updated = tx_trans_updated.drop(columns=["NU_ANO_CENSO"])

tx_trans_updated = tx_trans_updated.replace("--", None)

bd_dir = bd.read_sql(
    "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.uf`",
    billing_project_id="basedosdados-dev",
)

regioes = bd_dir["regiao"].unique()  # type: ignore
estados = bd_dir["nome"].unique()  # type: ignore

brasil_tx_trans_updated = tx_trans_updated.loc[
    tx_trans_updated["CODIGO"] == "Brasil",
].drop(columns=["CODIGO"])[bq_brasil_tx_trans.columns]  # type: ignore

assert brasil_tx_trans_updated.shape[1] == bq_brasil_tx_trans.shape[1]  # type: ignore

brasil_output_path = os.path.join(OUTPUT, "brasil_taxa_transicao")
os.makedirs(brasil_output_path)

pd.concat([bq_brasil_tx_trans, brasil_tx_trans_updated]).to_csv(  # type: ignore
    os.path.join(brasil_output_path, "tx_transicao_brasil.csv"), index=False
)

bq_uf_tx_trans = bq_uf_tx_trans.drop(columns=["ano_de"]).rename(  # type: ignore
    columns={"ano_para": "ano"}  # type: ignore
)

uf_tx_trans_updated = (
    tx_trans_updated.loc[tx_trans_updated["CODIGO"].isin(estados),]
    .merge(bd_dir[["nome", "sigla"]], left_on="CODIGO", right_on="nome")  # type: ignore
    .drop(columns=["CODIGO"])
    .rename(columns={"sigla": "sigla_uf"})[bq_uf_tx_trans.columns]  # type: ignore
)

assert uf_tx_trans_updated.shape[1] == bq_uf_tx_trans.shape[1]  # type: ignore

uf_output_path = os.path.join(OUTPUT, "uf_taxa_transicao")

for sigla_uf, df in pd.concat([bq_uf_tx_trans, uf_tx_trans_updated]).groupby(  # type: ignore
    "sigla_uf"
):
    path = os.path.join(uf_output_path, f"sigla_uf={sigla_uf}")
    os.makedirs(path, exist_ok=True)
    df.drop(columns=["sigla_uf"]).to_csv(
        os.path.join(path, "taxa_transicao_uf.csv"), index=False
    )


bq_regiao_tx_trans = bq_regiao_tx_trans.drop(columns=["ano_de"]).rename(  # type: ignore
    columns={"ano_para": "ano"}  # type: ignore
)

regiao_tx_trans_updated = (
    tx_trans_updated.loc[tx_trans_updated["CODIGO"].isin(regioes),].rename(
        columns={"CODIGO": "regiao"}
    )[bq_regiao_tx_trans.columns]  # type: ignore
)

assert regiao_tx_trans_updated.shape[1] == bq_regiao_tx_trans.shape[1]  # type: ignore

regiao_output_path = os.path.join(OUTPUT, "regiao_taxa_transicao")
os.makedirs(regiao_output_path)

pd.concat([bq_regiao_tx_trans, regiao_tx_trans_updated]).to_csv(  # type: ignore
    os.path.join(regiao_output_path, "tx_transicao_regiao.csv"), index=False
)
