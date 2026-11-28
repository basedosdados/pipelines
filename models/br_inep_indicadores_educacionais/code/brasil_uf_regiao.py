import os
import zipfile
from functools import reduce
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

from models.br_inep_indicadores_educacionais.code.constants_brasil_uf_regiao import (
    renames_afd,
    renames_atu,
    renames_dsu,
    renames_had,
    renames_icg,
    renames_ied,
    renames_ird,
    renames_tdi,
    renames_tnr,
    renames_tx,
)

input = Path("input") / "br_inep_indicadores_educacionais" / "brasil_uf_regiao"
output = (
    Path("output") / "br_inep_indicadores_educacionais" / "brasil_uf_regiao"
)


def download_data(ano: int) -> None:
    urls = [
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_brasil_regioes_ufs_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_brasil_regioes_ufs_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_BRASIL_REGIOES_UF.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_BRASIL_REGIOES_UFS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_BRASIL_REGIOES_UFS.zip",
    ]

    for url in urls:
        print(url)
        for attempt in range(5):
            try:
                response = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    verify=False,
                    timeout=120,
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == 4:
                    raise
                print(f"  retry {attempt + 1}/5 ({e})")
        with open(os.path.join(input, url.split("/")[-1]), "wb") as f:
            # pyrefly: ignore [unbound-name]
            f.write(response.content)

    for file in os.listdir(input):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input, file)) as z:
                z.extractall(input)
                os.remove(os.path.join(input, file))


# ! ================================================ Brasil =================================================


def brasil(ano: int, tabela: str) -> None:
    afd = pd.read_excel(
        os.path.join(
            input,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=renames_afd(), errors="raise")
    afd = afd[afd["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    ## Média de alunos por turma (ATU)
    atu = pd.read_excel(
        os.path.join(
            input,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=renames_atu(), errors="raise")
    atu = atu[atu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            input,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=renames_dsu(ano), errors="raise")
    dsu = dsu[dsu["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Média de Horas-aula diária HAD -> 2023
    had = pd.read_excel(
        os.path.join(
            input,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=renames_had(ano), errors="raise")
    had = had[had["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Complexidade de Gestão da Escola (ICG) -> 2023
    icg = pd.read_excel(
        os.path.join(
            input,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=renames_icg(), errors="raise")

    icg = icg[icg["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Esforço Docente (IED) -> 2023
    ied = pd.read_excel(
        os.path.join(
            input,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=renames_ied(), errors="raise")
    ied = ied[ied["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Regularidade do Corpo Docente (IRD) -> 2023

    ird = pd.read_excel(
        os.path.join(
            input,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=renames_ird(), errors="raise")
    ird = ird[ird["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Taxas de Distorção Idade-série (TDI) -> 2023
    tdi = pd.read_excel(
        os.path.join(
            input,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=renames_tdi(), errors="raise")
    tdi = tdi[tdi["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    # Taxa de Não Resposta (tnr) -> 2023
    tnr = pd.read_excel(
        os.path.join(
            input,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=renames_tnr(ano), errors="raise")
    tnr = tnr[tnr["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    tx = pd.read_excel(
        os.path.join(
            input,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=renames_tx(ano), errors="raise")
    tx = tx[tx["UNIDGEO"] == "Brasil"].drop(columns=["UNIDGEO"])

    keys_col_merge = ["ano", "localizacao", "rede"]

    dfs_indicadores = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        dfs_indicadores,
    )

    df = df.loc[df["ano"] == ano]
    df["rede"] = df["rede"].str.lower().replace({"pública": "publica"})
    df["localizacao"] = df["localizacao"].str.lower()
    df = df.replace("--", None)

    bq_cols: list[str] = (
        bd.read_sql(
            "select * from basedosdados-staging.br_inep_indicadores_educacionais_staging.brasil limit 0",
            billing_project_id="basedosdados-dev",
        )
        .columns.drop(["ano"])
        .to_list()
    )
    missing_cols = [i for i in bq_cols if i not in df.columns]

    if len(missing_cols) > 0:
        print(f"Adding missing columns {missing_cols}")
        df[missing_cols] = None

    df = df[bq_cols]
    print(df)
    output_path = os.path.join(output, tabela, f"ano={ano}")
    os.makedirs(output_path, exist_ok=True)
    df.to_csv(os.path.join(output_path, "brasil.csv"), index=False)


# ! ================================================ UF =================================================


def uf(ano: int, tabela: str) -> None:
    # Ler diretório de estados e regiões (para mapear UFs e regiões)
    bd_dir = bd.read_sql(
        "SELECT sigla, nome, regiao FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados-dev",
    )
    estados = bd_dir["nome"].unique()

    afd = pd.read_excel(
        os.path.join(
            input,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=renames_afd(), errors="raise")
    afd = afd.loc[afd["UNIDGEO"].isin(estados)]

    ## Média de alunos por turma (ATU)
    atu = pd.read_excel(
        os.path.join(
            input,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=renames_atu(), errors="raise")
    atu = atu.loc[atu["UNIDGEO"].isin(estados)]

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            input,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=renames_dsu(ano), errors="raise")
    dsu = dsu.loc[dsu["UNIDGEO"].isin(estados)]

    # Média de Horas-aula diária HAD -> 2023
    had = pd.read_excel(
        os.path.join(
            input,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=renames_had(ano), errors="raise")
    had = had.loc[had["UNIDGEO"].isin(estados)]

    # Complexidade de Gestão da Escola (ICG) -> 2023
    icg = pd.read_excel(
        os.path.join(
            input,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=renames_icg(), errors="raise")
    icg = icg.loc[icg["UNIDGEO"].isin(estados)]

    # Esforço Docente (IED) -> 2023
    ied = pd.read_excel(
        os.path.join(
            input,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=renames_ied(), errors="raise")
    ied = ied.loc[ied["UNIDGEO"].isin(estados)]

    # Regularidade do Corpo Docente (IRD) -> 2023
    ird = pd.read_excel(
        os.path.join(
            input,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=renames_ird(), errors="raise")
    ird = ird.loc[ird["UNIDGEO"].isin(estados)]

    # Taxas de Distorção Idade-série (TDI) -> 2023
    tdi = pd.read_excel(
        os.path.join(
            input,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=renames_tdi(), errors="raise")
    tdi = tdi.loc[tdi["UNIDGEO"].isin(estados)]

    # Taxa de Não Resposta (tnr) -> 2023
    tnr = pd.read_excel(
        os.path.join(
            input,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=renames_tnr(ano), errors="raise")
    tnr = tnr.loc[tnr["UNIDGEO"].isin(estados)]

    tx = pd.read_excel(
        os.path.join(
            input,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=renames_tx(ano), errors="raise")
    tx = tx.loc[tx["UNIDGEO"].isin(estados)]

    keys_col_merge = ["ano", "localizacao", "rede", "UNIDGEO"]

    dfs_indicadores = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        dfs_indicadores,
    )

    df = df.rename(columns={"UNIDGEO": "uf"})
    df = df.loc[df["ano"] == ano]
    df["rede"] = df["rede"].str.lower().replace({"pública": "publica"})
    df["localizacao"] = df["localizacao"].str.lower()
    df = df.replace("--", None)
    df = df.merge(
        bd_dir[["nome", "sigla"]],
        left_on="uf",
        right_on="nome",
    ).drop(columns=["uf", "nome"])
    df = df.rename(columns={"sigla": "sigla_uf"})

    bq_cols: list[str] = (
        bd.read_sql(
            "select * from basedosdados-staging.br_inep_indicadores_educacionais_staging.uf limit 0",
            billing_project_id="basedosdados-dev",
        )
        .columns.drop(["ano"])
        .to_list()
    )
    missing_cols = [i for i in bq_cols if i not in df.columns]

    if len(missing_cols) > 0:
        print(f"Adding missing columns {missing_cols}")
        df[missing_cols] = None

    df = df[bq_cols]
    print(df)
    for uf in df["sigla_uf"].unique():
        output_path = os.path.join(
            output, tabela, f"ano={ano}", f"sigla_uf={uf}"
        )
        os.makedirs(output_path, exist_ok=True)
        df_uf = df[(df["sigla_uf"] == uf)].drop(columns=["sigla_uf"])
        df_uf.to_csv(os.path.join(output_path, "uf.csv"), index=False)


# ! ================================================ Região =================================================


def regiao(ano: int, tabela: str) -> None:
    # Ler diretório de estados e regiões (para mapear UFs e regiões)
    bd_dir = bd.read_sql(
        "SELECT sigla, nome, regiao FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados-dev",
    )
    regioes = bd_dir["regiao"].unique()

    afd = pd.read_excel(
        os.path.join(
            input,
            f"AFD_{ano}_BRASIL_REGIOES_UF",
            f"AFD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    afd = afd.rename(columns=renames_afd(), errors="raise")
    afd = afd.loc[afd["UNIDGEO"].isin(regioes)]

    ## Média de alunos por turma (ATU)
    atu = pd.read_excel(
        os.path.join(
            input,
            f"ATU_{ano}_BRASIL_REGIOES_UFS",
            f"ATU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=renames_atu(), errors="raise")
    atu = atu.loc[atu["UNIDGEO"].isin(regioes)]

    # Percentual de Docentes com Curso Superior
    dsu = pd.read_excel(
        os.path.join(
            input,
            f"DSU_{ano}_BRASIL_REGIOES_UFS",
            f"DSU_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=renames_dsu(ano), errors="raise")
    dsu = dsu.loc[dsu["UNIDGEO"].isin(regioes)]

    # Média de Horas-aula diária HAD -> 2023
    had = pd.read_excel(
        os.path.join(
            input,
            f"HAD_{ano}_BRASIL_REGIOES_UFS",
            f"HAD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    had = had.rename(columns=renames_had(ano), errors="raise")
    had = had.loc[had["UNIDGEO"].isin(regioes)]

    # Complexidade de Gestão da Escola (ICG) -> 2023
    icg = pd.read_excel(
        os.path.join(
            input,
            f"ICG_{ano}_BRASIL_REGIOES_UFS",
            f"ICG_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=renames_icg(), errors="raise")
    icg = icg.loc[icg["UNIDGEO"].isin(regioes)]

    # Esforço Docente (IED) -> 2023
    ied = pd.read_excel(
        os.path.join(
            input,
            f"IED_{ano}_BRASIL_REGIOES_UFS",
            f"IED_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=renames_ied(), errors="raise")
    ied = ied.loc[ied["UNIDGEO"].isin(regioes)]

    # Regularidade do Corpo Docente (IRD) -> 2023
    ird = pd.read_excel(
        os.path.join(
            input,
            f"IRD_{ano}_BRASIL_REGIOES_UFS",
            f"IRD_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=renames_ird(), errors="raise")
    ird = ird.loc[ird["UNIDGEO"].isin(regioes)]

    # Taxas de Distorção Idade-série (TDI) -> 2023
    tdi = pd.read_excel(
        os.path.join(
            input,
            f"TDI_{ano}_BRASIL_REGIOES_UFS",
            f"TDI_BRASIL_REGIOES_UFS_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=renames_tdi(), errors="raise")
    tdi = tdi.loc[tdi["UNIDGEO"].isin(regioes)]

    # Taxa de Não Resposta (tnr) -> 2023
    tnr = pd.read_excel(
        os.path.join(
            input,
            f"tnr_brasil_regioes_ufs_{ano}",
            f"tnr_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=renames_tnr(ano), errors="raise")
    tnr = tnr.loc[tnr["UNIDGEO"].isin(regioes)]

    tx = pd.read_excel(
        os.path.join(
            input,
            f"tx_rend_brasil_regioes_ufs_{ano}",
            f"tx_rend_brasil_regioes_ufs_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=renames_tx(ano), errors="raise")
    tx = tx.loc[tx["UNIDGEO"].isin(regioes)]

    keys_col_merge = ["ano", "localizacao", "rede", "UNIDGEO"]

    dfs_indicadores = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        dfs_indicadores,
    )

    df = df.loc[df["ano"] == ano]
    df["rede"] = df["rede"].str.lower().replace({"pública": "publica"})
    df["localizacao"] = df["localizacao"].str.lower()
    df = df.replace("--", None)
    df = df.rename(columns={"UNIDGEO": "regiao"})

    bq_cols: list[str] = (
        bd.read_sql(
            "select * from basedosdados-staging.br_inep_indicadores_educacionais_staging.regiao limit 0",
            billing_project_id="basedosdados-dev",
        )
        .columns.drop(["ano"])
        .to_list()
    )
    missing_cols = [i for i in bq_cols if i not in df.columns]

    if len(missing_cols) > 0:
        print(f"Adding missing columns {missing_cols}")
        df[missing_cols] = None

    df = df[bq_cols]
    print(df)

    output_path = os.path.join(output, tabela, f"ano={ano}")
    os.makedirs(output_path, exist_ok=True)
    df.to_csv(os.path.join(output_path, "uf.csv"), index=False)


if __name__ == "__main__":
    input.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)

    # download_data(ano=2025)
    brasil(ano=2025, tabela="brasil")
    uf(ano=2025, tabela="uf")
    regiao(ano=2025, tabela="regiao")

    for dir in output.iterdir():
        bd.Table(
            dataset_id="br_inep_indicadores_educacionais", table_id=dir.stem
        ).create(
            path=dir,
            if_storage_data_exists="replace",
            if_table_exists="replace",
        )
