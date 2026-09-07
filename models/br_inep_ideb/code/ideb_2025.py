import os
from pathlib import Path

import basedosdados as bd
import numpy as np
import pandas as pd
import requests

input = Path("input", "br_inep_ideb")
output = Path("output", "br_inep_ideb")


def main(year: int, skip_download: bool = False) -> None:
    """Build and upload IDEB data for one reference year.
    Args:
        year: IDEB reference year to process.
        skip_download: Skip download of files
    """
    input.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)

    urls = {
        "brasil": f"https://download.inep.gov.br/ideb/resultados/divulgacao_brasil_ideb_{year}.zip",
        "regioes_estados": f"https://download.inep.gov.br/ideb/resultados/divulgacao_regioes_ufs_ideb_{year}.zip",
        "municipio_anos_iniciais": f"https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_municipios_{year}.zip",
        "municipio_anos_finais": f"https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_municipios_{year}.zip",
        "municipio_em": f"https://download.inep.gov.br/ideb/resultados/divulgacao_ensino_medio_municipios_{year}.zip",
        "escola_anos_iniciais": f"https://download.inep.gov.br/ideb/resultados/divulgacao_anos_iniciais_escolas_{year}.zip",
        "escola_anos_finais": f"https://download.inep.gov.br/ideb/resultados/divulgacao_anos_finais_escolas_{year}.zip",
        "escola_em": f"https://download.inep.gov.br/ideb/resultados/divulgacao_ensino_medio_escolas_{year}.zip",
    }

    if not skip_download:
        for table_name, url in urls.items():
            print(url)
            for attempt in range(5):
                try:
                    response = requests.get(
                        url,
                        headers={"User-Agent": "Mozilla/5.0"},
                        verify=False,
                        timeout=120 * 2,
                    )
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == 4:
                        raise
                    print(f"  retry {attempt + 1}/5 ({e})")
            file = input / table_name / url.split("/")[-1]
            file.parent.mkdir(parents=True, exist_ok=True)
            with open(file, "wb") as f:
                # pyrefly: ignore [unbound-name]
                f.write(response.content)

    xlsx_br = os.path.join(
        input,
        "brasil",
        f"divulgacao_brasil_ideb_{year}",
        f"divulgacao_brasil_ideb_{year}.xlsx",
    )

    sheet_names_br = pd.ExcelFile(xlsx_br).sheet_names

    common_renames = {
        "rede": "rede",
        "VL_NOTA_MATEMATICA_2025": "nota_saeb_matematica",
        "VL_NOTA_PORTUGUES_2025": "nota_saeb_lingua_portuguesa",
        "VL_NOTA_MEDIA_2025": "nota_saeb_media_padronizada",
        "VL_INDICADOR_REND_2025": "indicador_rendimento",
        "VL_APROVACAO_2025_SI_4": "taxa_aprovacao",
        "VL_OBSERVADO_2025": "ideb",
    }

    df_brasil = pd.concat(
        [
            pd.read_excel(xlsx_br, sheet_name=sheet_name, skiprows=9)[
                list(common_renames.keys())
            ]
            .rename(columns=common_renames, errors="raise")
            .assign(anos_escolares=sheet_name)
            for sheet_name in sheet_names_br
        ]
    )

    df_brasil = df_brasil.pipe(lambda d: d.loc[d["rede"].notna()]).assign(
        ano=year
    )

    df_brasil["rede"].unique()

    df_brasil["rede"] = (
        df_brasil["rede"]
        .str.lower()
        .replace({"privada (1)": "privada", "pública": "publica"})
    )

    df_brasil["anos_escolares"] = df_brasil["anos_escolares"].replace(
        {
            "Brasil (Anos Iniciais)": "iniciais (1-5)",
            "Brasil (Anos Finais)": "finais (6-9)",
            "Brasil (EM)": "todos (1-4)",
        }
    )

    df_brasil["projecao"] = np.nan

    df_brasil["ensino"] = df_brasil["anos_escolares"].apply(
        lambda v: "medio" if v == "todos (1-4)" else "fundamental"
    )

    df_brasil_upstream = bd.read_sql(
        "select * from `basedosdados.br_inep_ideb.brasil`",
        billing_project_id="basedosdados-dev",
    )

    tb_brasil = bd.Table(dataset_id="br_inep_ideb", table_id="brasil")

    tb_brasil_cols_from_bq = tb_brasil._get_columns_from_bq()

    assert len(tb_brasil_cols_from_bq["partition_columns"]) == 0

    tb_brasil_order_cols: list[str] = [
        i["name"] for i in tb_brasil_cols_from_bq["columns"]
    ]

    df_brasil_updated = pd.concat(
        [
            df_brasil[tb_brasil_order_cols],
            df_brasil_upstream.loc[df_brasil_upstream["year"] != year],
        ]
    )

    output_br = os.path.join(output, "brasil.csv")

    print(df_brasil_updated)
    df_brasil_updated.to_csv(output_br, index=False)

    # Regioes, UFs
    xlsx_regioes_ufs = os.path.join(
        input,
        "regioes_estados",
        f"divulgacao_regioes_ufs_ideb_{year}",
        f"divulgacao_regioes_ufs_ideb_{year}.xlsx",
    )

    sheet_names_regioes_ufs = pd.ExcelFile(xlsx_regioes_ufs).sheet_names

    df_regioes_ufs_latest = pd.concat(
        [
            pd.read_excel(xlsx_regioes_ufs, sheet_name=sheet_name, skiprows=9)
            .rename(
                columns={"VL_INDICADOR_REND_2023.1": "VL_INDICADOR_REND_2025"}
            )[
                [
                    *["Unnamed: 0", "Unnamed: 1"],
                    *[i for i in common_renames if i != "rede"],
                ]
            ]
            .rename(columns={"Unnamed: 0": "uf_regiao", "Unnamed: 1": "rede"})
            .rename(columns=common_renames, errors="raise")
            .assign(anos_escolares=sheet_name)
            for sheet_name in sheet_names_regioes_ufs
        ]
    )

    df_regioes_ufs_latest = df_regioes_ufs_latest.pipe(
        lambda d: d.loc[d["rede"].notna()]
    ).assign(ano=year)

    df_regioes_ufs_latest["rede"].unique()

    df_regioes_ufs_latest["rede"] = (
        df_regioes_ufs_latest["rede"]
        .str.lower()
        .replace(
            {
                "privada (1)": "privada",
                "privada (2)": "privada",
                "total (3)(4)": "total",
                "total (4)": "total",
                "pública": "publica",
                "pública (4)": "publica",
            }
        )
    )

    df_regioes_ufs_latest["anos_escolares"].unique()

    df_regioes_ufs_latest["anos_escolares"] = df_regioes_ufs_latest[
        "anos_escolares"
    ].replace(
        {
            "UF e Regiões (AI)": "iniciais (1-5)",
            "UF e Regiões (AF)": "finais (6-9)",
            "UF e Regiões (EM)": "todos (1-4)",
        }
    )

    df_regioes_ufs_latest["projecao"] = np.nan

    df_regioes_ufs_latest["ensino"] = df_regioes_ufs_latest[
        "anos_escolares"
    ].apply(lambda v: "medio" if v == "todos (1-4)" else "fundamental")

    df_regioes_ufs_latest["uf_regiao"].unique()

    sigla_ufs_replaces = {
        "R. G. do Norte": "Rio Grande do Norte",
        "R. G. do Sul": "Rio Grande do Sul",
        "M. G. do Sul": "Mato Grosso do Sul",
    }

    df_regioes_ufs_latest["uf_regiao"] = df_regioes_ufs_latest[
        "uf_regiao"
    ].replace(sigla_ufs_replaces)

    br_dirs = bd.read_sql(
        "SELECT * from `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados-dev",
    )

    assert isinstance(br_dirs, pd.DataFrame)

    ## Região

    df_regioes_latest = df_regioes_ufs_latest.loc[
        df_regioes_ufs_latest["uf_regiao"].isin(
            br_dirs["regiao"].unique().tolist()
        )
    ]

    assert len(df_regioes_latest["uf_regiao"].unique()) == 5

    df_regioes_latest = df_regioes_latest.rename(
        columns={"uf_regiao": "regiao"}, errors="raise"
    )

    tb_regiao = bd.Table(dataset_id="br_inep_ideb", table_id="regiao")

    tb_regiao_cols_from_bq = tb_regiao._get_columns_from_bq()

    assert len(tb_regiao_cols_from_bq["partition_columns"]) == 0

    tb_regiao_order_cols: list[str] = [
        i["name"] for i in tb_regiao_cols_from_bq["columns"]
    ]

    df_regiao_upstream = bd.read_sql(
        "select * from `basedosdados.br_inep_ideb.regiao`",
        billing_project_id="basedosdados-dev",
    )

    df_regiao_updated = pd.concat(
        [
            df_regioes_latest[tb_regiao_order_cols],
            df_regiao_upstream.loc[df_regiao_upstream["year"] != year],
        ]
    )

    output_regiao = os.path.join(output, "regiao.csv")

    df_regiao_updated.to_csv(output_regiao, index=False)

    print(df_regiao_updated)

    ## UFs
    df_ufs_latest = df_regioes_ufs_latest.loc[
        df_regioes_ufs_latest["uf_regiao"].isin(
            br_dirs["nome"].unique().tolist()
        )
    ]

    assert len(df_ufs_latest["uf_regiao"].unique()) == 27

    df_ufs_latest["uf_regiao"].unique()

    df_ufs_latest = df_ufs_latest.rename(
        columns={"uf_regiao": "sigla_uf"}, errors="raise"
    )

    df_ufs_latest["sigla_uf"] = df_ufs_latest["sigla_uf"].replace(
        {i["nome"]: i["sigla"] for i in br_dirs.to_dict("records")}
    )

    tb_uf = bd.Table(dataset_id="br_inep_ideb", table_id="uf")

    tb_uf_cols_from_bq = tb_uf._get_columns_from_bq()

    assert len(tb_uf_cols_from_bq["partition_columns"]) == 0

    tb_uf_order_cols: list[str] = [
        i["name"] for i in tb_uf_cols_from_bq["columns"]
    ]

    df_uf_upstream = bd.read_sql(
        "select * from `basedosdados.br_inep_ideb.uf`",
        billing_project_id="basedosdados-dev",
    )

    df_uf_updated = pd.concat(
        [
            df_ufs_latest[tb_uf_order_cols],
            df_uf_upstream.loc[df_uf_upstream["year"] != year],
        ]
    )

    output_uf = os.path.join(output, "uf.csv")

    df_uf_updated.to_csv(output_uf, index=False)

    print(df_uf_updated)

    # Municipios
    xlsx_mun_anos_iniciais = os.path.join(
        input,
        "municipio_anos_iniciais",
        f"divulgacao_anos_iniciais_municipios_{year}",
        f"divulgacao_anos_iniciais_municipios_{year}.xlsx",
    )

    xlsx_mun_anos_finais = os.path.join(
        input,
        "municipio_anos_finais",
        f"divulgacao_anos_finais_municipios_{year}",
        f"divulgacao_anos_finais_municipios_{year}.xlsx",
    )

    xlsx_mun_em = os.path.join(
        input,
        "municipio_em",
        f"divulgacao_ensino_medio_municipios_{year}",
        f"divulgacao_ensino_medio_municipios_{year}.xlsx",
    )

    df_municipio_latest = (
        pd.concat(
            [
                pd.read_excel(path, skiprows=9)[
                    [
                        *[i for i in common_renames if i != "rede"],
                        *["SG_UF", "CO_MUNICIPIO", "REDE"],
                    ]
                ]
                .rename(
                    columns={
                        "SG_UF": "sigla_uf",
                        "REDE": "rede",
                        "CO_MUNICIPIO": "id_municipio",
                    },
                    errors="raise",
                )
                .rename(columns=common_renames, errors="raise")
                .assign(anos_escolares=table_name)
                for table_name, path in {
                    "anos_iniciais": xlsx_mun_anos_iniciais,
                    "anos_finais": xlsx_mun_anos_finais,
                    "em": xlsx_mun_em,
                }.items()
            ]
        )
        .pipe(lambda d: d.loc[d["rede"].notna()])
        .assign(ano=year, projecao=np.nan)
    )

    df_municipio_latest.head()

    df_municipio_latest["rede"].unique()

    df_municipio_latest["rede"] = (
        df_municipio_latest["rede"]
        .str.lower()
        .replace(
            {
                "pública": "publica",
            }
        )
    )

    df_municipio_latest["anos_escolares"].unique()

    df_municipio_latest["anos_escolares"] = df_municipio_latest[
        "anos_escolares"
    ].replace(
        {
            "anos_iniciais": "iniciais (1-5)",
            "anos_finais": "finais (6-9)",
            "em": "todos (1-4)",
        }
    )

    df_municipio_latest["ensino"] = df_municipio_latest[
        "anos_escolares"
    ].apply(lambda v: "medio" if v == "todos (1-4)" else "fundamental")

    df_municipio_latest["ensino"].unique()

    assert len(df_municipio_latest["sigla_uf"].unique()) == 27

    df_municipio_latest["id_municipio"] = (
        df_municipio_latest["id_municipio"].astype(int).astype(str)
    )

    assert (
        df_municipio_latest[["id_municipio", "rede", "anos_escolares"]]
        .value_counts(dropna=False)
        .reset_index()["count"]
        .unique()[0]
        == 1
    )

    df_municipio_latest.head()

    tb_municipio = bd.Table(dataset_id="br_inep_ideb", table_id="municipio")

    tb_municipio_cols_from_bq = tb_municipio._get_columns_from_bq(mode="prod")

    assert len(tb_municipio_cols_from_bq["partition_columns"]) == 0

    tb_municipio_order_cols: list[str] = [
        i["name"] for i in tb_municipio_cols_from_bq["columns"]
    ]

    df_municipio_upstream = bd.read_sql(
        "select * from `basedosdados.br_inep_ideb.municipio`",
        billing_project_id="basedosdados-dev",
    )

    df_municipio_updated = pd.concat(
        [
            df_municipio_latest[tb_municipio_order_cols],
            df_municipio_upstream.loc[df_municipio_upstream["year"] != year],
        ]
    )

    output_municipio = os.path.join(output, "municipio.csv")

    print(df_municipio_updated)
    df_municipio_updated.to_csv(output_municipio, index=False)

    # Escolas
    xlsx_escolas_anos_iniciais = os.path.join(
        input,
        "escola_anos_iniciais",
        f"divulgacao_anos_iniciais_escolas_{year}",
        f"divulgacao_anos_iniciais_escolas_{year}.xlsx",
    )

    xlsx_escolas_anos_finais = os.path.join(
        input,
        "escola_anos_finais",
        f"divulgacao_anos_finais_escolas_{year}",
        f"divulgacao_anos_finais_escolas_{year}.xlsx",
    )

    xlsx_escolas_em = os.path.join(
        input,
        "escola_em",
        f"divulgacao_ensino_medio_escolas_{year}",
        f"divulgacao_ensino_medio_escolas_{year}.xlsx",
    )

    df_escolas_latest: pd.DataFrame = (
        pd.concat(
            [
                pd.read_excel(path, skiprows=9)[
                    [
                        *[i for i in common_renames if i != "rede"],
                        *["SG_UF", "CO_MUNICIPIO", "REDE", "ID_ESCOLA"],
                    ]
                ]
                .rename(
                    columns={
                        "SG_UF": "sigla_uf",
                        "REDE": "rede",
                        "CO_MUNICIPIO": "id_municipio",
                        "ID_ESCOLA": "id_escola",
                    },
                    errors="raise",
                )
                .rename(columns=common_renames, errors="raise")
                .assign(anos_escolares=table_name)
                for table_name, path in {
                    "anos_iniciais": xlsx_escolas_anos_iniciais,
                    "anos_finais": xlsx_escolas_anos_finais,
                    "em": xlsx_escolas_em,
                }.items()
            ]
        )
        .pipe(lambda d: d.loc[d["rede"].notna()])
        .assign(ano=year, projecao=np.nan)
    )

    df_escolas_latest.head()

    df_escolas_latest["rede"].unique()

    df_escolas_latest["rede"] = df_escolas_latest["rede"].str.lower()

    df_escolas_latest["anos_escolares"].unique()

    df_escolas_latest["anos_escolares"] = df_escolas_latest[
        "anos_escolares"
    ].replace(
        {
            "anos_iniciais": "iniciais (1-5)",
            "anos_finais": "finais (6-9)",
            "em": "todos (1-4)",
        }
    )

    df_escolas_latest["ensino"] = df_escolas_latest["anos_escolares"].apply(
        lambda v: "medio" if v == "todos (1-4)" else "fundamental"
    )

    df_escolas_latest["ensino"].unique()

    assert len(df_escolas_latest["sigla_uf"].unique()) == 27

    df_escolas_latest["id_municipio"]
    df_escolas_latest["id_escola"]

    df_escolas_latest["id_municipio"] = (
        df_escolas_latest["id_municipio"].astype(int).astype(str)
    )

    df_escolas_latest["id_escola"] = (
        df_escolas_latest["id_escola"].astype(int).astype(str)
    )

    assert (
        df_escolas_latest[["rede", "anos_escolares", "id_escola"]]
        .value_counts(dropna=False)
        .reset_index()["count"]
        .unique()[0]
        == 1
    )

    df_escolas_latest.head()

    tb_escola = bd.Table(dataset_id="br_inep_ideb", table_id="escola")

    tb_escola_cols_from_bq = tb_escola._get_columns_from_bq(mode="prod")

    assert len(tb_escola_cols_from_bq["partition_columns"]) == 0

    tb_escola_order_cols: list[str] = [
        i["name"] for i in tb_escola_cols_from_bq["columns"]
    ]

    df_escola_upstream = bd.read_sql(
        "select * from `basedosdados.br_inep_ideb.escola`",
        billing_project_id="basedosdados-dev",
    )

    df_escolas_updated = pd.concat(
        [
            df_escolas_latest[tb_escola_order_cols],
            df_escola_upstream.loc[df_escola_upstream["year"] != year],
        ]
    )

    output_escola = os.path.join(output, "escola.csv")

    df_escolas_updated.to_csv(output_escola, index=False)

    print(df_escolas_updated)


if __name__ == "__main__":
    main(2025)

    for table_id in ["brasil", "escola", "municipio", "regiao", "uf"]:
        tb = bd.Table(dataset_id="br_inep_ideb", table_id=table_id)
        tb.create(
            output / f"{table_id}.csv",
            if_storage_data_exists="replace",
            if_table_exists="replace",
        )
