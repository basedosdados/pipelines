import os
import zipfile
from functools import reduce

import basedosdados as bd
import pandas as pd
import requests

from models.br_inep_indicadores_educacionais.code.constants_municipio import (
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

folder = os.path.join(
    os.getcwd(),
    "models",
    "br_inep_indicadores_educacionais",
    "code",
    "tmp",
)

input = os.path.join(folder, "input")
output = os.path.join(folder, "municipios", "output")


def municipios(ano: int) -> None:
    os.makedirs(input, exist_ok=True)
    os.makedirs(output, exist_ok=True)

    urls_municipios = [
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/AFD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ATU_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/DSU_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/HAD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/ICG_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IED_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/IRD_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/TDI_{ano}_MUNICIPIOS.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tnr_municipios_{ano}.zip",
        f"https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais/{ano}/tx_rend_municipios_{ano}.zip",
    ]

    skip_download = True

    if not skip_download:
        for url in urls_municipios:
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

    bq_cols: list[str] = (
        bd.read_sql(
            "select * from basedosdados-dev.br_inep_indicadores_educacionais.municipio limit 0",
            billing_project_id="basedosdados-dev",
        )
        .columns.drop(["ano"])
        .to_list()
    )
    afd = pd.read_excel(
        os.path.join(
            input, f"AFD_{ano}_MUNICIPIOS", f"AFD_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=10,
    )

    afd_cols: dict[str, str] = renames_afd()
    afd = afd.rename(columns=afd_cols, errors="raise")

    atu = pd.read_excel(
        os.path.join(
            input, f"ATU_{ano}_MUNICIPIOS", f"ATU_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    )

    atu = atu.rename(columns=renames_atu(), errors="raise")

    dsu = pd.read_excel(
        os.path.join(
            input, f"DSU_{ano}_MUNICIPIOS", f"DSU_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=9,
    )

    dsu = dsu.rename(columns=renames_dsu(ano), errors="raise")

    had = pd.read_excel(
        os.path.join(
            input, f"HAD_{ano}_MUNICIPIOS", f"HAD_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    )

    had = had.rename(
        columns=renames_had(ano),
        errors="raise",
    )

    icg = pd.read_excel(
        os.path.join(
            input, f"ICG_{ano}_MUNICIPIOS", f"ICG_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    )

    icg = icg.rename(columns=renames_icg(), errors="raise")

    ied = pd.read_excel(
        os.path.join(
            input, f"IED_{ano}_MUNICIPIOS", f"IED_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=10,
    )

    ied = ied.rename(columns=renames_ied(), errors="raise")

    ird = pd.read_excel(
        os.path.join(
            input, f"IRD_{ano}_MUNICIPIOS", f"IRD_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=9,
    )

    ird = ird.rename(columns=renames_ird(), errors="raise")

    tdi = pd.read_excel(
        os.path.join(
            input, f"TDI_{ano}_MUNICIPIOS", f"TDI_MUNICIPIOS_{ano}.xlsx"
        ),
        skiprows=8,
    )

    tdi = tdi.rename(columns=renames_tdi(), errors="raise")

    tnr = pd.read_excel(
        os.path.join(
            input, f"tnr_municipios_{ano}", f"tnr_municipios_{ano}.xlsx"
        ),
        skiprows=8,
    )

    tnr = tnr.rename(columns=renames_tnr(ano), errors="raise")

    tx = pd.read_excel(
        os.path.join(
            input,
            f"tx_rend_municipios_{ano}",
            f"tx_rend_municipios_{ano}.xlsx",
        ),
        skiprows=8,
    )

    tx = tx.rename(columns=renames_tx(ano), errors="raise")

    keys_col_merge = ["ano", "id_municipio", "localizacao", "rede"]

    dfs_indicadores = [afd, atu, dsu, had, icg, ied, ird, tdi, tx, tnr]
    for df in dfs_indicadores:
        df["id_municipio"] = df["id_municipio"].astype("Int64").astype("str")

    df = reduce(
        lambda left, right: pd.merge(  # noqa: PD015
            left, right, on=keys_col_merge, how="outer"
        ),
        dfs_indicadores,
    )
    df = df.apply(lambda x: x.replace("--", None))
    df = df.loc[df["ano"] == ano]
    df["rede"] = df["rede"].str.lower().replace({"pública": "publica"})
    df["localizacao"] = df["localizacao"].str.lower()

    missing_cols = [i for i in bq_cols if i not in df.columns]

    if len(missing_cols) > 0:
        print(f"Adding missing columns {missing_cols}")
        df[missing_cols] = None

    df = df.drop(columns="ano")[bq_cols]

    print(df)

    escola_output_path = os.path.join(output, f"ano={ano}")
    os.makedirs(escola_output_path, exist_ok=True)
    df.to_csv(os.path.join(escola_output_path, "municipio.csv"), index=False)


if __name__ == "__main__":
    # municipios(2025)
    tb = bd.Table(
        dataset_id="br_inep_indicadores_educacionais", table_id="municipio"
    )
    tb.create(
        output, if_storage_data_exists="replace", if_table_exists="replace"
    )
