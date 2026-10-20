"""
Tasks for br_cgu_pessoal_executivo_federal.

`crawl` é a mesma raspagem leve de sempre (scrape da página de listagem,
sem baixar CSV) — só que agora devolve a competência mais recente já como
`date`, em vez da string formatada que o flow antigo calculava e
descartava (`crawl_urls, _ = crawl(URL)`).
"""

import os
import re
from datetime import date
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task

from pipelines.datasets.br_cgu_pessoal_executivo_federal.constants import (
    COVERAGE,
    ROOT,
    URL,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task
def crawl(url: str) -> tuple[list[str], date]:
    """Lista os CSVs publicados pelo CGU e acha a competência mais recente.

    Args:
        url: página de listagem do CGU.

    Returns:
        Lista de URLs dos CSVs encontrados, e a competência mais recente
        (extraída do padrão AAAAMM no nome dos arquivos) como `date`
        (dia 1 do mês).
    """
    html_page = requests.get(url, timeout=300).content
    links = BeautifulSoup(html_page, "html.parser").find_all(
        "a", {"class": "internal-link"}, href=True
    )
    urls = [a["href"] for a in links if a["href"].endswith("csv")]

    competencias = sorted(re.findall(r"\d{6}", "".join(urls)))
    latest = competencias[-1]
    reference_date = date(int(latest[:4]), int(latest[4:]), 1)

    return urls, reference_date


@task
def clean_save_table(root: str, url_list: list):
    """Standardizes column names and selected variables"""
    path_out = f"{root}/terceirizados.csv"
    os.makedirs(root, exist_ok=True)

    cols = [
        "id_terc",
        "sg_orgao_sup_tabela_ug",
        "cd_ug_gestora",
        "nm_ug_tabela_ug",
        "sg_ug_gestora",
        "nr_contrato",
        "nr_cnpj",
        "nm_razao_social",
        "nr_cpf",
        "nm_terceirizado",
        "nm_categoria_profissional",
        "nm_escolaridade",
        "nr_jornada",
        "nm_unidade_prestacao",
        "vl_mensal_salario",
        "vl_mensal_custo",
        "Num_Mes_Carga",
        "Mes_Carga",
        "Ano_Carga",
        "sg_orgao",
        "nm_orgao",
        "cd_orgao_siafi",
        "cd_orgao_siape",
    ]
    df = pd.DataFrame(columns=cols)
    for url in url_list:
        csv = requests.get(url, timeout=300)
        file_bytes = BytesIO()
        file_bytes.write(csv.content)
        file_bytes.seek(0)
        # handle cases with and without header
        if "id_terc" in csv.text[:20]:
            df_url = pd.read_csv(
                file_bytes,
                sep=";",
                low_memory=False,
                dtype=str,
                encoding="latin-1",
            )
        else:
            df_url = pd.read_csv(
                file_bytes,
                sep=";",
                low_memory=False,
                names=cols,
                header=None,
                dtype=str,
                encoding="latin-1",
            )
        df = pd.concat([df, df_url], ignore_index=True)
        del df_url

    new_cols_names = {
        "id_terc": "id_terceirizado",
        "sg_orgao_sup_tabela_ug": "sigla_orgao_superior_unidade_gestora",
        "cd_ug_gestora": "codigo_unidade_gestora",
        "nm_ug_tabela_ug": "unidade_gestora",
        "sg_ug_gestora": "sigla_unidade_gestora",
        "nr_contrato": "contrato_empresa",
        "nr_cnpj": "cnpj_empresa",
        "nm_razao_social": "razao_social_empresa",
        "nr_cpf": "cpf",
        "nm_terceirizado": "nome",
        "nm_categoria_profissional": "categoria_profissional",
        "nm_escolaridade": "nivel_escolaridade",
        "nr_jornada": "quantidade_horas_trabalhadas_semanais",
        "nm_unidade_prestacao": "unidade_trabalho",
        "vl_mensal_salario": "valor_mensal",
        "vl_mensal_custo": "custo_mensal",
        "Num_Mes_Carga": "mes",
        "Ano_Carga": "ano",
        "sg_orgao": "sigla_orgao_trabalho",
        "nm_orgao": "nome_orgao_trabalho",
        "cd_orgao_siafi": "codigo_siafi_trabalho",
        "cd_orgao_siape": "codigo_siape_trabalho",
    }

    df = df.drop(columns=["Mes_Carga"])
    df = df.rename(columns=new_cols_names)

    selected_cols = [
        "ano",
        "mes",
        "id_terceirizado",
        "sigla_orgao_superior_unidade_gestora",
        "codigo_unidade_gestora",
        "unidade_gestora",
        "sigla_unidade_gestora",
        "contrato_empresa",
        "cnpj_empresa",
        "razao_social_empresa",
        "cpf",
        "nome",
        "categoria_profissional",
        "nivel_escolaridade",
        "quantidade_horas_trabalhadas_semanais",
        "unidade_trabalho",
        "valor_mensal",
        "custo_mensal",
        "sigla_orgao_trabalho",
        "nome_orgao_trabalho",
        "codigo_siafi_trabalho",
        "codigo_siape_trabalho",
    ]
    df = df[selected_cols]

    df.to_csv(path_out, encoding="utf-8", index=False)

    return path_out


def get_latest_update() -> SourceInspection:
    _, reference_date = crawl(URL)
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    urls, _ = crawl(URL)
    filepath = clean_save_table(ROOT, urls)

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=filepath,
        dump_mode="overwrite",
    )
