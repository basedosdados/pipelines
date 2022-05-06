# -*- coding: utf-8 -*-
"""
Tasks for br_cgu_terceirizados
"""
import os
import re
from io import BytesIO
from typing import Tuple
import requests

import pandas as pd
from bs4 import BeautifulSoup
from prefect import task


# pylint: disable=C0103
@task(nout=2)
def crawl(url: str) -> Tuple[str, str]:
    """
    Get all table urls from CGU website and extract temporal covarage
    """
    html_page = requests.get(url).content
    urls = BeautifulSoup(html_page, "html.parser").find_all(
        "a", {"class": "internal-link"}, href=True
    )
    urls = [url["href"] for url in urls if url["href"].endswith("csv")]
    regex_pattern = re.compile(r"\d{6}")
    temporal_coverage = regex_pattern.findall("".join(urls))
    temporal_coverage.sort()
    start_date = temporal_coverage[0][:4] + "-" + temporal_coverage[0][4:]
    end_date = temporal_coverage[-1][:4] + "-" + temporal_coverage[-1][4:]
    temporal_coverage = start_date + "(4)" + end_date

    return urls, temporal_coverage


# pylint: disable=C0103
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
        csv = requests.get(url)
        file_bytes = BytesIO()
        file_bytes.write(csv.content)
        file_bytes.seek(0)
        # handle cases with and without header
        if "id_terc" in csv.text[:20]:
            df_url = pd.read_csv(file_bytes, sep=";", low_memory=False, dtype=str)
        else:
            df_url = pd.read_csv(
                file_bytes,
                sep=";",
                low_memory=False,
                names=cols,
                header=None,
                dtype=str,
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

    df.drop(columns=["Mes_Carga"], inplace=True)
    df.rename(columns=new_cols_names, inplace=True)

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
