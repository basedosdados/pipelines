# -*- coding: utf-8 -*-
""" Utils for the Brazilian Comex Stat pipeline. """
# pylint: disable=invalid-name
import os
import requests

from tqdm import tqdm


def create_paths(tables, path, ufs):
    """
    Create and partition folders
    """
    path_temps = [path, path + "input/", path + "output/"]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)

    for table in tables:
        for ano in [*range(1997, 2024)]:

            for mes in [*range(1, 13)]:

                if "municipio" in table:

                    for uf in ufs:

                        os.makedirs(
                            path + f"output/{table}/ano={ano}/mes={mes}/sigla_uf={uf}",
                            exist_ok=True,
                        )

                else:
                    os.makedirs(
                        path + f"output/{table}/ano={ano}/mes={mes}/", exist_ok=True
                    )


def download_data(path):
    """
    Crawler for br_me_comex_stat
    """
    groups = {
        "ncm": ["EXP_COMPLETA", "IMP_COMPLETA"],
        "mun": ["EXP_COMPLETA_MUN", "IMP_COMPLETA_MUN"],
    }

    for item, value in groups.items():
        for group in tqdm(value):
            print(f"Baixando {item} do {group}")
            url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{item}/{group}.zip"
            r = requests.get(url, verify=False, timeout=99999999)
            with open(path + f"input/{group}.zip", "wb") as f:
                f.write(r.content)
