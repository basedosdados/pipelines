
""" 
Utils for br_me_comex_stat
"""

import os
import requests

from tqdm import tqdm

def create_paths(table, path, ufs, current_year):
    """ Create and partition folders"""

    path_temps = [path, path + "input/", path + "output/"]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True) ### TODO: verificar se já existe 

    for ano in [*range(1997, current_year)]:

        for mes in [*range(1, 13)]:

            if 'municipio' in table:

                for uf in ufs:

                    os.makedirs(
                        path + f"output/{table}/ano={ano}/mes={mes}/sigla_uf={uf}", exist_ok=True
                    )

            else:
                    os.makedirs(
                        path + f"output/{table}/ano={ano}/mes={mes}/", exist_ok=True
                        
                    )

def download_data(path, group, item):

    """ 
    Crawler for br_me_comex_stat 
    """

    # for item in groups.keys():
    #     for group in tqdm(groups[item]):
    print(f"Baixando {item} do {group}")

    zip_output_path = path + "input/" + f"{group}.zip"
    url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{item}/{group}.zip"
    r = requests.get(url, verify=False, timeout=99999999)
    open(zip_output_path, "wb").write(r.content)

    import zipfile
    with zipfile.ZipFile(zip_output_path,"r") as zip_ref:
        zip_ref.extractall(path + "input/")
    
    os.remove(zip_output_path)

    print('Download concluído!')
