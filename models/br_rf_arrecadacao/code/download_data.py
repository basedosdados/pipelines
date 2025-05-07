# -*- coding: utf-8 -*-
import gzip

import requests


def download_data(url, file_path):
    response = requests.get(url)

    with open(file_path, "w") as file:
        if response.headers["Content-Type"] == "application/gzip":
            content = str(gzip.decompress(response.content), "utf-8")
        else:
            content = response.content.decode("latin1")
        file.write(content)


if __name__ == "__main__":
    extractions = [
        {
            "url": "https://www.gov.br/receitafederal/dados/arrecadacao-estado.csv",
            "file_path": "input/arrecadacao-estado.csv",
        },  # Tabela uf
        {
            "url": "https://www.gov.br/receitafederal/dados/arrecadacao-cnae.csv",
            "file_path": "input/arrecadacao-cnae.csv",
        },  # Tabela cnae
        {
            "url": "https://www.gov.br/receitafederal/dados/arrecadacao-natureza.csv",
            "file_path": "input/arrecadacao-natureza.csv",
        },  # Tabela natureza_juridica
        {
            "url": "https://www.gov.br/receitafederal/dados/arrecadacao-ir-ipi.csv",
            "file_path": "input/arrecadacao-ir-ipi.csv",
        },  # Tabela ir_ipi
        {
            "url": "https://www.gov.br/receitafederal/dados/arrecadacao-itr.csv",
            "file_path": "input/arrecadacao-itr.csv",
        },  # Tabela itr
    ]

    for extract in extractions:
        download_data(extract["url"], extract["file_path"])
