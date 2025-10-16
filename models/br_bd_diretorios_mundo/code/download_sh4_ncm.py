from io import BytesIO
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

CURRENT_DIR = Path(__file__).parent
PARENT_DIR = CURRENT_DIR.parent
TMP_DIR = PARENT_DIR / "tmp"
TMP_DIR.mkdir(exist_ok=True, parents=True)
TABLE_IDS = {
    "ncm": "nomenclatura_comum_mercosul",
    "sh4": "sistema_harmonizado",
}

URLS = {
    "ncm": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
    "sh4": "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_SH.csv",
}


def download_data():
    for key in URLS:
        print(f"Baixando tabela {key}")

        response = requests.get(URLS[key], verify=False)
        if response.status_code == 200:
            data = BytesIO(response.content)
            df = pd.read_csv(data, encoding="latin-1", sep=";", dtype=str)
            df.to_csv(
                str(TMP_DIR / f"{TABLE_IDS[key]}.csv"),
                index=False,
                encoding="utf-8",
                sep=",",
            )
        else:
            print(f"Erro! O status da requisição foi {response.status_code}")


def upload_to_bigquery(
    table_id: str, path_to_data: str, dataset_id: str = f"{PARENT_DIR.stem}"
):
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    tb.create(
        path=path_to_data,
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="csv",
    )


if __name__ == "__main__":
    # download_data()
    for key in TABLE_IDS:
        upload_to_bigquery(
            table_id=TABLE_IDS[key],
            path_to_data=str(TMP_DIR / f"{TABLE_IDS[key]}.csv"),
        )
