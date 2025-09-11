from pathlib import Path

from utils import download_data, upload_data_to_big_query

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

if __name__ == "__main__":
    for key in URLS:
        download_data(URLS[key], TMP_DIR / f"{TABLE_IDS[key]}.csv")

    upload_data_to_big_query(TMP_DIR, TABLE_IDS)
