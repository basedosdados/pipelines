"""
TRATAMENTO ― Índice Brasileiro de Conectividade (IBC) · Anatel
--------------------------------------------------------------

Baixa o pacote `ibc.zip` do portal de Dados Abertos da Anatel,
extrai **IBC_municipios_indicadores_normalizados.csv**,
padroniza nomes/formatos de colunas e grava em Parquet:

    Dados siscan/ibc_data/br_anatel_indice_brasileiro_conectividade.parquet
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests

# ------------------------------------------------------------------
# Configurações
IBC_URL = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/meu_municipio/ibc.zip"
BASE_DIR = Path(r"models/br_anatel_indice_brasileiro_conectividade")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
ZIP_PATH = INPUT_DIR / "ibc.zip"
EXTRACT_DIR = INPUT_DIR / "ibc_extracted"
CSV_TARGET = "IBC_municipios_indicadores_normalizados.csv"
PARQUET_OUT = OUTPUT_DIR / "br_anatel_indice_brasileiro_conectividade.parquet"

DATASET_ID = "br_anatel_indice_brasileiro_conectividade"  # Nome do dataset
TABLE_ID = "municipio"  # Nome da tabela

NUM_COLS = [
    "ibc",
    "cobertura_pop_4g5g",
    "hhi_smp",
    "densidade_scm",
    "hhi_scm",
    "densidade_smp",
    "adensamento_estacoes",
    "fibra",
]


# ------------------------------------------------------------------
def download_zip(url: str, dest: Path) -> None:
    if dest.exists():
        print("Zip já existe - pulando download")
        return
    print("Baixando zip …")
    dest.write_bytes(requests.get(url, timeout=90).content)
    print("Download concluído")


def extract_csv(zip_path: Path, member: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        if member not in zf.namelist():
            raise FileNotFoundError(f"{member} não encontrado no zip")
        zf.extract(member, out_dir)
    print(f"CSV extraído → {out_dir / member}")
    return out_dir / member


def load_transform(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="utf-8",
        na_values=["", "NA", "N/A", "null", "-"],
        dtype=str,  # tudo string primeiro
    ).drop(columns=["Município", "Cobertura área agricultável"])

    # tipos
    df["ano"] = df["Ano"].astype(str)
    df["id_municipio"] = df["Código Município"].astype(str)
    df["sigla_uf"] = df["UF"].astype(str)

    df = df.rename(
        columns={
            "IBC": "ibc",
            "Cobertura Pop. 4G5G": "cobertura_pop_4g5g",
            "Fibra": "fibra",
            "Densidade SMP": "densidade_smp",
            "HHI SMP": "hhi_smp",
            "Densidade SCM": "densidade_scm",
            "HHI SCM": "hhi_scm",
            "Adensamento Estações": "adensamento_estacoes",
        }
    )[["ano", "id_municipio", "sigla_uf", *NUM_COLS]]

    # numéricos
    for col in NUM_COLS:
        df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="coerce")

    return df


# ------------------------------------------------------------------
def main() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    download_zip(IBC_URL, ZIP_PATH)
    csv_path = extract_csv(ZIP_PATH, CSV_TARGET, EXTRACT_DIR)
    df = load_transform(csv_path)

    df.to_parquet(PARQUET_OUT, index=False)
    print(f"Parquet salvo: {PARQUET_OUT}  |  {len(df):,} linhas")

    # Grava no BasedosDados
    tb = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)

    tb.create(
        path=PARQUET_OUT,  # Caminho para o arquivo csv ou parquet
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="parquet",
    )


# ------------------------------------------------------------------
if __name__ == "__main__":
    main()
