import argparse
import datetime
import io
import re
import zipfile
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from bs4 import BeautifulSoup

from pipelines.utils.tasks import _upload_to_gcs

DATASET_ID = "br_bd_diretorios_brasil"
TABLE_ID = "natureza_juridica"
FINAL_COLUMNS = ["id_natureza_juridica", "descricao", "escopo"]

# Mesma fonte usada em pipelines/datasets/br_rf_cnpj/constants.py
URL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/"
HEADERS = {
    "Depth": "1",
    "Content-Type": "application/xml",
    "Accept": "application/xml",
    "User-Agent": "Mozilla/5.0",
}
XML_BODY = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:allprop/>
</d:propfind>
"""


def _propfind(url: str) -> list[str]:
    response = requests.request(
        method="PROPFIND",
        url=url,
        headers=HEADERS,
        data=XML_BODY,
        timeout=60,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    return [h.get_text(strip=True) for h in soup.find_all("d:href")]


def latest_folder_url() -> str:
    """Retorna a URL da pasta (YYYY-MM) mais recente da Receita Federal."""
    folders = [
        href.rstrip("/").split("/")[-1]
        for href in _propfind(URL)
        if re.fullmatch(r"\d{4}-\d{2}", href.rstrip("/").split("/")[-1])
    ]
    latest = max(folders, key=lambda x: datetime.datetime.strptime(x, "%Y-%m"))
    print(f"Pasta mais recente da Receita Federal: {latest}")
    return f"{URL}{latest}/"


def download_naturezas() -> pd.DataFrame:
    """Baixa e lê o arquivo Naturezas*.zip da Receita Federal."""
    folder_url = latest_folder_url()
    files = [
        h.split("/")[-1]
        for h in _propfind(folder_url)
        if re.search(r"naturezas.*\.zip", h, re.IGNORECASE)
    ]
    if not files:
        raise FileNotFoundError(
            f"Naturezas.zip não encontrado em {folder_url}"
        )

    print(f"Baixando {files[0]}")
    response = requests.get(
        folder_url + files[0],
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=120,
    )
    response.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(response.content))
    with zf.open(zf.namelist()[0]) as fp:
        df = pd.read_csv(
            fp,
            sep=";",
            encoding="latin1",
            header=None,
            names=["id_natureza_juridica", "descricao"],
            dtype=str,
        )

    df["id_natureza_juridica"] = (
        df["id_natureza_juridica"].str.strip().str.zfill(4)
    )
    df["descricao"] = df["descricao"].str.strip()
    return df.dropna(subset=["id_natureza_juridica"]).drop_duplicates(
        subset=["id_natureza_juridica"]
    )


def read_existing(billing_project_id: str) -> pd.DataFrame:
    """Lê a tabela atual em produção."""
    df = bd.read_sql(
        f"select * from `basedosdados.{DATASET_ID}.{TABLE_ID}`",
        billing_project_id=billing_project_id,
    )
    df["id_natureza_juridica"] = (
        df["id_natureza_juridica"].astype(str).str.strip().str.zfill(4)
    )
    return df[FINAL_COLUMNS]


def build_final(
    existing: pd.DataFrame, baixados: pd.DataFrame
) -> pd.DataFrame:
    """Mantém as linhas existentes e acrescenta apenas os códigos novos."""
    novos = baixados[
        ~baixados["id_natureza_juridica"].isin(
            existing["id_natureza_juridica"]
        )
    ].copy()
    novos["escopo"] = None
    novos = novos[~novos["id_natureza_juridica"].isin(["0000", "8885"])]

    print(f"Existentes: {len(existing)} | Baixados: {len(baixados)}")
    print(f"Códigos novos ({len(novos)}):")
    print(novos[["id_natureza_juridica", "descricao"]].to_string(index=False))

    ausentes = existing[
        ~existing["id_natureza_juridica"].isin(
            baixados["id_natureza_juridica"]
        )
    ]
    if not ausentes.empty:
        print(
            f"Aviso: {len(ausentes)} códigos existentes não estão no arquivo da RF (mantidos):"
        )
        print(
            ausentes[["id_natureza_juridica", "descricao"]].to_string(
                index=False
            )
        )

    final = pd.concat([existing, novos], ignore_index=True)
    return final[FINAL_COLUMNS].sort_values("id_natureza_juridica")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--billing-project-id", required=True)
    args = parser.parse_args()

    root_dir = Path(__file__).parent.parent.parent.parent
    output_dir = root_dir / "tmp" / DATASET_ID / TABLE_ID / "output"
    output_dir.mkdir(exist_ok=True, parents=True)

    existing = read_existing(args.billing_project_id)
    baixados = download_naturezas()
    final = build_final(existing, baixados)

    output_path = output_dir / "natureza_juridica.csv"
    final.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Arquivo salvo em {output_path}")

    _upload_to_gcs(
        data_path=output_path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name="basedosdados-dev",
    )
