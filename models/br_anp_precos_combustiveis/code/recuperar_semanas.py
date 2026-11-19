"""
Recupera semanas de br_anp_precos_combustiveis que já saíram dos arquivos de 4 semanas.

Lê os arquivos semestrais da série histórica da ANP (combustíveis automotivos e GLP),
aplica o mesmo tratamento do flow (`tratar_precos`) e gera as pastas
`data_coleta=aaaa-mm-dd/data.csv` das datas do intervalo que faltam na staging de dev.
Nunca sobrescreve uma pasta que já existe. Ver "Recuperar semanas perdidas" no README
do pipeline.

Uso (na raiz do repositório):

    uv run python models/br_anp_precos_combustiveis/code/recuperar_semanas.py \\
        --inicio 2023-08-05 --fim 2023-08-20            # gera e confere, sem gravar
    uv run python models/br_anp_precos_combustiveis/code/recuperar_semanas.py \\
        --inicio 2023-08-05 --fim 2023-08-20 --gravar   # grava na staging de dev

Os arquivos baixados e as pastas geradas ficam em `$ANP_DATA_DIR`
(padrão: `~/Downloads/br_anp_precos_combustiveis_data`).
"""

import argparse
import base64
import collections
import datetime as dt
import hashlib
import io
import os
import re
import shutil
import zipfile
from pathlib import Path

import pandas as pd
import requests
from google.cloud import storage
from google.oauth2 import service_account

from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    TEXTO,
    partition_data,
    read_anp_file,
    tratar_precos,
)
from pipelines.utils.data_tests.rows import check_rows_written

PAGINA = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
}
# arquivos cujo nome não traz o semestre
SEMESTRE_FIXO = {"precos-semestrais-ca.zip": ("ca", 2022, 1)}
BUCKET = "basedosdados-dev"
PREFIXO = "staging/br_anp_precos_combustiveis/microdados/"
DATA_DIR = Path(
    os.environ.get(
        "ANP_DATA_DIR",
        Path.home() / "Downloads" / "br_anp_precos_combustiveis_data",
    )
)
CREDENCIAIS = Path.home() / ".basedosdados" / "credentials" / "staging.json"


def semestres(inicio: dt.date, fim: dt.date) -> list[tuple[int, int]]:
    """lista os semestres (ano, 1 ou 2) que cobrem o intervalo."""
    lista, ano, sem = [], inicio.year, 1 if inicio.month <= 6 else 2
    while (ano, sem) <= (fim.year, 1 if fim.month <= 6 else 2):
        lista.append((ano, sem))
        ano, sem = (ano, 2) if sem == 1 else (ano + 1, 1)
    return lista


def links_semestrais() -> dict[tuple[str, int, int], str]:
    """lê a página da série histórica e mapeia (tipo, ano, semestre) para a url."""
    html = requests.get(PAGINA, headers=HEADERS, timeout=60).text
    urls = set(re.findall(r'href="([^"]+/shpc/dsas/(?:ca|glp)/[^"]+)"', html))
    links = {}
    for url in urls:
        nome = url.rsplit("/", 1)[-1]
        tipo = url.split("/shpc/dsas/")[1].split("/")[0]
        if nome in SEMESTRE_FIXO:
            chave = SEMESTRE_FIXO[nome]
        else:
            m = re.search(r"(\d{4})-?0([12])", nome)
            if not m:
                raise ValueError(f"Semestre não identificado no link: {url}")
            chave = (tipo, int(m.group(1)), int(m.group(2)))
        if chave in links:
            raise ValueError(
                f"Dois links para {chave}: {links[chave]} e {url}"
            )
        links[chave] = url
    return links


def baixar(url: str, destino: Path) -> Path:
    """baixa o arquivo, conferindo o tamanho; reaproveita o que já foi baixado."""
    if destino.exists():
        return destino
    r = requests.get(url, headers=HEADERS, timeout=600)
    r.raise_for_status()
    esperado = r.headers.get("Content-Length")
    if esperado and int(esperado) != len(r.content):
        raise ValueError(
            f"Download cortado: {url} ({len(r.content)} de {esperado} bytes)"
        )
    tmp = destino.with_suffix(destino.suffix + ".part")
    tmp.write_bytes(r.content)
    tmp.rename(destino)
    return destino


def ler_semestral(path: Path) -> pd.DataFrame:
    """lê um arquivo semestral: csv solto ou zipado, em utf-8 ou latin-1."""
    if zipfile.is_zipfile(path):
        z = zipfile.ZipFile(path)
        if any(n.startswith("xl/") for n in z.namelist()):
            return read_anp_file(str(path))
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"Esperava 1 csv em {path}, achei {csvs}")
        raw = z.read(csvs[0])
    else:
        raw = path.read_bytes()
    try:
        texto = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = raw.decode("latin-1")
    return pd.read_csv(io.StringIO(texto), sep=";", dtype=TEXTO)


def pastas_existentes(bucket) -> set[str]:
    """datas que já têm pasta na staging de dev."""
    datas = set()
    for blob in bucket.client.list_blobs(bucket, prefix=PREFIXO):
        m = re.search(r"data_coleta=(\d{4}-\d{2}-\d{2})/", blob.name)
        if m:
            datas.add(m.group(1))
    return datas


def md5(path: Path) -> str:
    return base64.b64encode(hashlib.md5(path.read_bytes()).digest()).decode()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--inicio", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--fim", required=True, type=dt.date.fromisoformat)
    parser.add_argument(
        "--gravar", action="store_true", help="grava na staging de dev"
    )
    args = parser.parse_args()
    if args.fim < args.inicio:
        parser.error("--fim antes de --inicio")

    entrada = DATA_DIR / "input"
    saida = DATA_DIR / "recuperacao" / f"{args.inicio}_{args.fim}"
    entrada.mkdir(parents=True, exist_ok=True)
    if saida.exists():
        shutil.rmtree(saida)

    # 1. baixa e lê os semestrais do intervalo
    links = links_semestrais()
    partes = []
    for ano, sem in semestres(args.inicio, args.fim):
        for tipo in ("ca", "glp"):
            if (tipo, ano, sem) not in links:
                raise ValueError(
                    f"Sem arquivo semestral {tipo} {ano}-0{sem} na página da ANP"
                )
            url = links[(tipo, ano, sem)]
            arquivo = baixar(
                url,
                entrada
                / ("dsas_" + "_".join(url.split("/shpc/dsas/")[1].split("/"))),
            )
            df = ler_semestral(arquivo)
            datas = pd.to_datetime(
                df["Data da Coleta"], format="%d/%m/%Y"
            ).dt.date
            partes.append(df[(datas >= args.inicio) & (datas <= args.fim)])
            print(
                f"{tipo} {ano}-0{sem}: {len(df)} linhas, {len(partes[-1])} no intervalo"
            )
    bruto = pd.concat(partes, ignore_index=True)

    # 2. separa as datas que já estão na staging de dev
    cred = service_account.Credentials.from_service_account_file(CREDENCIAIS)
    bucket = storage.Client(project=BUCKET, credentials=cred).bucket(
        BUCKET, user_project=BUCKET
    )
    existentes = pastas_existentes(bucket)
    iso = pd.to_datetime(
        bruto["Data da Coleta"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")
    no_intervalo = sorted(iso.unique())
    ja_tem = [d for d in no_intervalo if d in existentes]
    faltam = [d for d in no_intervalo if d not in existentes]
    print(
        f"\ndatas no intervalo: {len(no_intervalo)} | já na staging de dev (ignoradas): {ja_tem}"
    )
    print(f"datas a recuperar: {faltam}")
    if not faltam:
        return
    bruto = bruto[iso.isin(faltam)].reset_index(drop=True)
    lidas = collections.Counter(iso[iso.isin(faltam)])

    # 3. trata como o flow e grava as partições
    tratado = tratar_precos(bruto)
    if len(tratado) != len(bruto):
        raise ValueError(
            f"O tratamento mudou o número de linhas: {len(bruto)} -> {len(tratado)}"
        )
    partition_data(
        tratado,
        # pyrefly: ignore [bad-argument-type]
        column_name="data_coleta",
        output_directory=str(saida),
    )
    check_rows_written(
        rows_read={f"data_coleta={d}": n for d, n in lidas.items()},
        output_dir=saida,
    )

    # 4. resumo para conferência
    print("\ndata        linhas  sem_id_municipio  linhas_identicas")
    for d in faltam:
        t = tratado[tratado["data_coleta"] == d]
        print(
            f"{d}  {len(t):6d}  {t['id_municipio'].eq('').sum():16d}  {t.duplicated().sum():16d}"
        )
    print(f"\npastas geradas em {saida}")

    # 5. grava só pastas novas (if_generation_match=0 recusa sobrescrever)
    if not args.gravar:
        print("nada gravado (use --gravar)")
        return
    for d in faltam:
        local = saida / f"data_coleta={d}" / "data.csv"
        blob = bucket.blob(f"{PREFIXO}data_coleta={d}/data.csv")
        blob.upload_from_filename(
            str(local), content_type="text/csv", if_generation_match=0
        )
        blob.reload()
        status = "ok" if blob.md5_hash == md5(local) else "MD5 DIFERENTE"
        print(f"gravado {blob.name} ({blob.size} bytes, md5 {status})")


if __name__ == "__main__":
    main()
