"""
Recupera semanas de br_anp_precos_combustiveis que já saíram dos arquivos de 4 semanas.

Lê os arquivos semestrais da série histórica da ANP (combustíveis automotivos e GLP),
aplica o mesmo tratamento do flow (`tratar_precos`) e gera as pastas
`data_coleta=aaaa-mm-dd/data.csv` das datas do intervalo que faltam na staging de dev.
Sem `--substituir`, nunca sobrescreve uma pasta que já existe. Ver "Recuperar semanas perdidas" no README
do pipeline.

Uso (na raiz do repositório):

    uv run python models/br_anp_precos_combustiveis/code/recuperar_semanas.py \\
        --inicio 2023-08-05 --fim 2023-08-20            # gera e confere, sem gravar
    uv run python models/br_anp_precos_combustiveis/code/recuperar_semanas.py \\
        --inicio 2023-08-05 --fim 2023-08-20 --gravar   # grava na staging de dev

Com `--substituir <prefixo do backup>`, regera também as datas que já existem (para
reconstruir a staging). O script confere antes que o backup tem os mesmos arquivos da
staging atual.

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
from concurrent.futures import ThreadPoolExecutor
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


def md5(path: Path) -> str:
    return base64.b64encode(hashlib.md5(path.read_bytes()).digest()).decode()


def blobs(bucket, prefixo: str) -> dict[str, tuple[str, int]]:
    """nome relativo -> (md5, geração) dos arquivos sob o prefixo."""
    return {
        b.name[len(prefixo) :]: (b.md5_hash, b.generation)
        for b in bucket.client.list_blobs(bucket, prefix=prefixo)
    }


def gerar_semestre(
    links: dict, ano: int, sem: int, inicio: dt.date, fim: dt.date, saida: Path
) -> pd.DataFrame:
    """gera as pastas das datas do semestre no intervalo e devolve o resumo por data."""
    partes = []
    for tipo in ("ca", "glp"):
        if (tipo, ano, sem) not in links:
            raise ValueError(
                f"Sem arquivo semestral {tipo} {ano}-0{sem} na página da ANP"
            )
        url = links[(tipo, ano, sem)]
        nome = "dsas_" + "_".join(url.split("/shpc/dsas/")[1].split("/"))
        partes.append(ler_semestral(baixar(url, DATA_DIR / "input" / nome)))
    bruto = pd.concat(partes, ignore_index=True)
    datas = pd.to_datetime(
        bruto["Data da Coleta"], format="%d/%m/%Y", errors="coerce"
    )
    # linhas sem data são linhas em branco do arquivo (ex.: ca-2025-01)
    sem_data = datas.isna()
    if bruto[sem_data].notna().any(axis=None):
        raise ValueError(
            f"{ano}-0{sem}: linha com conteúdo e sem data de coleta"
        )
    no_intervalo = (datas.dt.date >= inicio) & (datas.dt.date <= fim)
    bruto = bruto[no_intervalo].reset_index(drop=True)
    if bruto.empty:
        return pd.DataFrame()
    lidas = collections.Counter(datas[no_intervalo].dt.strftime("%Y-%m-%d"))

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
    resumo = tratado.groupby("data_coleta").agg(
        linhas=("data_coleta", "size"),
        sem_id_municipio=("id_municipio", lambda x: int(x.eq("").sum())),
    )
    resumo["linhas_identicas"] = (
        tratado[tratado.duplicated()].groupby("data_coleta").size()
    )
    return resumo.fillna(0).astype(int).reset_index()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--inicio", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--fim", required=True, type=dt.date.fromisoformat)
    parser.add_argument(
        "--gravar", action="store_true", help="grava na staging de dev"
    )
    parser.add_argument(
        "--substituir",
        metavar="PREFIXO_BACKUP",
        help="regera também as datas que já existem; exige o prefixo de um backup "
        "da staging no mesmo bucket, idêntico ao estado atual",
    )
    args = parser.parse_args()
    if args.fim < args.inicio:
        parser.error("--fim antes de --inicio")

    cred = service_account.Credentials.from_service_account_file(CREDENCIAIS)
    bucket = storage.Client(project=BUCKET, credentials=cred).bucket(
        BUCKET, user_project=BUCKET
    )
    atuais = blobs(bucket, PREFIXO)
    if args.substituir:
        backup = blobs(bucket, args.substituir.rstrip("/") + "/")
        faltando = [
            n for n, (h, _) in atuais.items() if backup.get(n, (None,))[0] != h
        ]
        if faltando:
            raise ValueError(
                f"Backup incompleto: {len(faltando)} arquivos, ex.: {faltando[:3]}"
            )
        print(
            f"backup conferido: {len(atuais)} arquivos idênticos em {args.substituir}"
        )

    saida = DATA_DIR / "recuperacao" / f"{args.inicio}_{args.fim}"
    (DATA_DIR / "input").mkdir(parents=True, exist_ok=True)
    if saida.exists():
        shutil.rmtree(saida)

    # 1. gera um semestre por vez
    links = links_semestrais()
    resumos = []
    for ano, sem in semestres(args.inicio, args.fim):
        r = gerar_semestre(links, ano, sem, args.inicio, args.fim, saida)
        if not r.empty:
            print(f"{ano}-0{sem}: {len(r)} datas, {r.linhas.sum()} linhas")
            resumos.append(r)
    resumo = pd.concat(resumos, ignore_index=True)
    resumo["existia"] = [
        f"data_coleta={d}/data.csv" in atuais for d in resumo.data_coleta
    ]

    # 2. só as datas novas, salvo com --substituir
    if not args.substituir:
        for d in resumo[resumo.existia].data_coleta:
            shutil.rmtree(saida / f"data_coleta={d}")
        resumo = resumo[~resumo.existia]
    resumo.to_csv(saida / "resumo.csv", index=False)
    print(
        f"\ndatas geradas: {len(resumo)} ({resumo.existia.sum()} substituem pastas "
        f"existentes) | linhas: {resumo.linhas.sum()} | sem id_municipio: "
        f"{resumo.sem_id_municipio.sum()} | linhas idênticas: {resumo.linhas_identicas.sum()}"
    )
    print(f"resumo por data em {saida / 'resumo.csv'}")
    if not args.gravar:
        print("nada gravado (use --gravar)")
        return

    # 3. grava; a geração impede sobrescrever um arquivo que mudou desde a listagem
    def gravar(d: str) -> str:
        nome = f"data_coleta={d}/data.csv"
        local = saida / nome
        blob = bucket.blob(PREFIXO + nome)
        geracao = atuais[nome][1] if nome in atuais else 0
        blob.upload_from_filename(
            str(local), content_type="text/csv", if_generation_match=geracao
        )
        blob.reload()
        return "ok" if blob.md5_hash == md5(local) else f"MD5 DIFERENTE {d}"

    with ThreadPoolExecutor(max_workers=16) as ex:
        status = collections.Counter(ex.map(gravar, resumo.data_coleta))
    print(f"gravados: {dict(status)}")


if __name__ == "__main__":
    main()
