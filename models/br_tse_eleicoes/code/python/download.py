"""
Download one election year of TSE bulk files into INPUT_DIR, in the layout the
builders expect: ``<family>/<zip_stem>.zip`` extracted to ``<family>/<zip_stem>/``.

Usage:
    TSE_DATA_DIR=~/Library/Caches/br_tse_eleicoes_data \
        uv run -m models.br_tse_eleicoes.code.python.download 2026 [family ...]

Re-running skips zips whose size matches the server's Content-Length, so an
interrupted run resumes. A file the CDN does not have yet (404) is reported
and skipped rather than failing the run.
"""

import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from models.br_tse_eleicoes.code.python.config import ALL_UFS, INPUT_DIR

CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://dadosabertos.tse.jus.br/",
}

# family (INPUT_DIR subfolder) -> list of CDN paths, {ano}/{uf} templated
FAMILIES: dict[str, list[str]] = {
    "consulta_cand": [
        "consulta_cand/consulta_cand_{ano}.zip",
        "consulta_cand_complementar/consulta_cand_complementar_{ano}.zip",
    ],
    "bem_candidato": ["bem_candidato/bem_candidato_{ano}.zip"],
    "consulta_coligacao": ["consulta_coligacao/consulta_coligacao_{ano}.zip"],
    "consulta_vagas": ["consulta_vagas/consulta_vagas_{ano}.zip"],
    "votacao_candidato_munzona": [
        "votacao_candidato_munzona/votacao_candidato_munzona_{ano}.zip"
    ],
    "votacao_partido_munzona": [
        "votacao_partido_munzona/votacao_partido_munzona_{ano}.zip"
    ],
    "detalhe_votacao_munzona": [
        "detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}.zip"
    ],
    "detalhe_votacao_secao": [
        "detalhe_votacao_secao/detalhe_votacao_secao_{ano}.zip"
    ],
    "votacao_secao": [
        f"votacao_secao/votacao_secao_{{ano}}_{uf}.zip"
        for uf in [*ALL_UFS, "BR", "ZZ"]
    ],
    "perfil_eleitorado": ["perfil_eleitorado/perfil_eleitorado_{ano}.zip"],
    "perfil_eleitorado_secao": [
        f"perfil_eleitor_secao/perfil_eleitor_secao_{{ano}}_{uf}.zip"
        for uf in [*ALL_UFS, "ZZ"]
    ],
    "perfil_eleitorado_local_votacao": [
        "eleitorado_locais_votacao/eleitorado_local_votacao_{ano}.zip"
    ],
    "prestacao_contas": [
        "prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}.zip",
        "prestacao_contas/prestacao_de_contas_eleitorais_orgaos_partidarios_{ano}.zip",
    ],
}


def _fetch(family: str, path: str) -> str:
    url = f"{CDN}/{path}"
    dest_dir = INPUT_DIR / family
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / Path(path).name

    head = requests.head(url, headers=HEADERS, timeout=60)
    if head.status_code == 404:
        return f"404   {path}"
    head.raise_for_status()
    size = int(head.headers.get("Content-Length", -1))

    if not (zip_path.exists() and zip_path.stat().st_size == size):
        tmp = zip_path.with_suffix(".part")
        with requests.get(url, headers=HEADERS, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
        if tmp.stat().st_size != size:
            msg = f"{path}: got {tmp.stat().st_size} bytes, expected {size}"
            raise OSError(msg)
        tmp.rename(zip_path)

    out_dir = dest_dir / zip_path.stem
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out_dir)
    return f"ok    {path} ({size / 1e6:.0f} MB)"


def download(ano: int, families: list[str] | None = None) -> None:
    jobs = [
        (fam, p.format(ano=ano))
        for fam, paths in FAMILIES.items()
        if not families or fam in families
        for p in paths
    ]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for msg in pool.map(lambda j: _fetch(*j), jobs):
            print(msg, flush=True)


if __name__ == "__main__":
    download(int(sys.argv[1]), sys.argv[2:] or None)
