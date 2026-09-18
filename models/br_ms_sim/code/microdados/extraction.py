import os
import urllib.request
from pathlib import Path

# FTP do DATASUS — arquivos de obitos nao fetais (CID-10)
FTP_URL = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DO{uf}{ano}.dbc"

UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


def download_file(uf: str, ano: int, input_dir: str) -> Path | None:
    url = FTP_URL.format(uf=uf, ano=ano)
    destino = Path(input_dir) / f"DO{uf}{ano}.dbc"

    if destino.exists():
        print(f"  Arquivo ja existe, pulando: {destino.name}")
        return destino

    try:
        print(f"  Baixando: {destino.name}")
        urllib.request.urlretrieve(url, filename=str(destino))
        print(
            f"  Concluido: {destino.name} ({destino.stat().st_size // 1024} KB)"
        )
        return destino
    except Exception as e:
        print(f"  Arquivo nao encontrado: UF={uf}, ano={ano} — {e}")
        return None


def download(year_range: list[int], input_dir: str) -> None:
    os.makedirs(input_dir, exist_ok=True)

    for ano in year_range:
        for uf in UFS:
            download_file(uf, ano, input_dir)
