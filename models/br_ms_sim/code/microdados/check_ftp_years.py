"""Verifica quais anos/UFs existem no FTP DATASUS (SIM CID10 DORES)."""

import urllib.request

# pyrefly: ignore [missing-import]
from extraction import UFS

FTP_URL = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DO{uf}{ano}.dbc"


def file_exists(uf: str, ano: int) -> bool:
    url = FTP_URL.format(uf=uf, ano=ano)
    try:
        urllib.request.urlopen(url)
        return True
    except Exception:
        return False


def check_year(ano: int) -> None:
    found = [uf for uf in UFS if file_exists(uf, ano)]
    print(f"\n=== Ano {ano} ===")
    print(f"  UFs disponíveis: {len(found)}/27")
    if found:
        print(f"  Lista: {', '.join(found)}")
    else:
        print("  Nenhum arquivo encontrado no FTP.")


if __name__ == "__main__":
    for year in (2023, 2024, 2025):
        check_year(year)
