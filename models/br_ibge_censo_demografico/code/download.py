"""Download Censo 2022 public microdata (per-UF CSV zips) and the layout.

Usage:
    uv run python models/br_ibge_censo_demografico/code/download.py [--ufs RR,AC]
"""

from __future__ import annotations

import argparse
import ssl
import time
import urllib.request
from pathlib import Path

import certifi

from models.br_ibge_censo_demografico.code import constants

_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def download_file(url: str, dest: Path, retries: int = 3) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"skip {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
        return
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(retries):
        try:
            with (
                urllib.request.urlopen(
                    url, context=_SSL_CONTEXT, timeout=120
                ) as src,
                tmp.open("wb") as out,
            ):
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            tmp.replace(dest)
            print(
                f"downloaded {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)"
            )
            return
        except Exception as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"failed {url}: {exc}") from exc
            time.sleep(2**attempt)


def download_docs() -> None:
    constants.DOCS_DIR.mkdir(parents=True, exist_ok=True)
    download_file(
        f"{constants.FTP_DOCS}/Layout%20Microdados%20CD2022%20-%20acesso%20P%c3%bablico.xlsx",
        constants.DOCS_DIR / constants.LAYOUT_XLSX_NAME,
    )
    download_file(
        f"{constants.FTP_DOCS}/Dicion%c3%a1rio%20de%20Vari%c3%a1veis%20-%20Microdados%20CD2022.pdf",
        constants.DOCS_DIR / "dicionario_variaveis.pdf",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ufs", default="", help="Comma-separated siglas, e.g. RR,AC"
    )
    args = parser.parse_args()
    wanted = {u.strip().upper() for u in args.ufs.split(",") if u.strip()}
    download_docs()
    constants.INPUT_DIR.mkdir(parents=True, exist_ok=True)
    for _code, sigla, zip_name in constants.UF_ZIPS:
        if wanted and sigla not in wanted:
            continue
        download_file(
            f"{constants.FTP_CSV}/{zip_name}", constants.INPUT_DIR / zip_name
        )


if __name__ == "__main__":
    main()
