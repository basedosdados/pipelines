"""Download + clean one UF at a time, then delete the zip.

Usage:
    uv run python models/br_ibge_censo_demografico/code/process.py [--ufs RR,AC]
"""

from __future__ import annotations

import argparse

import clean
import download

from models.br_ibge_censo_demografico.code import constants


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ufs", default="", help="Comma-separated siglas")
    args = parser.parse_args()
    wanted = {u.strip().upper() for u in args.ufs.split(",") if u.strip()}
    download.download_docs()
    constants.INPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_counts: dict[str, dict[str, int]] = {}
    for _code, sigla, zip_name in constants.UF_ZIPS:
        if wanted and sigla not in wanted:
            continue
        zip_path = constants.INPUT_DIR / zip_name
        print(f"\n######## {sigla} ########", flush=True)
        download.download_file(f"{constants.FTP_CSV}/{zip_name}", zip_path)
        all_counts[sigla] = clean.clean_uf(sigla, zip_path, only=set())
        zip_path.unlink(missing_ok=True)
        print(f"  deleted {zip_name}", flush=True)
    all_counts["dicionario"] = {"dicionario": clean.write_dicionario()}
    clean._save_counts(all_counts)
    print("\nALL UFS CLEANED", flush=True)


if __name__ == "__main__":
    main()
