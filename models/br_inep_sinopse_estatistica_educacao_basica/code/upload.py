"""Sobe o que está em `output/` para o staging de `basedosdados-dev`.

Separado do `run_local.py` de propósito: aquele trata e confere, este envia.

Duas coisas para saber antes de rodar:

- a carga sobrescreve arquivo por arquivo, então os anos que não estão em
  `output/` continuam no bucket como estão. O nome do arquivo tem de bater com
  o que já está lá (`data.csv`), senão a tabela externa lê os dois e a partição
  sai em dobro;
- produção não é tocada. Quem materializa `basedosdados.<dataset>.*` é o
  `table-approve` quando o PR é mesclado.

    uv run python upload.py                         # as tabelas em output/
    uv run python upload.py -t localizacao          # uma tabela
    uv run python upload.py --output /tmp/x         # de outro diretório
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from constants import DATASET_ID, OUTPUT  # type: ignore
from utils import CLEANERS, upload_tables  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs="+",
        choices=sorted(CLEANERS),
        metavar="TABELA",
        help="tabelas a subir (padrão: todas as que estiverem em output/)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help=f"de onde subir (padrão: {OUTPUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.output.is_dir():
        print(f"{args.output} não existe — rode o run_local.py primeiro")
        return 1

    print(f"subindo de {args.output} para {DATASET_ID}_staging")
    upload_tables(args.output, DATASET_ID, args.tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())
