"""Roda o tratamento da tabela `escola` do Censo Escolar na máquina local.

Serve para desenvolver e conferir a transformação antes de subir qualquer
coisa. Três decisões o definem:

- lê o schema da tabela publicada pela API de metadados, que não cria job no
  BigQuery e por isso funciona com a credencial de desenvolvimento;
- nunca sobe nada, nem tem a opção — `utils.upload_table` existe para quem
  precisar, mas subir dado é decisão de outro momento;
- imprime as colunas que ficaram sem origem nesta edição, que é como se separa
  o que o INEP descontinuou do que o tratamento perdeu.

O download e a extração acontecem só se os CSVs não estiverem em disco.

    uv run python run_local.py                 # trata
    uv run python run_local.py --output /tmp/x # outro destino
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from constants import (  # type: ignore
    ANO,
    ARCHITECTURE_URL,
    BILLING_PROJECT_ID,
    COLUNAS_NAO_PUBLICADAS,
    DATASET_ID,
    INPUT,
    OUTPUT,
    PROJECT_ID,
    TABELAS,
    TABLE_ID,
    URL,
    ZIP_PATH,
)
from utils import (  # type: ignore
    align_columns,
    build_escola,
    colunas_da_edicao,
    fill_sigla_uf,
    find_csv,
    load_table_columns,
    prepare_input,
    read_architecture_table,
    read_table,
    write_partitioned,
)

#: Escolas na edição 2025, uma linha por escola. Divergir daqui significa que a
#: transformação ou a fonte mudaram — o que pode ser certo, mas nunca por
#: acidente. Atualize junto com a mudança.
LINHAS_ESPERADAS = 214_192


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help=f"onde escrever (padrão: {OUTPUT})",
    )
    parser.add_argument(
        "--no-check",
        action="store_true",
        help="não compara com a contagem de referência",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    INPUT.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)

    prepare_input(URL, ZIP_PATH, INPUT, TABELAS)

    architecture = colunas_da_edicao(
        read_architecture_table(ARCHITECTURE_URL), ANO
    )
    print(f"arquitetura: {len(architecture)} colunas vigentes em {ANO}")

    dfs = {}
    for tabela in TABELAS:
        path = find_csv(INPUT, tabela)
        dfs[tabela] = read_table(path, architecture)
        print(
            f"{tabela:<10} {path.name:<32} "
            f"{len(dfs[tabela]):>8,} linhas  "
            f"{len(dfs[tabela].columns):>3} colunas"
        )

    escola = fill_sigla_uf(build_escola(dfs))
    colunas = load_table_columns(
        PROJECT_ID, DATASET_ID, TABLE_ID, BILLING_PROJECT_ID
    )
    escola, faltando = align_columns(
        escola, colunas, nao_publicadas=COLUNAS_NAO_PUBLICADAS
    )

    # A tabela guarda colunas de anos anteriores, que esta edição não tem e
    # cuja cobertura temporal já foi encerrada — essas são nulas por desenho.
    # As que interessam conferir são as que a arquitetura ainda dá como
    # correntes: em 2025 as duas de poder público estavam aqui por defeito do
    # tratamento, e não por ausência na fonte.
    vigentes = set(architecture["name"])
    sem_origem = [col for col in faltando if col in vigentes]
    encerradas = len(faltando) - len(sem_origem)

    print(
        f"\n{len(faltando)} colunas preenchidas com nulo: {encerradas} com "
        f"cobertura encerrada antes de {ANO} e {len(sem_origem)} correntes "
        "sem coluna de origem na fonte:"
    )
    for col in sem_origem:
        print(f"  {col}")

    written = write_partitioned(escola, TABLE_ID, ANO, args.output)
    print(f"\n{written:,} linhas escritas em {args.output}")

    if not args.no_check and written != LINHAS_ESPERADAS:
        print(
            f"\nA contagem diverge da referência ({LINHAS_ESPERADAS:,}). Se a "
            "mudança foi intencional, atualize LINHAS_ESPERADAS no mesmo commit."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
