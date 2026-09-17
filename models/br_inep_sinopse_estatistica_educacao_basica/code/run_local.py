"""Roda o tratamento da Sinopse na máquina local, sem tocar no BigQuery.

Serve para desenvolver e conferir a transformação antes de subir qualquer coisa.
Três decisões o definem:

- usa o mapa fixo de unidades da federação, porque o service account de dev não
  tem permissão de criar job no BigQuery e `bd.read_sql` responde 403;
- nunca sobe nada, nem tem a opção — `utils.upload_tables` existe para quem
  precisar, mas subir dado é decisão de outro momento;
- compara o que produziu com as contagens de referência e sai com código de erro
  se divergir, para servir de verificação e não só de execução.

    uv run python run_local.py                     # todas as tabelas
    uv run python run_local.py -t faixa_etaria     # uma tabela
    uv run python run_local.py --output /tmp/x     # outro destino
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from constants import INPUT, OUTPUT, URL, YEAR, ZIP_PATH  # type: ignore
from utils import (  # type: ignore
    CLEANERS,
    clean_all,
    download_zip,
    find_workbook,
    load_municipio_ids,
    load_uf_map,
)

#: Linhas por tabela na Sinopse de 2025, conferidas contra a linha Brasil da
#: planilha. Divergir daqui significa que a transformação mudou — o que pode ser
#: certo, mas nunca por acidente. Atualize junto com a mudança.
LINHAS_ESPERADAS_2025 = {
    "etapa_ensino_serie": 646_236,
    "faixa_etaria": 200_556,
    "localizacao": 401_112,
    "tempo_ensino": 311_976,
    "sexo_raca_cor": 601_668,
    "docente_etapa_ensino": 1_136_484,
    "docente_localizacao": 1_002_780,
    "docente_escolaridade": 428_967,
    "docente_deficiencia": 122_562,
    "docente_faixa_etaria_sexo": 857_934,
    "docente_regime_contrato": 668_520,
}


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
        help="tabelas a tratar (padrão: todas)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help=f"onde escrever (padrão: {OUTPUT})",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="usa a planilha já em disco",
    )
    parser.add_argument(
        "--no-check",
        action="store_true",
        help="não compara com as contagens de referência",
    )
    return parser.parse_args()


def report(written: dict[str, int], check: bool) -> bool:
    """Imprime o que foi escrito e devolve se passou na conferência."""
    largura = max(len(name) for name in written)
    ok = True

    print(f"\n{'tabela':<{largura}}  {'linhas':>11}  {'referência':>11}")
    print("-" * (largura + 28))

    for table_id, rows in written.items():
        esperado = LINHAS_ESPERADAS_2025.get(table_id)
        if not check or esperado is None:
            veredito = ""
        elif rows == esperado:
            veredito = "  confere"
        else:
            veredito = f"  DIVERGE ({rows - esperado:+,})"
            ok = False
        alvo = f"{esperado:,}" if esperado is not None else "—"
        print(f"{table_id:<{largura}}  {rows:>11,}  {alvo:>11}{veredito}")

    print("-" * (largura + 28))
    print(f"{'total':<{largura}}  {sum(written.values()):>11,}")
    return ok


def main() -> int:
    args = parse_args()

    INPUT.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)

    if not args.skip_download:
        print(f"baixando de {URL}")
        download_zip(URL, ZIP_PATH, INPUT)

    workbook = find_workbook(INPUT)
    print(f"planilha: {workbook.name}")

    # sem billing_project_id: mapa fixo, nenhuma consulta ao BigQuery
    uf_map = load_uf_map()
    municipios = load_municipio_ids()
    print(
        "municípios: diretório da Base dos Dados"
        if municipios
        else "municípios: sem BigQuery, as tabelas são comparadas entre si"
    )

    written = clean_all(
        workbook, uf_map, YEAR, args.output, args.tables, municipios
    )
    ok = report(written, check=not args.no_check)

    print(f"\nsaída em {args.output}")
    if not ok:
        print(
            "\nAs contagens divergem da referência. Se a mudança foi "
            "intencional, atualize LINHAS_ESPERADAS_2025 no mesmo commit."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
