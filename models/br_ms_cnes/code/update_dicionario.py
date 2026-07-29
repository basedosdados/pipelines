"""Acrescenta ao dicionário do br_ms_cnes os códigos novos do DATASUS.

O dicionário deste conjunto não é gerado por crawler nem por flow: é um CSV
único no staging, mantido à mão. Sempre que o DATASUS cria um código novo, o
teste `custom_dictionary_coverage` quebra e o flow morre depois de
materializar. Este script existe para que a próxima vez seja um diff
revisável.

Uso:
    export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/credentials-dev.json"
    cd models/br_ms_cnes/code

    # Conferir o que será acrescentado (não altera nada)
    uv run python update_dicionario.py --dry-run

    # Aplicar: grava o CSV e sobe para o staging dev
    uv run python update_dicionario.py --apply
"""

from __future__ import annotations

import argparse
from pathlib import Path

import basedosdados as bd
import pandas as pd

DATASET_ID = "br_ms_cnes"
TABLE_ID = "dicionario"
BILLING_PROJECT = "basedosdados-dev"
STAGING_TABLE = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{TABLE_ID}"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_CSV = OUTPUT_DIR / "dicionario.csv"

COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]

# Todas as 1.591 linhas do dicionário usam "(1)" neste campo. Mantemos por
# consistência, embora a notação correta fosse "INICIO(1)FIM".
COBERTURA = "(1)"

# Códigos criados pelo DATASUS e ausentes do dicionário. Procedência por linha:
#
#   12, 13  Portaria SAES/MS nº 3.695, de 15/01/2026, Art. 2º §1º — primária
#   11, 14, 15, 16
#           Portaria SAES/MS nº 4.109/2026, citada por terceiros; não
#           localizamos o texto oficial (ver issue #1714)
#   82      CNES 4.8.40, divulgado por terceiros; idem
#
# As tabelas auxiliares oficiais (TAB_CNES.zip, regeradas em 21/07/2026) ainda
# trazem só 10 tipos de equipamento e param no 76 em EQUIPE.dbf — não servem.
NEW_ROWS: list[dict[str, str]] = [
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "11",
        "valor": "Avaliação Antropométrica e Funcional",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "12",
        "valor": "Radioterapia",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "13",
        "valor": "Quimioterapia",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "14",
        "valor": "Reabilitação",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "15",
        "valor": "Procedimentos Clínicos",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "16",
        "valor": "Procedimentos Cirúrgicos",
    },
    {
        "id_tabela": "equipe",
        "nome_coluna": "tipo_equipe",
        "chave": "82",
        "valor": "E-DOT - Equipe Hospitalar de Doação para Transplantes",
    },
]

# Rótulos que substituem tapa-buracos já presentes no dicionário — as chaves
# 77 a 81 foram registradas com o valor literal "Não encontrado nos dicionários
# oficiais" só para o `custom_dictionary_coverage` passar. Procedência:
#
#   77  Portaria SAES/MS nº 1.619/2024, Art. 3º §4º — texto oficial conferido
#   78, 79
#       Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024
#   80  Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº
#       2.070/2024. Não foi possível ler o texto oficial (bvsms recusou
#       conexão); a sigla aparece por extenso no nome das próprias equipes
#       cadastradas ("EAP-DESINST ESTADUAL SAO LUIS"). Rótulo abreviado: o
#       nome completo tem 137 caracteres contra ~60 do resto do arquivo.
#   81  Portaria SAES/MS nº 3.200/2025, Art. 5º — texto oficial conferido
#
# Os cinco códigos entram nos dados em 2025-11 (o 81 em 2025-12), ligados no
# CNES na mesma leva apesar de portarias de anos diferentes.
REPLACE_ROWS: dict[tuple[str, str, str], str] = {
    ("equipe", "tipo_equipe", "77"): (
        "EMAP-R - Equipe Multiprofissional de Apoio para Reabilitação"
    ),
    ("equipe", "tipo_equipe", "78"): (
        "EACP - Equipe Assistencial de Cuidados Paliativos"
    ),
    ("equipe", "tipo_equipe", "79"): (
        "EMCP - Equipe Matricial de Cuidados Paliativos"
    ),
    ("equipe", "tipo_equipe", "80"): (
        "EAP-DESINST - Equipe de Avaliação e Acompanhamento de Medidas "
        "Terapêuticas (transtorno mental em conflito com a lei)"
    ),
    ("equipe", "tipo_equipe", "81"): (
        "EqAE - Equipe de Atenção Especializada"
    ),
}


def load_staging_dictionary() -> pd.DataFrame:
    """Carrega o dicionário completo do staging.

    Returns:
        DataFrame com as colunas do dicionário, tudo como string. O `fillna`
        vem antes do `astype` de propósito: na ordem inversa, um nulo vira a
        string literal "nan".
    """
    sql = f"SELECT * FROM `{STAGING_TABLE}`"
    dicionario = bd.read_sql(
        sql, billing_project_id=BILLING_PROJECT, from_file=True
    )
    return dicionario.fillna("").astype(str)


def add_rows(atual: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Acrescenta as linhas novas, ignorando as que já existem.

    A chave de identidade é (id_tabela, nome_coluna, chave), para que rodar o
    script duas vezes não duplique nada.

    Args:
        atual: Dicionário como está hoje no staging.

    Returns:
        Par (linhas acrescentadas, dicionário atualizado).
    """
    existentes = set(
        zip(
            atual.id_tabela,
            atual.nome_coluna,
            atual.chave,
            strict=True,
        )
    )
    pendentes = [
        linha
        for linha in NEW_ROWS
        if (linha["id_tabela"], linha["nome_coluna"], linha["chave"])
        not in existentes
    ]

    novas = pd.DataFrame(pendentes)
    if novas.empty:
        return novas, atual

    novas["cobertura_temporal"] = COBERTURA
    novas = novas[COLUMNS]
    atualizado = pd.concat([atual[COLUMNS], novas], ignore_index=True)
    return novas, atualizado


def replace_rows(atual: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reescreve o valor de linhas que já existem no dicionário.

    Idempotente: linhas cujo valor já é o esperado não entram no relatório e
    não são tocadas.

    Args:
        atual: Dicionário como está hoje.

    Returns:
        Par (relatório das linhas alteradas, dicionário atualizado).
    """
    atualizado = atual.copy()
    relatorio = []

    for (tabela, coluna, chave), novo in REPLACE_ROWS.items():
        alvo = (
            (atualizado.id_tabela == tabela)
            & (atualizado.nome_coluna == coluna)
            & (atualizado.chave == chave)
        )
        antigos = atualizado.loc[alvo, "valor"].unique()
        if not len(antigos) or (len(antigos) == 1 and antigos[0] == novo):
            continue
        relatorio.append({"chave": chave, "de": antigos[0], "para": novo})
        atualizado.loc[alvo, "valor"] = novo

    return pd.DataFrame(relatorio), atualizado


def summarize(
    novas: pd.DataFrame, alteradas: pd.DataFrame, atualizado: pd.DataFrame
) -> None:
    """Imprime o resumo da operação.

    Args:
        novas: Linhas que serão acrescentadas.
        alteradas: Linhas existentes que terão o valor reescrito.
        atualizado: Dicionário resultante.
    """
    if novas.empty:
        print("Nenhuma linha a acrescentar.")
    else:
        print(f"Linhas a acrescentar: {len(novas)}")
        print(novas.to_string(index=False))

    print()

    if alteradas.empty:
        print("Nenhum rótulo a reescrever.")
    else:
        print(f"Rótulos a reescrever: {len(alteradas)}")
        for linha in alteradas.itertuples(index=False):
            print(f"  {linha.chave}: {linha.de!r}")
            print(f"      -> {linha.para!r}")

    print(f"\nTotal depois: {len(atualizado)}")


def upload_dictionary(path: Path) -> None:
    """Envia o CSV atualizado para o staging.

    Args:
        path: Caminho do CSV a subir.
    """
    table = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
    table.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print(f"Upload concluído: staging/{DATASET_ID}/{TABLE_ID}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Acrescenta códigos novos do DATASUS ao dicionário."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Grava o CSV e sobe para o staging (padrão: só dry-run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explícito; é o comportamento padrão.",
    )
    args = parser.parse_args()

    print(f"Lendo {STAGING_TABLE} ...")
    atual = load_staging_dictionary()
    novas, atualizado = add_rows(atual)
    alteradas, atualizado = replace_rows(atualizado)
    summarize(novas, alteradas, atualizado)

    if novas.empty and alteradas.empty:
        print("\nNada a fazer (dicionário já atualizado).")
        return

    if not args.apply:
        print(
            "\nDry-run. Para aplicar: "
            "uv run python update_dicionario.py --apply"
        )
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    atualizado.to_csv(OUTPUT_CSV, index=False)
    print(f"\nCSV salvo em {OUTPUT_CSV}")
    upload_dictionary(OUTPUT_CSV)
    print(
        "\nPróximo passo: cd ../../.. && "
        "uv run dbt run --select br_ms_cnes__dicionario"
    )


if __name__ == "__main__":
    main()
