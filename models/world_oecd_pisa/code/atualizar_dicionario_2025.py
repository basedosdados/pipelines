"""
Atualização do dicionário de `world_oecd_pisa.student` para o ciclo 2025.

Duas tarefas, a partir da arquitetura (`extra/student.csv`) e do dicionário
já publicado na BD (`extra/dictionary.csv`, sem 2025):

1. Colunas que só existem a partir de 2025 (`covered_by_dictionary=yes` e
   `original_name_2025` preenchido, sem nome original em nenhum ano anterior)
   não têm nenhuma linha no dicionário ainda — extrai a chave-valor do .sav
   2025 e cria essas linhas.

2. Colunas que já existiam antes de 2025 podem ter mudado de codificação no
   novo ciclo. Compara a chave-valor do .sav 2025 com o que já está
   registrado (a linha "(1)" é o padrão que vale para todos os anos; uma
   coluna já pode ter overrides pontuais tipo "2018" para um ano específico
   que divergiu — ver `models/br_ibge_pnadc` para o mesmo padrão). Nunca
   sobrescreve o histórico: toda divergência vira uma linha nova com
   temporal_coverage="2025", igual à convenção já usada para os anos
   anteriores.

Fonte da chave-valor: só existe nos metadados do .sav (`variable_value_labels`
do pyreadstat) — a staging em BigQuery guarda os códigos, não os rótulos.
Requer `pip install pyreadstat` e o arquivo CY09_MS_STU_PUF.sav local ou
acessível (ex.: Google Drive montado, como no Colab onde os anos anteriores
foram processados).

Uso:
    python atualizar_dicionario_2025.py

Gera, em extra/:
    dictionary_2025.csv   — dicionário antigo + linhas novas (task 1 e 2)
    relatorio_2025.csv    — 1 linha por coluna comparada, com o veredito
    diffs_2025.csv        — 1 linha por chave cujo rótulo mudou (revisão humana)

Co-Authored-By: Claude Sonnet 5
"""

import json
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]  # models/world_oecd_pisa
ARQUITETURA = BASE / "extra" / "student.csv"
DICIONARIO_ANTIGO = BASE / "extra" / "dictionary.csv"

# Ajuste para o caminho real do .sav de 2025 antes de rodar — ou, se só o
# JSON de rótulos exportado do Colab estiver disponível, deixe ROTULOS_JSON
# apontando para ele e ignore SAV_2025.
SAV_2025 = BASE / "input" / "2025" / "CY09_MS_STU_PUF.sav"
ROTULOS_JSON = BASE / "extra" / "rotulos_2025.json"

SAIDA_DICIONARIO = BASE / "extra" / "dictionary_2025.csv"
SAIDA_RELATORIO = BASE / "extra" / "relatorio_2025.csv"
SAIDA_DIFFS = BASE / "extra" / "diffs_2025.csv"

TABLE_ID = "student"
ANO_NOVO = "2025"

ANOS_CICLO = (2025, 2022, 2018, 2015, 2012, 2009, 2006, 2003, 2000)
YEAR_COLS = [f"original_name_{ano}" for ano in ANOS_CICLO]
COL_2025, *OUTROS_ANOS_COLS = YEAR_COLS

COLUNAS_DICIONARIO = [
    "table_id",
    "column_name",
    "key",
    "temporal_coverage",
    "value",
]


def carregar_arquitetura(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def carregar_dicionario_antigo(path: Path) -> pd.DataFrame:
    dic = pd.read_csv(path, dtype=str, keep_default_na=False)
    return dic.loc[:, ~dic.columns.str.startswith("Unnamed")]


def carregar_rotulos_sav(sav_path: Path) -> dict:
    """Lê só os metadados do .sav — rápido mesmo em arquivos grandes, não
    carrega as linhas de dado. Devolve {nome_original: {chave: rotulo}}."""
    # pyrefly: ignore [missing-import]
    import pyreadstat

    _, meta = pyreadstat.read_sav(str(sav_path), metadataonly=True)
    return meta.variable_value_labels


def carregar_rotulos_json(json_path: Path) -> dict:
    """Alternativa a carregar_rotulos_sav quando o .sav não está acessível
    localmente: lê um dump de meta.variable_value_labels (só as colunas que
    interessam) exportado do Colab. Ver README/histórico do PR para o
    trecho que gera esse JSON a partir do pyreadstat."""
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)


def normaliza_chave(k) -> str:
    """1.0, '1.0' (string, vindo de JSON), '0001', 1 -> '1' — mesma
    convenção do dicionário atual (sem zero à esquerda; ver amostras de
    `gender`, `oecd`, `isced_level`, `country_birth_self`)."""
    try:
        f = float(k)
        if f.is_integer():
            k = int(f)
    except (TypeError, ValueError):
        pass
    s = str(k).strip()
    return (s.lstrip("0") or "0") if s.lstrip("-").isdigit() else s


def normaliza_valor(v) -> str:
    """Compara só o conteúdo textual do rótulo — ignora espaço duplicado e caixa."""
    return " ".join(str(v).split()).casefold()


def colunas_novas_2025(arq: pd.DataFrame) -> pd.DataFrame:
    """Task 1: colunas com dicionário que só existem a partir de 2025."""
    mask = (
        (arq["covered_by_dictionary"] == "yes")
        & (arq[COL_2025] != "")
        & arq[OUTROS_ANOS_COLS].apply(
            lambda r: all(v == "" for v in r), axis=1
        )
    )
    return arq.loc[mask, ["name", "temporal_coverage", COL_2025]]


def colunas_compartilhadas(arq: pd.DataFrame) -> pd.DataFrame:
    """Task 2: colunas com dicionário que já existiam antes de 2025."""
    mask = (
        (arq["covered_by_dictionary"] == "yes")
        & (arq[COL_2025] != "")
        & arq[OUTROS_ANOS_COLS].apply(
            lambda r: any(v != "" for v in r), axis=1
        )
    )
    return arq.loc[mask, ["name", "temporal_coverage", COL_2025]]


def linhas_para_colunas_novas(
    novas: pd.DataFrame, rotulos: dict
) -> pd.DataFrame:
    """Task 1 — gera as linhas de dicionário para colunas exclusivas de 2025.
    temporal_coverage vem direto da arquitetura (ex.: "2025(1)")."""
    linhas, sem_rotulo = [], []
    for nome, cobertura, original in novas.itertuples(index=False):
        rot = rotulos.get(original)
        if not rot:
            sem_rotulo.append((nome, original))
            continue
        for chave, valor in rot.items():
            linhas.append(
                {
                    "table_id": TABLE_ID,
                    "column_name": nome,
                    "key": normaliza_chave(chave),
                    "temporal_coverage": cobertura,
                    "value": str(valor).strip(),
                }
            )
    if sem_rotulo:
        print(
            f"AVISO: {len(sem_rotulo)} colunas novas de 2025 sem value label no .sav:"
        )
        for nome, original in sem_rotulo:
            print(f"    {nome} ({original})")
    return pd.DataFrame(linhas, columns=COLUNAS_DICIONARIO)


def comparar_colunas_compartilhadas(
    compartilhadas: pd.DataFrame, rotulos: dict, dic_antigo: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Task 2 — compara a chave-valor de 2025 com o histórico, coluna a coluna.

    Devolve (relatorio, diffs, linhas_2025):
    - relatorio: 1 linha por coluna comparada, com o veredito.
    - diffs: 1 linha por chave cujo rótulo mudou, para revisão humana.
    - linhas_2025: linhas a inserir no dicionário (sempre temporal_coverage
      ="2025" — o histórico nunca é sobrescrito).
    """
    relatorio_rows, diffs_rows, linhas_2025 = [], [], []

    for nome, _cobertura_arq, original in compartilhadas.itertuples(
        index=False
    ):
        bloco_antigo = dic_antigo[dic_antigo["column_name"] == nome]
        rot = rotulos.get(original)

        if bloco_antigo.empty:
            # Coluna passou a ter dicionário agora, mas nunca teve linha
            # registrada (ex.: form_id/BOOKID, effort_*). Só temos o .sav de
            # 2025 aqui — os códigos são adicionados com cobertura 2025 e
            # ficam sinalizados para confirmar contra os .sav de anos
            # anteriores antes de estender a cobertura.
            status = (
                "AUSENTE_NO_DICIONARIO_ANTIGO"
                if rot
                else "AUSENTE_NO_DICIONARIO_E_SEM_VALUE_LABEL"
            )
            relatorio_rows.append(
                {
                    "column_name": nome,
                    "original_name_2025": original,
                    "status": status,
                    "mudaram": None,
                    "so_2025": len(rot) if rot else None,
                    "so_anteriores": None,
                }
            )
            if rot:
                for chave, valor in rot.items():
                    linhas_2025.append(
                        {
                            "table_id": TABLE_ID,
                            "column_name": nome,
                            "key": normaliza_chave(chave),
                            "temporal_coverage": ANO_NOVO,
                            "value": str(valor).strip(),
                        }
                    )
            continue

        if not rot:
            relatorio_rows.append(
                {
                    "column_name": nome,
                    "original_name_2025": original,
                    "status": "SEM_VALUE_LABEL_NO_SAV",
                    "mudaram": None,
                    "so_2025": None,
                    "so_anteriores": None,
                }
            )
            continue

        # Referência histórica: a linha "(1)" é o rótulo padrão (vale para
        # todos os anos salvo overrides pontuais tipo "2018"). Quando não há
        # linha "(1)" — coluna só tem overrides ano-a-ano — usa o bloco
        # inteiro; nesse caso, chaves repetidas em anos diferentes ficam
        # com o último valor lido (raro; ver aviso abaixo).
        referencia = bloco_antigo[bloco_antigo["temporal_coverage"] == "(1)"]
        so_overrides = referencia.empty
        if so_overrides:
            referencia = bloco_antigo

        atuais_2025 = {
            normaliza_chave(k): str(v).strip() for k, v in rot.items()
        }
        historico = dict(
            zip(
                referencia["key"].map(normaliza_chave),
                referencia["value"],
                strict=True,
            )
        )

        mudaram = {
            k
            for k in atuais_2025.keys() & historico.keys()
            if normaliza_valor(atuais_2025[k]) != normaliza_valor(historico[k])
        }
        so_2025 = atuais_2025.keys() - historico.keys()
        so_anteriores = historico.keys() - atuais_2025.keys()

        status = "IGUAL" if not (mudaram or so_2025) else "DIVERGENTE"
        if so_overrides:
            status += "_SO_OVERRIDES_SEM_LINHA_(1)"
        relatorio_rows.append(
            {
                "column_name": nome,
                "original_name_2025": original,
                "status": status,
                "mudaram": len(mudaram),
                "so_2025": len(so_2025),
                "so_anteriores": len(so_anteriores),
            }
        )

        for k in sorted(mudaram):
            diffs_rows.append(
                {
                    "column_name": nome,
                    "key": k,
                    "valor_historico": historico[k],
                    "valor_2025": atuais_2025[k],
                }
            )
        for k in sorted(mudaram | so_2025):
            linhas_2025.append(
                {
                    "table_id": TABLE_ID,
                    "column_name": nome,
                    "key": k,
                    "temporal_coverage": ANO_NOVO,
                    "value": atuais_2025[k],
                }
            )

    relatorio = pd.DataFrame(relatorio_rows)
    diffs = pd.DataFrame(
        diffs_rows,
        columns=["column_name", "key", "valor_historico", "valor_2025"],
    )
    linhas_2025_df = pd.DataFrame(linhas_2025, columns=COLUNAS_DICIONARIO)
    return relatorio, diffs, linhas_2025_df


def main() -> None:
    print("Lendo arquitetura e dicionário antigo...")
    arq = carregar_arquitetura(ARQUITETURA)
    dic_antigo = carregar_dicionario_antigo(DICIONARIO_ANTIGO)

    if ROTULOS_JSON.exists():
        print(f"Lendo rótulos de {ROTULOS_JSON}...")
        rotulos = carregar_rotulos_json(ROTULOS_JSON)
    else:
        print(f"Lendo metadados do .sav ({SAV_2025})...")
        rotulos = carregar_rotulos_sav(SAV_2025)

    novas = colunas_novas_2025(arq)
    compartilhadas = colunas_compartilhadas(arq)
    print(
        f"{len(novas)} colunas novas em 2025, "
        f"{len(compartilhadas)} colunas compartilhadas com dicionário"
    )

    linhas_novas = linhas_para_colunas_novas(novas, rotulos)
    relatorio, diffs, linhas_2025 = comparar_colunas_compartilhadas(
        compartilhadas, rotulos, dic_antigo
    )

    # dedupe pelas 5 colunas (linha idêntica, ex.: rodar o script 2x) — NAO
    # pelas 4 primeiras. O dicionario antigo tem ~100 linhas onde a mesma
    # (column_name, key, temporal_coverage) tem 2 valores legitimamente
    # diferentes (ex.: country_birth_mother, chave 939200 == "Another
    # country (JPN)" para uns paises e "Other country in Asia..." para
    # outros — o PISA reaproveita codigo numerico por pais nessas colunas
    # "national categories"). Dedupe em 4 colunas apagaria uma das duas
    # linhas por acidente.
    final = (
        pd.concat([dic_antigo, linhas_novas, linhas_2025], ignore_index=True)
        .drop_duplicates(subset=COLUNAS_DICIONARIO)
        .sort_values(["column_name", "temporal_coverage", "key"])
        .reset_index(drop=True)
    )

    final.to_csv(SAIDA_DICIONARIO, index=False)
    relatorio.sort_values("status").to_csv(SAIDA_RELATORIO, index=False)
    diffs.to_csv(SAIDA_DIFFS, index=False)

    print(
        f"\n{len(dic_antigo)} -> {len(final)} linhas no dicionário ({SAIDA_DICIONARIO})"
    )
    print(f"\nRelatório por coluna ({SAIDA_RELATORIO}):")
    print(relatorio["status"].value_counts().to_string())
    if not diffs.empty:
        print(
            f"\n{len(diffs)} chaves com rótulo divergente — ver {SAIDA_DIFFS}"
        )


if __name__ == "__main__":
    main()
