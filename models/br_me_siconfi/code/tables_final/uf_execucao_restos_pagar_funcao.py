import pandas as pd

from .shared import ORDEM_UF_SIMPLES, apply_conta_split, partition_and_save

LEVEL = "uf"
ANEXO = "DCA-Anexo I-G"


def build(path_dados, path_queries, comp, year_data, ano):
    df = year_data.get(LEVEL, {}).get(ANEXO)
    if df is None or df.empty:
        print(f"  uf_execucao_restos_pagar_funcao {ano}: no data, skipping")
        return pd.DataFrame()

    df = apply_conta_split(df)
    df["portaria"] = df["portaria"].str.lstrip("0")
    df["ano"] = str(ano)
    df["estagio_bd"] = ""
    df["id_conta_bd"] = ""
    df["conta_bd"] = ""

    df = df[ORDEM_UF_SIMPLES]
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").astype("float")

    print(f"  uf_execucao_restos_pagar_funcao {ano}: {len(df):,} rows")
    partition_and_save(df, "uf_execucao_restos_pagar_funcao", ano, path_dados)
    return pd.DataFrame()
