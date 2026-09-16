"""
Manual candidate corrections.
Equivalent of fnc/limpa_candidato.do.
"""

import pandas as pd


def fix_candidate(df: pd.DataFrame) -> pd.DataFrame:
    """Apply manual corrections to specific candidate rows."""
    # Paulo Peixer — fix CPF and titulo_eleitoral (year 2000, municipio 4202909)
    if "id_municipio" not in df.columns:
        return df
    mask = (
        (df["ano"] == "2000")
        & (df["id_municipio"] == "4202909")
        & (df["nome"] == "Paulo Peixer")
    )
    if "cpf" in df.columns:
        df.loc[mask, "cpf"] = "30964261987"
    if "titulo_eleitoral" in df.columns:
        df.loc[mask, "titulo_eleitoral"] = "001954260914"

    # José Carlos Selbach Eymael — fix titulo_eleitoral (year 2006, SP)
    mask2 = (
        (df["ano"] == "2006")
        & (df["sigla_uf"] == "SP")
        & (df["nome"] == "José Carlos Selbach Eymael")
    )
    if "titulo_eleitoral" in df.columns:
        df.loc[mask2, "titulo_eleitoral"] = "086136320116"

    return df
