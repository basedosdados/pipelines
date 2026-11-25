"""Corrige `sigla_iso2` no staging de `br_bd_diretorios_mundo.pais`.

Duas correções no CSV de staging:

- Namíbia: o código ISO 3166-1 alpha-2 é a string "NA", que o `pd.read_csv`
  padrão lê como ausente. O CSV de staging foi salvo com o campo vazio, e a
  tabela publicada ficou com `sigla_iso2` nulo.
- Kosovo: sem código ISO 3166-1 oficial. Recebe "XK", código de uso
  reservado adotado pela Comissão Europeia, pelo Banco Mundial e pela Meta.

Uso:
    uv run python models/br_bd_diretorios_mundo/code/fix_pais_sigla_iso2.py <in.csv> <out.csv>
"""

import sys

import pandas as pd

# (coluna de busca, valor, sigla_iso2 correta)
FIXES = [
    ("sigla_iso3", "NAM", "NA"),
    ("sigla_cow", "KOS", "XK"),
]


def fix(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for key_col, key_val, iso2 in FIXES:
        mask = df[key_col] == key_val
        if mask.sum() != 1:
            raise ValueError(
                f"{key_col}={key_val}: {mask.sum()} linhas, esperava 1"
            )
        df.loc[mask, "sigla_iso2"] = iso2
    iso2 = df["sigla_iso2"].replace("", pd.NA).dropna()
    if iso2.duplicated().any():
        raise ValueError(
            f"sigla_iso2 duplicada: {iso2[iso2.duplicated()].tolist()}"
        )
    return df


if __name__ == "__main__":
    src, dst = sys.argv[1], sys.argv[2]
    # keep_default_na=False: "NA" é a sigla da Namíbia, não um valor ausente
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    fix(df).to_csv(dst, index=False, lineterminator="\r\n")
