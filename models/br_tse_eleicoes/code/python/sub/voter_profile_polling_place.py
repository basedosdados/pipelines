"""
Build: perfil_eleitorado_local_votacao (voter profile by polling place).
Equivalent of sub/perfil_eleitorado_local_votacao.do.
Single national file per year, 2010-2024.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.helpers import merge_municipio, read_raw_csv

YEARS = list(range(2010, 2025, 2))


def build_perfil_local_votacao(ano: int) -> pd.DataFrame:
    """Build voter profile polling place for a single year."""
    base = (
        INPUT_DIR
        / f"perfil_eleitorado_local_votacao/eleitorado_local_votacao_{ano}/eleitorado_local_votacao_{ano}"
    )
    df = read_raw_csv(str(base), drop_first_row=True)

    df = df[
        [
            "v3",
            "v6",
            "v7",
            "v8",
            "v10",
            "v11",
            "v12",
            "v15",
            "v16",
            "v17",
            "v19",
            "v20",
            "v21",
            "v22",
            "v23",
            "v24",
            "v25",
            "v27",
            "v29",
            "v31",
            "v33",
            "v35",
        ]
    ].copy()
    df.columns = [
        "ano",
        "turno",
        "sigla_uf",
        "id_municipio_tse",
        "zona",
        "secao",
        "tipo_secao_agregada",
        "numero",
        "nome",
        "tipo",
        "endereco",
        "bairro",
        "cep",
        "telefone",
        "latitude",
        "longitude",
        "situacao",
        "situacao_zona",
        "situacao_secao",
        "situacao_localidade",
        "situacao_secao_acessibilidade",
        "eleitores_secao",
    ]

    # turno fix
    df["turno"] = df["turno"].replace({"1º Turno": "1", "2º Turno": "2"})

    # clean specific fields
    for col in ["telefone", "latitude", "longitude"]:
        df.loc[df[col] == "-1", col] = ""

    # destring
    for col in [
        "ano",
        "turno",
        "id_municipio_tse",
        "latitude",
        "longitude",
        "eleitores_secao",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # merge municipio
    df["id_municipio_tse"] = (
        df["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    df = merge_municipio(df)

    col_order = [
        "ano",
        "turno",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "zona",
        "secao",
        "tipo_secao_agregada",
        "numero",
        "nome",
        "tipo",
        "endereco",
        "bairro",
        "cep",
        "telefone",
        "latitude",
        "longitude",
        "situacao",
        "situacao_zona",
        "situacao_secao",
        "situacao_localidade",
        "situacao_secao_acessibilidade",
        "eleitores_secao",
    ]
    return df[[c for c in col_order if c in df.columns]]


def build_all():
    """Build for all years and save."""
    frames = []
    for ano in YEARS:
        print(f"  perfil_eleitorado_local_votacao {ano}")
        df = build_perfil_local_votacao(ano)
        frames.append(df)

    # Stata appends all years into one .dta
    result = pd.concat(frames, ignore_index=True)
    out = OUTPUT_PYTHON / "perfil_eleitorado_local_votacao.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
