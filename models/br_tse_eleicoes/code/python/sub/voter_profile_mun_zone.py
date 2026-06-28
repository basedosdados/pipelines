"""
Build: perfil_eleitorado_municipio_zona (voter profile by municipality-zone).
Equivalent of sub/perfil_eleitorado_municipio_zona.do.
Single national file per year (no per-state loop).
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON, YEARS_EVEN
from utils.helpers import merge_municipio, read_raw_csv


def build_perfil_mun_zona(ano: int) -> pd.DataFrame:
    """Build voter profile mun-zone for a single year."""
    base = (
        INPUT_DIR
        / f"perfil_eleitorado/perfil_eleitorado_{ano}/perfil_eleitorado_{ano}"
    )
    df = read_raw_csv(str(base), drop_first_row=True)

    if ano <= 2022:
        df = df[
            [
                "v3",
                "v4",
                "v5",
                "v7",
                "v9",
                "v10",
                "v12",
                "v14",
                "v16",
                "v18",
                "v19",
                "v20",
                "v21",
            ]
        ].copy()
        df.columns = [
            "ano",
            "sigla_uf",
            "id_municipio_tse",
            "situacao_biometria",
            "zona",
            "genero",
            "estado_civil",
            "grupo_idade",
            "instrucao",
            "eleitores",
            "eleitores_biometria",
            "eleitores_deficiencia",
            "eleitores_inclusao_nome_social",
        ]
    elif ano == 2024:
        df = df[
            [
                "v3",
                "v4",
                "v5",
                "v7",
                "v8",
                "v10",
                "v12",
                "v14",
                "v24",
                "v25",
                "v26",
                "v27",
            ]
        ].copy()
        df.columns = [
            "ano",
            "sigla_uf",
            "id_municipio_tse",
            "zona",
            "genero",
            "estado_civil",
            "grupo_idade",
            "instrucao",
            "eleitores",
            "eleitores_biometria",
            "eleitores_deficiencia",
            "eleitores_inclusao_nome_social",
        ]
        df["situacao_biometria"] = ""

    # destring
    for col in ["ano", "id_municipio_tse", "zona"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in [
        "eleitores",
        "eleitores_biometria",
        "eleitores_deficiencia",
        "eleitores_inclusao_nome_social",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # replace -1 with NaN
    for col in [
        "zona",
        "eleitores",
        "eleitores_biometria",
        "eleitores_deficiencia",
        "eleitores_inclusao_nome_social",
    ]:
        df.loc[df[col] == -1, col] = pd.NA

    # merge municipio
    df["id_municipio_tse"] = (
        df["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    df = merge_municipio(df)

    # fix 2014 ano
    df.loc[df["ano"] == 201407, "ano"] = 2014

    # final column order
    col_order = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "situacao_biometria",
        "zona",
        "genero",
        "estado_civil",
        "grupo_idade",
        "instrucao",
        "eleitores",
        "eleitores_biometria",
        "eleitores_deficiencia",
        "eleitores_inclusao_nome_social",
    ]
    return df[[c for c in col_order if c in df.columns]]


def build_all():
    """Build for all years and save."""
    for ano in YEARS_EVEN:
        print(f"  perfil_eleitorado_municipio_zona {ano}")
        df = build_perfil_mun_zona(ano)
        out = OUTPUT_PYTHON / f"perfil_eleitorado_municipio_zona_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
