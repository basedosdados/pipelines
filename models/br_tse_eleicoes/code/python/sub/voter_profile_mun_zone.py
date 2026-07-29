"""
Build: perfil_eleitorado_municipio_zona (voter profile by municipality-zone).
Equivalent of sub/perfil_eleitorado_municipio_zona.do.
Single national file per year (no per-state loop).
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON, YEARS_EVEN
from utils.helpers import merge_municipio, read_raw_csv, select_named


def build_perfil_mun_zona(ano: int) -> pd.DataFrame:
    """Build voter profile mun-zone for a single year."""
    base = (
        INPUT_DIR
        / f"perfil_eleitorado/perfil_eleitorado_{ano}/perfil_eleitorado_{ano}"
    )
    df = read_raw_csv(str(base), drop_first_row=True)

    if df.attrs.get("tse_has_header"):
        # Read-by-header-name path. Dual keys cover the naming churn across
        # TSE generations (ANO_ELEICAO/AA_ELEICAO, QT_ELEITORES_PERFIL/
        # QT_ELEITORES, QT_ELEITORES_INC_NM_SOCIAL/QT_ELEITORES_NOME_SOCIAL,
        # and the TP_OBRIGATORIEDADE_VOTO insertion that shifted the 2024
        # positional block). Demographic columns keep the CD_* codes, as the
        # Stata pipeline stored codes, not labels.
        keep_cols = {
            "ano_eleicao": "ano",
            "aa_eleicao": "ano",
            "sg_uf": "sigla_uf",
            "cd_municipio": "id_municipio_tse",
            "cd_mun_sit_biometrica": "situacao_biometria",
            "nr_zona": "zona",
            "cd_genero": "genero",
            "cd_estado_civil": "estado_civil",
            "cd_faixa_etaria": "grupo_idade",
            "cd_grau_escolaridade": "instrucao",
            "qt_eleitores_perfil": "eleitores",
            "qt_eleitores": "eleitores",
            "qt_eleitores_biometria": "eleitores_biometria",
            "qt_eleitores_deficiencia": "eleitores_deficiencia",
            "qt_eleitores_inc_nm_social": "eleitores_inclusao_nome_social",
            "qt_eleitores_nome_social": "eleitores_inclusao_nome_social",
        }
        df = select_named(df, keep_cols)
        if "situacao_biometria" not in df.columns:
            df["situacao_biometria"] = ""
    elif ano <= 2022:
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
