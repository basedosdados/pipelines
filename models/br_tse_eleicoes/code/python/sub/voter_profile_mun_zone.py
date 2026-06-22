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
    df = read_raw_csv(
        str(base),
        drop_first_row=True,
        family="perfil_eleitorado",
        ano=ano,
    )

    # Header-based selection by official TSE name. Two layout generations: the
    # n=21 years carry the CD/DS_MUN_SIT_BIOMETRICA block (and no raça/identidade
    # blocks); the n=28 years drop biometria and insert raça/identidade/quilombola
    # /libras/obrigatoriedade. The demographic columns use the CD_ (code) variant
    # to match the previously-OK years and the dominant production generation
    # (genero ∈ 0/2/4); the old positional map drifted on the n=28 years.
    if ano in (1994, 1996, 1998, 2000, 2002, 2004, 2006, 2018):
        col_map = {
            "ANO_ELEICAO": "ano",
            "SG_UF": "sigla_uf",
            "CD_MUNICIPIO": "id_municipio_tse",
            "CD_MUN_SIT_BIOMETRICA": "situacao_biometria",
            "NR_ZONA": "zona",
            "CD_GENERO": "genero",
            "CD_ESTADO_CIVIL": "estado_civil",
            "CD_FAIXA_ETARIA": "grupo_idade",
            "CD_GRAU_ESCOLARIDADE": "instrucao",
            "QT_ELEITORES_PERFIL": "eleitores",
            "QT_ELEITORES_BIOMETRIA": "eleitores_biometria",
            "QT_ELEITORES_DEFICIENCIA": "eleitores_deficiencia",
            "QT_ELEITORES_INC_NM_SOCIAL": "eleitores_inclusao_nome_social",
        }
        df = df[list(col_map.keys())].rename(columns=col_map)
    else:  # n=28 years — no biometria block
        col_map = {
            "ANO_ELEICAO": "ano",
            "SG_UF": "sigla_uf",
            "CD_MUNICIPIO": "id_municipio_tse",
            "NR_ZONA": "zona",
            "CD_GENERO": "genero",
            "CD_ESTADO_CIVIL": "estado_civil",
            "CD_FAIXA_ETARIA": "grupo_idade",
            "CD_GRAU_ESCOLARIDADE": "instrucao",
            "QT_ELEITORES_PERFIL": "eleitores",
            "QT_ELEITORES_BIOMETRIA": "eleitores_biometria",
            "QT_ELEITORES_DEFICIENCIA": "eleitores_deficiencia",
            "QT_ELEITORES_INC_NM_SOCIAL": "eleitores_inclusao_nome_social",
        }
        df = df[list(col_map.keys())].rename(columns=col_map)
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
