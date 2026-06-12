"""
Build: perfil_eleitorado_secao (voter profile by section).
Equivalent of sub/perfil_eleitorado_secao.do.
Per-state files, 2008-2024. Uses named columns (has header row).
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.helpers import merge_municipio

# fmt: off
UFS = {
    2008: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2010: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2012: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2014: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2016: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2018: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "ZZ"],
    2020: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2022: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "ZZ"],
    2024: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}
# fmt: on


def _read_perfil_secao(ano: int, uf: str) -> pd.DataFrame:
    """Read a single voter profile section file (has named headers)."""
    parent = INPUT_DIR / "perfil_eleitorado_secao"
    sub = parent / f"perfil_eleitor_secao_{ano}_{uf}"
    patterns = [
        parent / f"perfil_eleitor_secao_{ano}_{uf}.txt",
        parent / f"perfil_eleitor_secao_{ano}_{uf}.csv",
        sub / f"perfil_eleitor_secao_{ano}_{uf}.txt",
        sub / f"perfil_eleitor_secao_{ano}_{uf}.csv",
    ]

    path = None
    for p in patterns:
        if p.exists():
            path = p
            break
    if path is None:
        raise FileNotFoundError(
            f"No file found for perfil_eleitorado_secao {ano} {uf}"
        )

    df = pd.read_csv(
        path, sep=";", dtype=str, encoding="latin-1", keep_default_na=False
    )
    df.columns = df.columns.str.lower().str.strip('"')
    return df


def build_perfil_secao(ano: int) -> pd.DataFrame:
    """Build voter profile section for a single year."""
    frames = []

    for uf in UFS[ano]:
        df = _read_perfil_secao(ano, uf)

        # DF 2014 special case: situacao_biometrica column may be missing
        if (
            uf == "DF"
            and ano == 2014
            and "cd_mun_sit_biometrica" not in df.columns
        ):
            df["cd_mun_sit_biometrica"] = ""

        if ano <= 2022:
            keep_cols = {
                "ano_eleicao": "ano",
                "sg_uf": "sigla_uf",
                "cd_municipio": "id_municipio_tse",
                "cd_mun_sit_biometrica": "situacao_biometria",
                "nr_zona": "zona",
                "nr_secao": "secao",
                "cd_genero": "genero",
                "cd_estado_civil": "estado_civil",
                "cd_faixa_etaria": "grupo_idade",
                "cd_grau_escolaridade": "instrucao",
                "qt_eleitores_perfil": "eleitores",
                "qt_eleitores_biometria": "eleitores_biometria",
                "qt_eleitores_deficiencia": "eleitores_deficiencia",
                "qt_eleitores_inc_nm_social": "eleitores_inclusao_nome_social",
            }
        else:  # 2024
            keep_cols = {
                "ano_eleicao": "ano",
                "sg_uf": "sigla_uf",
                "cd_municipio": "id_municipio_tse",
                "nr_zona": "zona",
                "nr_secao": "secao",
                "cd_genero": "genero",
                "cd_estado_civil": "estado_civil",
                "cd_faixa_etaria": "grupo_idade",
                "cd_grau_escolaridade": "instrucao",
                "qt_eleitores_perfil": "eleitores",
                "qt_eleitores_biometria": "eleitores_biometria",
                "qt_eleitores_deficiencia": "eleitores_deficiencia",
                "qt_eleitores_inc_nm_social": "eleitores_inclusao_nome_social",
            }

        # Keep and rename available columns
        available = {k: v for k, v in keep_cols.items() if k in df.columns}
        df = df[list(available.keys())].rename(columns=available)

        if ano == 2024:
            df["situacao_biometria"] = ""

        # clean -1 in eleitores columns
        for col in [
            "eleitores",
            "eleitores_biometria",
            "eleitores_deficiencia",
            "eleitores_inclusao_nome_social",
        ]:
            if col in df.columns:
                df.loc[df[col] == "-1", col] = ""

        # destring
        for col in [
            "id_municipio_tse",
            "zona",
            "secao",
            "eleitores",
            "eleitores_biometria",
            "eleitores_deficiencia",
            "eleitores_inclusao_nome_social",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # collapse (sum) by group
        group_cols = [
            "ano",
            "sigla_uf",
            "id_municipio_tse",
            "situacao_biometria",
            "zona",
            "secao",
            "genero",
            "estado_civil",
            "grupo_idade",
            "instrucao",
        ]
        sum_cols = [
            "eleitores",
            "eleitores_biometria",
            "eleitores_deficiencia",
            "eleitores_inclusao_nome_social",
        ]
        group_cols_available = [c for c in group_cols if c in df.columns]
        sum_cols_available = [c for c in sum_cols if c in df.columns]

        df = df.groupby(group_cols_available, as_index=False, dropna=False)[
            sum_cols_available
        ].sum()

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # Enforce column order to match Stata output
    col_order = [
        "ano",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "zona",
        "secao",
        "genero",
        "estado_civil",
        "grupo_idade",
        "instrucao",
        "situacao_biometria",
        "eleitores",
        "eleitores_biometria",
        "eleitores_deficiencia",
        "eleitores_inclusao_nome_social",
    ]
    result = result[[c for c in col_order if c in result.columns]]

    return result


def build_all():
    """Build for all years and save."""
    for ano in sorted(UFS.keys()):
        print(f"  perfil_eleitorado_secao {ano}")
        df = build_perfil_secao(ano)
        out = OUTPUT_PYTHON / f"perfil_eleitorado_secao_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
