"""
Build: perfil_eleitorado_secao (voter profile by section).
Equivalent of sub/perfil_eleitorado_secao.do.
Per-state files, 2008-2024. Uses named columns (has header row).
"""

from pathlib import Path

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


def perfil_secao_path(ano: int, uf: str) -> "Path":
    """Resolve the raw perfil_secao file path for one (ano, uf)."""
    parent = INPUT_DIR / "perfil_eleitorado_secao"
    sub = parent / f"perfil_eleitor_secao_{ano}_{uf}"
    patterns = [
        parent / f"perfil_eleitor_secao_{ano}_{uf}.txt",
        parent / f"perfil_eleitor_secao_{ano}_{uf}.csv",
        sub / f"perfil_eleitor_secao_{ano}_{uf}.txt",
        sub / f"perfil_eleitor_secao_{ano}_{uf}.csv",
    ]
    for p in patterns:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"No file found for perfil_eleitorado_secao {ano} {uf}"
    )


def _read_perfil_secao(ano: int, uf: str) -> pd.DataFrame:
    """Read a single voter profile section file (has named headers)."""
    df = pd.read_csv(
        perfil_secao_path(ano, uf),
        sep=";",
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
    )
    df.columns = df.columns.str.lower().str.strip('"')
    return df


PERFIL_GROUP_COLS = [
    "ano", "sigla_uf", "id_municipio_tse", "situacao_biometria", "zona",
    "secao", "genero", "estado_civil", "grupo_idade", "instrucao",
]  # fmt: skip
PERFIL_SUM_COLS = [
    "eleitores", "eleitores_biometria", "eleitores_deficiencia",
    "eleitores_inclusao_nome_social",
]  # fmt: skip
PERFIL_COL_ORDER = [
    "ano", "sigla_uf", "id_municipio", "id_municipio_tse", "zona", "secao",
    "genero", "estado_civil", "grupo_idade", "instrucao",
    "situacao_biometria", "eleitores", "eleitores_biometria",
    "eleitores_deficiencia", "eleitores_inclusao_nome_social",
]  # fmt: skip


def clean_perfil_frame(df: pd.DataFrame, ano: int, uf: str) -> pd.DataFrame:
    """Row-wise clean of one perfil_secao frame (whole file or a chunk).

    Everything up to (but not including) the group-by sum — chunk-safe.
    Shared by ``build_perfil_secao`` and the streaming build.
    """
    if (
        uf == "DF"
        and ano == 2014
        and "cd_mun_sit_biometrica" not in df.columns
    ):
        df["cd_mun_sit_biometrica"] = ""

    if ano <= 2022:
        keep_cols = {
            "ano_eleicao": "ano",
            "aa_eleicao": "ano",
            "sg_uf": "sigla_uf",
            "cd_municipio": "id_municipio_tse",
            "cd_mun_sit_biometrica": "situacao_biometria",
            "cd_mun_sit_biometria": "situacao_biometria",
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
            "aa_eleicao": "ano",
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

    # first present key wins per target (matches select_named semantics)
    available: dict[str, str] = {}
    taken: set[str] = set()
    for k, v in keep_cols.items():
        if k in df.columns and v not in taken:
            available[k] = v
            taken.add(v)
    df = df[list(available.keys())].rename(columns=available)

    if ano == 2024:
        df["situacao_biometria"] = ""

    for col in PERFIL_SUM_COLS:
        if col in df.columns:
            df.loc[df[col] == "-1", col] = ""

    for col in ["id_municipio_tse", "zona", "secao", *PERFIL_SUM_COLS]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _perfil_groupby(df: pd.DataFrame) -> pd.DataFrame:
    """Group-by sum for perfil_secao. Associative → chunk-safe when the
    per-chunk results are concatenated and re-grouped."""
    g = [c for c in PERFIL_GROUP_COLS if c in df.columns]
    s = [c for c in PERFIL_SUM_COLS if c in df.columns]
    return df.groupby(g, as_index=False, dropna=False)[s].sum()


def finalize_perfil_uf(agg: pd.DataFrame) -> pd.DataFrame:
    """Attach id_municipio to a UF's aggregated perfil rows."""
    agg = agg.copy()
    agg["id_municipio_tse"] = (
        agg["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    return merge_municipio(agg)


def build_perfil_secao(ano: int) -> pd.DataFrame:
    """Build voter profile section for a single year, in RAM.

    Giant years must use the streaming build in ``sub/streaming_secao.py``.
    """
    frames = []
    for uf in UFS[ano]:
        df = clean_perfil_frame(_read_perfil_secao(ano, uf), ano, uf)
        frames.append(finalize_perfil_uf(_perfil_groupby(df)))

    result = pd.concat(frames, ignore_index=True)
    return result[[c for c in PERFIL_COL_ORDER if c in result.columns]]


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
