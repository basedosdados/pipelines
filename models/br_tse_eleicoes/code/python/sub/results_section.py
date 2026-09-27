"""
Build: resultados_candidato_secao + resultados_partido_secao.
Equivalent of sub/resultados_secao.do.
Produces TWO output tables per year.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import (
    merge_municipio,
    parse_date_br,
    read_raw_csv,
    select_named,
)

# fmt: off
UFS = {
    1994: ["AC", "AL", "AM", "AP", "BA", "BR", "GO", "MA", "PI", "RO", "RS", "SC", "SE", "SP", "TO"],
    1996: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "PA", "PB", "PE", "PI", "RN", "RR", "RS", "SE", "SP", "TO"],
    1998: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2000: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP"],
    2002: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2004: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2006: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2008: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2010: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2012: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2014: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2016: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2018: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2020: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2022: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2024: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}
# fmt: on

# Proportional cargos where 2-digit numero_votavel = party legend vote
_PROP_CARGOS = {
    "vereador",
    "deputado estadual",
    "deputado distrital",
    "deputado federal",
    "senador",
}


# Shared column contracts (used by the in-RAM build and the streaming build)
_GROUP_COLS = [
    "ano", "id_eleicao", "tipo_eleicao", "data_eleicao", "turno",
    "sigla_uf", "id_municipio", "id_municipio_tse", "zona", "secao",
    "cargo", "numero_partido",
]  # fmt: skip
DUP_KEYS = [
    "ano", "turno", "tipo_eleicao", "id_municipio_tse", "zona", "secao",
    "cargo", "numero_candidato",
]  # fmt: skip
CAND_COLS = [
    "ano", "turno", "id_eleicao", "tipo_eleicao", "data_eleicao",
    "sigla_uf", "id_municipio", "id_municipio_tse", "zona", "secao",
    "cargo", "numero_candidato", "votos",
]  # fmt: skip
PART_COLS = [
    "ano", "turno", "id_eleicao", "tipo_eleicao", "data_eleicao",
    "sigla_uf", "id_municipio", "id_municipio_tse", "zona", "secao",
    "cargo", "numero_partido", "votos_nominais", "votos_legenda",
]  # fmt: skip


def _load_mun_uf() -> pd.DataFrame:
    from config import MUNICIPIO_DIR_CSV

    return pd.read_csv(MUNICIPIO_DIR_CSV, encoding="utf-8", dtype=str)[
        ["id_municipio_tse", "sigla_uf"]
    ]


def clean_secao_frame(
    df: pd.DataFrame, ano: int, uf: str, mun_uf: pd.DataFrame
) -> pd.DataFrame:
    """Row-wise clean of one raw seção frame (whole file or a chunk).

    Chunk-safe: every step is per-row or a join against a small directory,
    so applying it to a chunk and concatenating equals applying it to the
    whole file. Shared by ``build_resultados_secao`` and the streaming
    build so the transform lives in exactly one place.
    """
    # --- schema selection ---
    if (ano == 1998 and uf == "BR") or (ano == 2008 and uf == "TO"):
        df = df[
            ["v3", "v4", "v5", "v6", "v8", "v10", "v11", "v13", "v14", "v15"]
        ].copy()
        df.columns = [
            "ano", "turno", "tipo_eleicao", "sigla_uf", "id_municipio_tse",
            "zona", "secao", "cargo", "numero_votavel", "votos",
        ]  # fmt: skip
        df["id_eleicao"] = ""
        df["data_eleicao"] = ""
    elif df.attrs.get("tse_has_header"):
        df = select_named(
            df,
            {
                "ano_eleicao": "ano",
                "aa_eleicao": "ano",
                "nr_turno": "turno",
                "cd_eleicao": "id_eleicao",
                "ds_eleicao": "tipo_eleicao",
                "dt_eleicao": "data_eleicao",
                "sg_uf": "sigla_uf",
                "cd_municipio": "id_municipio_tse",
                "nr_zona": "zona",
                "nr_secao": "secao",
                "ds_cargo": "cargo",
                "nr_votavel": "numero_votavel",
                "qt_votos": "votos",
            },
        )
    else:
        df = df[
            ["v3", "v6", "v7", "v8", "v9", "v11", "v14", "v16", "v17",
             "v19", "v20", "v22"]
        ].copy()  # fmt: skip
        df.columns = [
            "ano", "turno", "id_eleicao", "tipo_eleicao", "data_eleicao",
            "sigla_uf", "id_municipio_tse", "zona", "secao", "cargo",
            "numero_votavel", "votos",
        ]  # fmt: skip

    # destring
    for col in ["ano", "turno", "id_municipio_tse", "votos"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # For BR files in 1994/1998, replace sigla_uf using directory
    if ano in (1994, 1998) and uf == "BR":
        df = df.drop(columns=["sigla_uf"])
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = df.merge(mun_uf, on="id_municipio_tse", how="left")
        df.loc[df["sigla_uf"].isna(), "sigla_uf"] = "ZZ"

    # clean strings
    for col in ["tipo_eleicao", "cargo"]:
        df[col] = clean_string_series(df[col])
    df["data_eleicao"] = parse_date_br(df["data_eleicao"])
    df["tipo_eleicao"] = clean_election_type_series(df["tipo_eleicao"], ano)

    # merge municipio
    df["id_municipio_tse"] = (
        df["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    return merge_municipio(df)


def split_secao_frame(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split a cleaned frame into (cand, nominais, legenda) — pre-aggregation.

    ``nominais`` still carries per-row votes (aggregate downstream);
    ``legenda`` is the 2-digit legend-vote rows. Chunk-safe.
    """
    df_valid = df[~df["numero_votavel"].isin(["95", "96", "97"])].copy()
    is_2digit_prop = (df_valid["numero_votavel"].str.len() == 2) & (
        df_valid["cargo"].isin(_PROP_CARGOS)
    )
    cand = (
        df_valid[~is_2digit_prop]
        .copy()
        .rename(columns={"numero_votavel": "numero_candidato"})
    )
    nominais = df_valid[~is_2digit_prop].copy()
    nominais["numero_partido"] = nominais["numero_votavel"].str[:2]
    legenda = (
        df_valid[is_2digit_prop]
        .copy()
        .rename(
            columns={
                "numero_votavel": "numero_partido",
                "votos": "votos_legenda",
            }
        )
    )
    return cand, nominais, legenda


def finalize_partido(
    nominais: pd.DataFrame, legenda: pd.DataFrame
) -> pd.DataFrame:
    """Aggregate one UF's nominais + legenda into the partido rows.

    Equivalent whether ``nominais``/``legenda`` are a single UF read whole
    or the concatenation of that UF's chunks (groupby-sum is associative;
    legenda is only concatenated). Shared by both build paths.
    """
    available_group = [c for c in _GROUP_COLS if c in nominais.columns]
    nom_agg = (
        nominais.groupby(available_group, as_index=False, dropna=False)[
            "votos"
        ]
        .sum()
        .rename(columns={"votos": "votos_nominais"})
    )
    leg_cols = [c for c in _GROUP_COLS if c in legenda.columns] + [
        "votos_legenda"
    ]
    legenda = legenda[leg_cols]
    merge_keys = [c for c in available_group if c in legenda.columns]
    partido = nom_agg.merge(legenda, on=merge_keys, how="outer")
    partido["votos_nominais"] = partido["votos_nominais"].fillna(0).astype(int)
    partido["votos_legenda"] = partido["votos_legenda"].fillna(0).astype(int)
    return partido


def build_resultados_secao(ano: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build section-level results in RAM. Returns (candidato_df, partido_df).

    Fine for small/medium years. Giant years (60M+ rows) must use the
    streaming build in ``sub/streaming_secao.py`` on a 16 GB host.
    """
    cand_frames = []
    part_frames = []
    mun_uf = _load_mun_uf()

    for uf in UFS[ano]:
        base = (
            INPUT_DIR
            / f"votacao_secao/votacao_secao_{ano}_{uf}/votacao_secao_{ano}_{uf}"
        )
        df = clean_secao_frame(read_raw_csv(str(base)), ano, uf, mun_uf)
        cand, nominais, legenda = split_secao_frame(df)
        cand_frames.append(cand)
        part_frames.append(finalize_partido(nominais, legenda))

    cand_result = pd.concat(cand_frames, ignore_index=True).drop_duplicates(
        subset=DUP_KEYS, keep="first"
    )
    part_result = pd.concat(part_frames, ignore_index=True)

    cand_result = cand_result[
        [c for c in CAND_COLS if c in cand_result.columns]
    ]
    part_result = part_result[
        [c for c in PART_COLS if c in part_result.columns]
    ]
    return cand_result, part_result


def build_all():
    for ano in sorted(UFS.keys()):
        print(f"  resultados_secao {ano}")
        cand, part = build_resultados_secao(ano)

        out = OUTPUT_PYTHON / f"resultados_candidato_secao_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        cand.to_parquet(out, index=False)

        out = OUTPUT_PYTHON / f"resultados_partido_secao_{ano}.parquet"
        part.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
