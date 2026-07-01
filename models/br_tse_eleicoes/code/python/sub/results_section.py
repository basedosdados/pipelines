"""
Build: resultados_candidato_secao + resultados_partido_secao.
Equivalent of sub/resultados_secao.do.
Produces TWO output tables per year.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br, read_raw_csv

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


def build_resultados_secao(ano: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build section-level results. Returns (candidato_df, partido_df)."""
    cand_frames = []
    part_frames = []

    # Load UF directory for BR files
    from config import MUNICIPIO_DIR_CSV

    mun_uf = pd.read_csv(MUNICIPIO_DIR_CSV, encoding="utf-8", dtype=str)[
        ["id_municipio_tse", "sigla_uf"]
    ]

    for uf in UFS[ano]:
        base = (
            INPUT_DIR
            / f"votacao_secao/votacao_secao_{ano}_{uf}/votacao_secao_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        # Special old schema for BR 1998 and TO 2008
        if (ano == 1998 and uf == "BR") or (ano == 2008 and uf == "TO"):
            raw_base = (
                INPUT_DIR
                / f"votacao_secao/votacao_secao_{ano}_{uf}/votacao_secao_{ano}_{uf}"
            )
            # Re-read without dropping first row (old schema has no header)
            df = read_raw_csv(str(raw_base), drop_first_row=False)
            df = df[
                [
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v8",
                    "v10",
                    "v11",
                    "v13",
                    "v14",
                    "v15",
                ]
            ].copy()
            df.columns = [
                "ano",
                "turno",
                "tipo_eleicao",
                "sigla_uf",
                "id_municipio_tse",
                "zona",
                "secao",
                "cargo",
                "numero_votavel",
                "votos",
            ]
            df["id_eleicao"] = ""
            df["data_eleicao"] = ""
        else:
            df = df[
                [
                    "v3",
                    "v6",
                    "v7",
                    "v8",
                    "v9",
                    "v11",
                    "v14",
                    "v16",
                    "v17",
                    "v19",
                    "v20",
                    "v22",
                ]
            ].copy()
            df.columns = [
                "ano",
                "turno",
                "id_eleicao",
                "tipo_eleicao",
                "data_eleicao",
                "sigla_uf",
                "id_municipio_tse",
                "zona",
                "secao",
                "cargo",
                "numero_votavel",
                "votos",
            ]

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
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # --- Split into candidato vs partido ---

        # Remove special codes 95, 96, 97 (brancos, nulos, etc.)
        df_valid = df[~df["numero_votavel"].isin(["95", "96", "97"])].copy()

        # CANDIDATO: drop 2-digit legend votes for proportional cargos
        cand = df_valid[
            ~(
                (df_valid["numero_votavel"].str.len() == 2)
                & (df_valid["cargo"].isin(_PROP_CARGOS))
            )
        ].copy()
        cand = cand.rename(columns={"numero_votavel": "numero_candidato"})
        cand_frames.append(cand)

        # PARTIDO: compute votos_nominais + votos_legenda
        # votos_nominais: sum of votes for candidates (numero_votavel > 2 digits) by partido
        nominais = df_valid[
            ~(
                (df_valid["numero_votavel"].str.len() == 2)
                & (df_valid["cargo"].isin(_PROP_CARGOS))
            )
        ].copy()
        nominais["numero_partido"] = nominais["numero_votavel"].str[:2]
        group_cols = [
            "ano",
            "id_eleicao",
            "tipo_eleicao",
            "data_eleicao",
            "turno",
            "sigla_uf",
            "id_municipio",
            "id_municipio_tse",
            "zona",
            "secao",
            "cargo",
            "numero_partido",
        ]
        available_group = [c for c in group_cols if c in nominais.columns]
        nom_agg = nominais.groupby(
            available_group, as_index=False, dropna=False
        )["votos"].sum()
        nom_agg = nom_agg.rename(columns={"votos": "votos_nominais"})

        # votos_legenda: 2-digit votes for proportional cargos
        legenda = df_valid[
            (df_valid["numero_votavel"].str.len() == 2)
            & (df_valid["cargo"].isin(_PROP_CARGOS))
        ].copy()
        legenda = legenda.rename(
            columns={
                "numero_votavel": "numero_partido",
                "votos": "votos_legenda",
            }
        )
        leg_cols = [c for c in group_cols if c in legenda.columns] + [
            "votos_legenda"
        ]
        legenda = legenda[leg_cols]

        # merge nominais + legenda — use all shared group columns as keys
        merge_keys = [c for c in available_group if c in legenda.columns]
        partido = nom_agg.merge(legenda, on=merge_keys, how="outer")
        partido["votos_nominais"] = (
            partido["votos_nominais"].fillna(0).astype(int)
        )
        partido["votos_legenda"] = (
            partido["votos_legenda"].fillna(0).astype(int)
        )
        part_frames.append(partido)

    # Assemble candidato
    cand_result = pd.concat(cand_frames, ignore_index=True)
    dup_keys = [
        "ano",
        "turno",
        "tipo_eleicao",
        "id_municipio_tse",
        "zona",
        "secao",
        "cargo",
        "numero_candidato",
    ]
    cand_result = cand_result.drop_duplicates(subset=dup_keys, keep="first")

    # Assemble partido
    part_result = pd.concat(part_frames, ignore_index=True)

    # Enforce column order to match Stata output
    cand_cols = [
        "ano",
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "zona",
        "secao",
        "cargo",
        "numero_candidato",
        "votos",
    ]
    part_cols = [
        "ano",
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "zona",
        "secao",
        "cargo",
        "numero_partido",
        "votos_nominais",
        "votos_legenda",
    ]
    cand_result = cand_result[
        [c for c in cand_cols if c in cand_result.columns]
    ]
    part_result = part_result[
        [c for c in part_cols if c in part_result.columns]
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
