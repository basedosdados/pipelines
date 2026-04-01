"""
Build: detalhes_votacao_municipio_zona (voting details by municipality-zone).
Equivalent of sub/detalhes_votacao_municipio_zona.do.
"""

import numpy as np
import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br, read_raw_csv

# fmt: off
UFS = {
    1994: ["AC", "AL", "AM", "AP", "BA", "BRASIL", "GO", "MA", "MS", "PI", "RR", "RS", "SC", "SE", "SP", "TO"],
    1996: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "PA", "PB", "PI", "RN", "RR", "SE", "SP", "TO"],
    1998: ["AC", "AL", "AM", "AP", "BA", "BRASIL", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2000: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
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


def build_detalhes_mun_zona(ano: int) -> pd.DataFrame:
    """Build voting details mun-zone for a single year."""
    frames = []

    for uf in UFS[ano]:
        base = (
            INPUT_DIR
            / f"detalhe_votacao_munzona/detalhe_votacao_munzona_{ano}/detalhe_votacao_munzona_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        if ano in (1994, 1998):
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
                    "v18",
                    "v19",
                    "v20",
                    "v21",
                    "v24",
                    "v26",
                    "v31",
                    "v32",
                    "v41",
                    "v43",
                    "v44",
                    "v45",
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
                "cargo",
                "aptos",
                "secoes",
                "secoes_agregadas",
                "comparecimento",
                "abstencoes",
                "votos_nominais",
                "votos_legenda",
                "votos_brancos",
                "v43",
                "v44",
                "v45",
            ]
            for col in [
                "votos_nominais",
                "votos_legenda",
                "v43",
                "v44",
                "v45",
            ]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["votos_validos"] = df["votos_nominais"] + df["votos_legenda"]
            df["votos_nulos"] = df["v43"] + df["v44"] + df["v45"]
            df = df.drop(columns=["v43", "v44", "v45"])
            df["aptos_totalizadas"] = np.nan
            df["secoes_totalizadas"] = np.nan

        elif ano == 1996 or 2000 <= ano <= 2016:
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
                    "v18",
                    "v19",
                    "v20",
                    "v21",
                    "v22",
                    "v23",
                    "v24",
                    "v25",
                    "v27",
                    "v28",
                    "v29",
                    "v30",
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
                "cargo",
                "aptos",
                "secoes",
                "secoes_agregadas",
                "aptos_totalizadas",
                "secoes_totalizadas",
                "comparecimento",
                "abstencoes",
                "votos_nominais",
                "votos_brancos",
                "votos_nulos",
                "votos_legenda",
            ]
            for col in ["votos_nominais", "votos_legenda"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["votos_validos"] = df["votos_nominais"] + df["votos_legenda"]

        else:  # >= 2018
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
                    "v18",
                    "v19",
                    "v20",
                    "v21",
                    "v24",
                    "v26",
                    "v30",
                    "v31",
                    "v41",
                    "v42",
                    "v32",
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
                "cargo",
                "aptos",
                "secoes",
                "secoes_agregadas",
                "comparecimento",
                "abstencoes",
                "votos_validos",
                "votos_nominais",
                "votos_brancos",
                "votos_nulos",
                "votos_legenda",
            ]
            df["aptos_totalizadas"] = np.nan
            df["secoes_totalizadas"] = np.nan

        # destring all numeric
        num_cols = [
            "ano",
            "turno",
            "id_municipio_tse",
            "zona",
            "aptos",
            "secoes",
            "secoes_agregadas",
            "aptos_totalizadas",
            "secoes_totalizadas",
            "comparecimento",
            "abstencoes",
            "votos_validos",
            "votos_nominais",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
        ]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # replace -1
        for col in [
            "zona",
            "aptos",
            "secoes",
            "secoes_agregadas",
            "aptos_totalizadas",
            "secoes_totalizadas",
            "comparecimento",
            "abstencoes",
            "votos_validos",
            "votos_nominais",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
        ]:
            if col in df.columns:
                df.loc[df[col] == -1, col] = pd.NA

        # BRASIL filter
        if uf == "BRASIL":
            df = df[(df["cargo"] == "PRESIDENTE") | (df["sigla_uf"] == "DF")]

        # clean strings
        for col in ["tipo_eleicao", "cargo"]:
            df[col] = clean_string_series(df[col])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )

        # collapse (sum) to handle TSE duplicates
        group_cols = [
            "ano",
            "turno",
            "id_eleicao",
            "tipo_eleicao",
            "data_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "zona",
            "cargo",
        ]
        sum_cols = [
            c
            for c in [
                "aptos",
                "secoes",
                "secoes_agregadas",
                "aptos_totalizadas",
                "secoes_totalizadas",
                "comparecimento",
                "abstencoes",
                "votos_validos",
                "votos_nominais",
                "votos_brancos",
                "votos_nulos",
                "votos_legenda",
            ]
            if c in df.columns
        ]
        df = df.groupby(group_cols, as_index=False, dropna=False)[
            sum_cols
        ].sum()

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # dates
        df["data_eleicao"] = parse_date_br(df["data_eleicao"])

        # proportions
        df["proporcao_comparecimento"] = (
            100 * df["comparecimento"] / df["aptos"]
        )
        df["proporcao_votos_validos"] = (
            100 * df["votos_validos"] / df["comparecimento"]
        )
        df["proporcao_votos_brancos"] = (
            100 * df["votos_brancos"] / df["comparecimento"]
        )
        df["proporcao_votos_nulos"] = (
            100 * df["votos_nulos"] / df["comparecimento"]
        )

        # drop odd years
        df = df[df["ano"] % 2 == 0]

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    result = result.drop_duplicates()

    # tag remaining dups (Stata keeps them but tags; we keep them too)
    col_order = [
        "ano",
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "zona",
        "cargo",
        "aptos",
        "secoes",
        "secoes_agregadas",
        "aptos_totalizadas",
        "secoes_totalizadas",
        "comparecimento",
        "abstencoes",
        "votos_validos",
        "votos_brancos",
        "votos_nulos",
        "votos_nominais",
        "votos_legenda",
        "proporcao_comparecimento",
        "proporcao_votos_validos",
        "proporcao_votos_brancos",
        "proporcao_votos_nulos",
    ]
    return result[[c for c in col_order if c in result.columns]]


def build_all():
    for ano in sorted(UFS.keys()):
        print(f"  detalhes_votacao_municipio_zona {ano}")
        df = build_detalhes_mun_zona(ano)
        out = OUTPUT_PYTHON / f"detalhes_votacao_municipio_zona_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
