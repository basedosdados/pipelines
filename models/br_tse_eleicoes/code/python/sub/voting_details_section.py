"""
Build: detalhes_votacao_secao (voting details by section).
Equivalent of sub/detalhes_votacao_secao.do.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br, read_raw_csv

# fmt: off
UFS = {
    1994: ["AC", "AL", "AM", "AP", "BA", "BRASIL", "GO", "MA", "MS", "PI", "RS", "SC", "SE", "SP", "TO"],
    1996: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "PA", "PB", "PE", "PI", "RN", "RR", "RS", "SE", "SP", "TO"],
    1998: ["AC", "AL", "AM", "AP", "BA", "BRASIL", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
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


def build_detalhes_secao(ano: int) -> pd.DataFrame:
    """Build voting details by section for a single year."""
    frames = []

    for uf in UFS[ano]:
        base = (
            INPUT_DIR
            / f"detalhe_votacao_secao/detalhe_votacao_secao_{ano}/detalhe_votacao_secao_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

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
                "v21",
                "v22",
                "v23",
                "v24",
                "v25",
                "v26",
                "v27",
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
            "aptos",
            "comparecimento",
            "abstencoes",
            "votos_nominais",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
            "votos_nulos_apu_sep",
        ]

        # destring
        num_cols = [
            "ano",
            "turno",
            "id_municipio_tse",
            "zona",
            "secao",
            "aptos",
            "comparecimento",
            "abstencoes",
            "votos_nominais",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
            "votos_nulos_apu_sep",
        ]
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # replace -1 with NaN
        for col in [
            "zona",
            "secao",
            "aptos",
            "comparecimento",
            "abstencoes",
            "votos_nominais",
            "votos_brancos",
            "votos_nulos",
            "votos_legenda",
            "votos_nulos_apu_sep",
        ]:
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

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # parse dates
        df["data_eleicao"] = parse_date_br(df["data_eleicao"])

        # proportions
        df["proporcao_comparecimento"] = (
            100 * df["comparecimento"] / df["aptos"]
        )
        df["proporcao_votos_nominais"] = (
            100 * df["votos_nominais"] / df["comparecimento"]
        )
        df["proporcao_votos_legenda"] = (
            100 * df["votos_legenda"] / df["comparecimento"]
        )
        df["proporcao_votos_brancos"] = (
            100 * df["votos_brancos"] / df["comparecimento"]
        )
        df["proporcao_votos_nulos"] = (
            100 * df["votos_nulos"] / df["comparecimento"]
        )

        # drop odd years
        df = df[df["ano"] % 2 == 0]

        # dedup
        dup_keys = [
            "ano",
            "turno",
            "tipo_eleicao",
            "sigla_uf",
            "id_municipio_tse",
            "zona",
            "secao",
            "cargo",
        ]
        df = df.drop_duplicates(
            subset=dup_keys, keep=False
        )  # drop ALL duplicates (dup > 0)

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

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
        "secao",
        "cargo",
        "aptos",
        "comparecimento",
        "abstencoes",
        "votos_nominais",
        "votos_brancos",
        "votos_nulos",
        "votos_legenda",
        "votos_nulos_apu_sep",
        "proporcao_comparecimento",
        "proporcao_votos_nominais",
        "proporcao_votos_legenda",
        "proporcao_votos_brancos",
        "proporcao_votos_nulos",
    ]
    return result[[c for c in col_order if c in result.columns]]


def build_all():
    for ano in sorted(UFS.keys()):
        print(f"  detalhes_votacao_secao {ano}")
        df = build_detalhes_secao(ano)
        out = OUTPUT_PYTHON / f"detalhes_votacao_secao_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
