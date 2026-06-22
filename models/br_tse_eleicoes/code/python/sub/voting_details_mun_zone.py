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
        df = read_raw_csv(
            str(base),
            drop_first_row=True,
            family="detalhe_votacao_munzona",
            ano=ano,
        )

        # The detalhe_votacao_munzona layout is identical across all years
        # (1994-2024). The old positional map for 1996/2000-2016 read the wrong
        # columns (e.g. aptos_totalizadas <- QT_SECOES_NAO_INSTALADAS), which
        # caused the 1996 coverage collapse. Header-based selection by official
        # name fixes it. Two branches differ only in how votos_validos/votos_nulos
        # are derived; both preserve the previously-OK behavior of their years
        # (≤2016 = the 1994/1998 method; ≥2018 = direct TSE totals).
        if ano <= 2016:
            col_map = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "CD_MUNICIPIO": "id_municipio_tse",
                "NR_ZONA": "zona",
                "DS_CARGO": "cargo",
                "QT_APTOS": "aptos",
                "QT_SECOES_PRINCIPAIS": "secoes",
                "QT_SECOES_AGREGADAS": "secoes_agregadas",
                "QT_COMPARECIMENTO": "comparecimento",
                "QT_ABSTENCOES": "abstencoes",
                "QT_VOTOS_NOMINAIS_VALIDOS": "votos_nominais",
                "QT_TOTAL_VOTOS_LEG_VALIDOS": "votos_legenda",
                "QT_VOTOS_BRANCOS": "votos_brancos",
            }
            # votos_nulos = nulos + nulos técnicos + anulados apurados em separado
            nulos = sum(
                pd.to_numeric(df[c], errors="coerce")
                for c in (
                    "QT_VOTOS_NULOS",
                    "QT_VOTOS_NULOS_TECNICOS",
                    "QT_VOTOS_ANULADOS_APU_SEP",
                )
            )
            df = df[list(col_map.keys())].rename(columns=col_map)
            df["votos_nulos"] = nulos
            df["votos_nominais"] = pd.to_numeric(
                df["votos_nominais"], errors="coerce"
            )
            df["votos_legenda"] = pd.to_numeric(
                df["votos_legenda"], errors="coerce"
            )
            df["votos_validos"] = df["votos_nominais"] + df["votos_legenda"]
            df["aptos_totalizadas"] = np.nan
            df["secoes_totalizadas"] = np.nan

        else:  # >= 2018 — direct TSE total columns
            col_map = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "CD_MUNICIPIO": "id_municipio_tse",
                "NR_ZONA": "zona",
                "DS_CARGO": "cargo",
                "QT_APTOS": "aptos",
                "QT_SECOES_PRINCIPAIS": "secoes",
                "QT_SECOES_AGREGADAS": "secoes_agregadas",
                "QT_COMPARECIMENTO": "comparecimento",
                "QT_ABSTENCOES": "abstencoes",
                "QT_TOTAL_VOTOS_VALIDOS": "votos_validos",
                "QT_VOTOS_NOMINAIS_VALIDOS": "votos_nominais",
                "QT_VOTOS_BRANCOS": "votos_brancos",
                "QT_TOTAL_VOTOS_NULOS": "votos_nulos",
                "QT_TOTAL_VOTOS_LEG_VALIDOS": "votos_legenda",
            }
            df = df[list(col_map.keys())].rename(columns=col_map)
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
