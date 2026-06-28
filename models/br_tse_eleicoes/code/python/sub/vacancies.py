"""
Build: vagas (vacancies per position).
Equivalent of sub/vagas.do.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br, read_raw_csv

# fmt: off
UFS = {
    1994: ["AC", "AL", "AM", "AP", "BA", "GO", "MA", "MS", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    1996: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "PA", "PB", "PE", "PI", "RN", "RR", "RS", "SE", "SP", "TO"],
    1998: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2000: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2002: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2004: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2006: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2008: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2010: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2012: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2014: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2016: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2018: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2020: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2022: ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2024: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
}
# fmt: on


def build_vagas(ano: int) -> pd.DataFrame:
    """Build vagas for a single year."""
    frames = []

    for uf in UFS[ano]:
        base = (
            INPUT_DIR
            / f"consulta_vagas/consulta_vagas_{ano}/consulta_vagas_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=(ano >= 2014))

        if ano <= 2012:
            df = df[["v3", "v4", "v5", "v6", "v9", "v10"]].copy()
            df.columns = [
                "ano",
                "tipo_eleicao",
                "sigla_uf",
                "id_municipio_tse",
                "cargo",
                "vagas",
            ]
            df["id_eleicao"] = ""
            df["data_eleicao"] = ""
        else:  # >= 2014
            df = df[
                ["v3", "v6", "v7", "v8", "v10", "v11", "v14", "v15"]
            ].copy()
            df.columns = [
                "ano",
                "id_eleicao",
                "tipo_eleicao",
                "data_eleicao",
                "sigla_uf",
                "id_municipio_tse",
                "cargo",
                "vagas",
            ]

        # destring
        for col in ["ano", "id_municipio_tse", "vagas"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # clean strings
        for col in ["tipo_eleicao", "cargo"]:
            if col in df.columns:
                df[col] = clean_string_series(df[col])

        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )

        # parse dates
        if "data_eleicao" in df.columns:
            df["data_eleicao"] = parse_date_br(df["data_eleicao"])

        # drop odd years
        df = df[df["ano"] % 2 == 0]

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        df = df.drop_duplicates()
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # final column order
    col_order = [
        "ano",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "cargo",
        "vagas",
    ]
    result = result[[c for c in col_order if c in result.columns]]

    return result


def build_all():
    """Build vagas for all years and save."""
    for ano in sorted(UFS.keys()):
        print(f"  vagas {ano}")
        df = build_vagas(ano)
        out = OUTPUT_PYTHON / f"vagas_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
