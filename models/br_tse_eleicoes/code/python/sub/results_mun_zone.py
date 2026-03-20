"""
Build: resultados_candidato_municipio_zona + resultados_partido_municipio_zona.
Equivalent of sub/resultados_municipio_zona.do.
Produces TWO output tables per year.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_party import clean_party_series
from utils.clean_result import clean_result_series
from utils.clean_string import clean_string_series
from utils.helpers import (
    clean_nulls,
    merge_municipio,
    parse_date_br,
    read_raw_csv,
)

# fmt: off
# State lists differ between candidato and partido but are identical in practice
_ALL_UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
_MUN = ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]

UFS_CAND = {
    1994: ["AC", "AL", "AM", "AP", "BA", "BR", "GO", "MA", "MS", "PB", "PE", "PI", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    1996: ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "PA", "PB", "PE", "PI", "RN", "RO", "RR", "RS", "SE", "SP", "TO"],
    1998: ["AC", "AL", "AM", "AP", "BA", "BRASIL", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2000: _MUN,
    2002: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2004: _MUN,
    2006: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2008: _MUN,
    2010: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2012: _MUN,
    2014: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2016: _MUN,
    2018: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2020: _MUN,
    2022: ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"],
    2024: _MUN,
}
UFS_PART = dict(UFS_CAND)  # same lists
# fmt: on


def _build_candidato(ano: int) -> pd.DataFrame:
    """Build candidate results at mun-zone level."""
    frames = []
    for uf in UFS_CAND[ano]:
        base = (
            INPUT_DIR
            / f"votacao_candidato_munzona/votacao_candidato_munzona_{ano}/votacao_candidato_munzona_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        if ano == 1994:
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v19": "sequencial_candidato",
                "v20": "numero_candidato",
                "v21": "nome_candidato",
                "v22": "nome_urna_candidato",
                "v29": "numero_partido",
                "v30": "sigla_partido",
                "v40": "votos",
                "v44": "resultado",
            }
        elif ano == 1996 or (2002 <= ano <= 2016):
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v19": "sequencial_candidato",
                "v20": "numero_candidato",
                "v21": "nome_candidato",
                "v22": "nome_urna_candidato",
                "v29": "numero_partido",
                "v30": "sigla_partido",
                "v36": "resultado",
                "v38": "votos",
            }
        elif ano in (1998, 2000) or (2018 <= ano <= 2022):
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v19": "sequencial_candidato",
                "v20": "numero_candidato",
                "v21": "nome_candidato",
                "v22": "nome_urna_candidato",
                "v29": "numero_partido",
                "v30": "sigla_partido",
                "v42": "votos",
                "v44": "resultado",
            }
        else:  # >= 2024
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v19": "sequencial_candidato",
                "v20": "numero_candidato",
                "v21": "nome_candidato",
                "v22": "nome_urna_candidato",
                "v35": "numero_partido",
                "v36": "sigla_partido",
                "v46": "votos",
                "v50": "resultado",
            }

        available = {k: v for k, v in cols.items() if k in df.columns}
        df = df[list(available.keys())].rename(columns=available)

        # destring
        for col in ["ano", "turno", "id_municipio_tse", "votos"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.loc[df["sequencial_candidato"] == "-1", "sequencial_candidato"] = ""
        df.loc[df["resultado"] == "2O. TURNO", "resultado"] = "2º turno"

        # clean nulls
        df = clean_nulls(df)

        # clean strings
        for col in ["tipo_eleicao", "cargo", "resultado"]:
            if col in df.columns:
                df[col] = clean_string_series(df[col])
        for col in ["nome_candidato", "nome_urna_candidato"]:
            if col in df.columns:
                df[col] = df[col].str.title()

        df["data_eleicao"] = parse_date_br(df["data_eleicao"])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla_partido"] = clean_party_series(df["sigla_partido"], ano)
        if "resultado" in df.columns:
            df["resultado"] = clean_result_series(df["resultado"])

        # Special year/uf filters for president data issues
        if ano == 2002 and uf != "BR":
            df = df[~((df["turno"] == 2) & (df["cargo"] == "presidente"))]
        if ano in (2006, 2010) and uf != "BR":
            df = df[df["cargo"] != "presidente"]
        if ano == 1998:
            if uf == "BRASIL":
                df = df[df["cargo"] == "presidente"]
            else:
                df = df[df["cargo"] != "presidente"]

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # collapse (sum) votos
    group_cols = [c for c in result.columns if c != "votos"]
    result = result.groupby(group_cols, as_index=False, dropna=False)[
        "votos"
    ].sum()

    # merge municipio
    result["id_municipio_tse"] = (
        result["id_municipio_tse"]
        .astype("Int64")
        .astype(str)
        .replace("<NA>", "")
    )
    result = merge_municipio(result)

    return result


def _build_partido(ano: int) -> pd.DataFrame:
    """Build party results at mun-zone level."""
    frames = []
    for uf in UFS_PART[ano]:
        base = (
            INPUT_DIR
            / f"votacao_partido_munzona/votacao_partido_munzona_{ano}/votacao_partido_munzona_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        if ano in (1994, 1998):
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v20": "numero_partido",
                "v21": "sigla_partido",
                "v33": "votos_legenda",
                "v34": "votos_nominais",
            }
        elif ano == 1996 or (2000 <= ano <= 2016):
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v20": "numero_partido",
                "v21": "sigla_partido",
                "v27": "votos_nominais",
                "v28": "votos_legenda",
            }
        elif 2018 <= ano <= 2020:
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v20": "numero_partido",
                "v21": "sigla_partido",
                "v33": "votos_legenda",
                "v34": "votos_nominais",
            }
        else:  # >= 2022
            cols = {
                "v3": "ano",
                "v6": "turno",
                "v7": "id_eleicao",
                "v8": "tipo_eleicao",
                "v9": "data_eleicao",
                "v11": "sigla_uf",
                "v14": "id_municipio_tse",
                "v16": "zona",
                "v18": "cargo",
                "v20": "numero_partido",
                "v21": "sigla_partido",
                "v33": "votos_legenda",
                "v34": "votos_nominais",
            }

        available = {k: v for k, v in cols.items() if k in df.columns}
        df = df[list(available.keys())].rename(columns=available)

        for col in [
            "ano",
            "turno",
            "id_municipio_tse",
            "votos_nominais",
            "votos_legenda",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = clean_nulls(df)
        for col in ["tipo_eleicao", "cargo"]:
            df[col] = clean_string_series(df[col])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla_partido"] = clean_party_series(df["sigla_partido"], ano)
        df["data_eleicao"] = parse_date_br(df["data_eleicao"])

        # collapse
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
            "numero_partido",
            "sigla_partido",
        ]
        sum_cols = ["votos_nominais", "votos_legenda"]
        available_group = [c for c in group_cols if c in df.columns]
        available_sum = [c for c in sum_cols if c in df.columns]
        df = df.groupby(available_group, as_index=False, dropna=False)[
            available_sum
        ].sum()

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # president data issue fixes
        if ano == 2006 and uf != "BR":
            df = df[df["cargo"] != "presidente"]
        if ano == 1998 and uf != "BRASIL":
            df = df[df["cargo"] == "presidente"]

        df = df.drop_duplicates()
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def build_all():
    for ano in sorted(UFS_CAND.keys()):
        print(f"  resultados_municipio_zona {ano}")
        cand = _build_candidato(ano)
        out = (
            OUTPUT_PYTHON
            / f"resultados_candidato_municipio_zona_{ano}.parquet"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        cand.to_parquet(out, index=False)

        part = _build_partido(ano)
        out = (
            OUTPUT_PYTHON / f"resultados_partido_municipio_zona_{ano}.parquet"
        )
        part.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
