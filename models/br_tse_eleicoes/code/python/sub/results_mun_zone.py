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
        df = read_raw_csv(
            str(base),
            drop_first_row=True,
            family="votacao_candidato_munzona",
            ano=ano,
        )

        # Header-based selection by official TSE name. The official names are
        # stable across all years (1994-2024); only their positions drifted,
        # which is exactly what broke the old vN map (e.g. 1994 votos read
        # SG_FEDERACAO). NOTE: 1998/2000 previously read QT_VOTOS_NOMINAIS_VALIDOS
        # (a positional artifact); they now read QT_VOTOS_NOMINAIS like every
        # other year, so the votos column is consistent across the series.
        cols = {
            "ANO_ELEICAO": "ano",
            "NR_TURNO": "turno",
            "CD_ELEICAO": "id_eleicao",
            "DS_ELEICAO": "tipo_eleicao",
            "DT_ELEICAO": "data_eleicao",
            "SG_UF": "sigla_uf",
            "CD_MUNICIPIO": "id_municipio_tse",
            "NR_ZONA": "zona",
            "DS_CARGO": "cargo",
            "SQ_CANDIDATO": "sequencial_candidato",
            "NR_CANDIDATO": "numero_candidato",
            "NM_CANDIDATO": "nome_candidato",
            "NM_URNA_CANDIDATO": "nome_urna_candidato",
            "NR_PARTIDO": "numero_partido",
            "SG_PARTIDO": "sigla_partido",
            "QT_VOTOS_NOMINAIS": "votos",
            "DS_SIT_TOT_TURNO": "resultado",
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
                df[col] = (
                    df[col]
                    .str.title()
                    .str.replace(
                        r"(?<=[A-Za-zÀ-ÿ])\u00b4([A-Z])",
                        lambda m: "\u00b4" + m.group(1).lower(),
                        regex=True,
                    )
                )

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

    # For years 1996-2016, the Stata reference .dta files were generated before
    # `order id_municipio, b(id_municipio_tse)` was added to the candidato section.
    # id_municipio therefore appears at the end for those years.
    # (1994 reference has id_municipio before id_municipio_tse — natural merge order.)
    if 1996 <= ano <= 2016 and "id_municipio" in result.columns:
        cols = [c for c in result.columns if c != "id_municipio"] + [
            "id_municipio"
        ]
        result = result[cols]

    return result


def _build_partido(ano: int) -> pd.DataFrame:
    """Build party results at mun-zone level."""
    frames = []
    for uf in UFS_PART[ano]:
        base = (
            INPUT_DIR
            / f"votacao_partido_munzona/votacao_partido_munzona_{ano}/votacao_partido_munzona_{ano}_{uf}"
        )
        df = read_raw_csv(
            str(base),
            drop_first_row=True,
            family="votacao_partido_munzona",
            ano=ano,
        )

        # Header-based selection by official TSE name. The vote columns have two
        # name variants and only one exists per year: 2000-2014 publish the
        # plain QT_VOTOS_* names; every other year publishes the *_VALIDOS
        # variant (so the choice is forced by availability, not a preference).
        if ano in (2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014):
            cols = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "CD_MUNICIPIO": "id_municipio_tse",
                "NR_ZONA": "zona",
                "DS_CARGO": "cargo",
                "NR_PARTIDO": "numero_partido",
                "SG_PARTIDO": "sigla_partido",
                "QT_VOTOS_NOMINAIS": "votos_nominais",
                "QT_VOTOS_LEGENDA": "votos_legenda",
            }
        else:  # 1994, 1996, 1998, 2016, 2018, 2020, 2022, 2024
            cols = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "CD_MUNICIPIO": "id_municipio_tse",
                "NR_ZONA": "zona",
                "DS_CARGO": "cargo",
                "NR_PARTIDO": "numero_partido",
                "SG_PARTIDO": "sigla_partido",
                "QT_VOTOS_NOMINAIS_VALIDOS": "votos_nominais",
                "QT_TOTAL_VOTOS_LEG_VALIDOS": "votos_legenda",
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

    result = pd.concat(frames, ignore_index=True)

    # For years <= 2016, the Stata reference .dta files were generated before
    # `order id_municipio, b(id_municipio_tse)` was added to the partido section.
    # id_municipio therefore appears at the end for those years.
    if ano <= 2016 and "id_municipio" in result.columns:
        cols = [c for c in result.columns if c != "id_municipio"] + [
            "id_municipio"
        ]
        result = result[cols]

    return result


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
