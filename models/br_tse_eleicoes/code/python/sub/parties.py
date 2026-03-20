"""
Build: partidos (parties/coalitions).
Equivalent of sub/partidos.do.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON, UFS_PARTIDOS
from utils.clean_election_type import clean_election_type_series
from utils.clean_party import clean_party_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br


def _try_read_partidos(ano: int, uf: str) -> pd.DataFrame:
    """Try multiple file-naming conventions for partido files."""
    candidates = [
        f"consulta_coligacao/CONSULTA_LEGENDA_{ano}/CONSULTA_LEGENDA_{ano}_{uf}",
        f"consulta_coligacao/consulta_legendas_{ano}/CONSULTA_LEGENDA_{ano}_{uf}",
        f"consulta_coligacao/consulta_legendas_{ano}/consulta_legendas_{ano}_{uf}",
        f"consulta_coligacao/consulta_coligacao_{ano}/consulta_coligacao_{ano}_{uf}",
    ]
    for pattern in candidates:
        base = INPUT_DIR / pattern
        for ext in [".txt", ".csv"]:
            path = base.with_suffix(ext)
            if path.exists():
                return pd.read_csv(
                    path,
                    sep=";",
                    header=None,
                    dtype=str,
                    encoding="utf-8",
                    keep_default_na=False,
                    quotechar='"',
                    on_bad_lines="warn",
                )
    msg = f"No file found for partidos {ano} {uf}"
    raise FileNotFoundError(msg)


def build_partidos(ano: int) -> pd.DataFrame:
    """Build parties for a single year."""
    is_federal = ano % 4 == 2  # federal years: 1990, 1994, 1998, 2002, ...
    frames = []

    for uf in UFS_PARTIDOS[ano]:
        raw = _try_read_partidos(ano, uf)
        raw.columns = [f"v{i + 1}" for i in range(len(raw.columns))]

        if ano <= 2012:
            # No header to drop for <= 2012
            if is_federal:
                df = raw[
                    [
                        "v3",
                        "v4",
                        "v5",
                        "v7",
                        "v10",
                        "v11",
                        "v12",
                        "v13",
                        "v14",
                        "v16",
                        "v17",
                        "v18",
                    ]
                ].copy()
                df.columns = [
                    "ano",
                    "turno",
                    "tipo_eleicao",
                    "sigla_uf",
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "nome_coligacao",
                    "composicao_coligacao",
                    "sequencial_coligacao",
                ]
            else:
                df = raw[
                    [
                        "v3",
                        "v4",
                        "v5",
                        "v6",
                        "v7",
                        "v10",
                        "v11",
                        "v12",
                        "v13",
                        "v14",
                        "v16",
                        "v17",
                        "v18",
                    ]
                ].copy()
                df.columns = [
                    "ano",
                    "turno",
                    "tipo_eleicao",
                    "sigla_uf",
                    "id_municipio_tse",
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "nome_coligacao",
                    "composicao_coligacao",
                    "sequencial_coligacao",
                ]
            df["id_eleicao"] = ""
            df["data_eleicao"] = ""

        elif 2014 <= ano <= 2020:
            raw = raw.iloc[1:].reset_index(drop=True)  # drop header row
            if is_federal:
                df = raw[
                    [
                        "v3",
                        "v6",
                        "v7",
                        "v8",
                        "v9",
                        "v11",
                        "v14",
                        "v15",
                        "v16",
                        "v17",
                        "v18",
                        "v19",
                        "v20",
                        "v21",
                    ]
                ].copy()
                df.columns = [
                    "ano",
                    "turno",
                    "id_eleicao",
                    "tipo_eleicao",
                    "data_eleicao",
                    "sigla_uf",
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "sequencial_coligacao",
                    "nome_coligacao",
                    "composicao_coligacao",
                ]
            else:
                df = raw[
                    [
                        "v3",
                        "v6",
                        "v7",
                        "v8",
                        "v9",
                        "v10",
                        "v11",
                        "v14",
                        "v15",
                        "v16",
                        "v17",
                        "v18",
                        "v19",
                        "v20",
                        "v21",
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
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "sequencial_coligacao",
                    "nome_coligacao",
                    "composicao_coligacao",
                ]

        else:  # >= 2022
            raw = raw.iloc[1:].reset_index(drop=True)
            if is_federal:
                df = raw[
                    [
                        "v3",
                        "v6",
                        "v7",
                        "v8",
                        "v9",
                        "v11",
                        "v14",
                        "v15",
                        "v16",
                        "v17",
                        "v18",
                        "v19",
                        "v20",
                        "v21",
                        "v22",
                        "v23",
                        "v24",
                        "v25",
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
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "numero_federacao",
                    "nome_federacacao",
                    "sigla_federacao",
                    "composicao_federacao",
                    "sequencial_coligacao",
                    "nome_coligacao",
                    "composicao_coligacao",
                    "situacao_legenda",
                ]
            else:
                df = raw[
                    [
                        "v3",
                        "v6",
                        "v7",
                        "v8",
                        "v9",
                        "v10",
                        "v11",
                        "v14",
                        "v15",
                        "v16",
                        "v17",
                        "v18",
                        "v19",
                        "v20",
                        "v21",
                        "v22",
                        "v23",
                        "v24",
                        "v25",
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
                    "cargo",
                    "tipo_agremiacao",
                    "numero",
                    "sigla",
                    "nome",
                    "numero_federacao",
                    "nome_federacacao",
                    "sigla_federacao",
                    "composicao_federacao",
                    "sequencial_coligacao",
                    "nome_coligacao",
                    "composicao_coligacao",
                    "situacao_legenda",
                ]

        # sigla_uf=BR -> empty
        df.loc[df["sigla_uf"] == "BR", "sigla_uf"] = ""
        if "id_municipio_tse" not in df.columns:
            df["id_municipio_tse"] = ""

        # destring
        for col in ["ano", "turno", "id_municipio_tse"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # fix partido name
        df.loc[
            df["nome"] == "PARTIDO DA HUMANISTA DA SOLIDARIEDADE", "nome"
        ] = "PARTIDO HUMANISTA DA SOLIDARIEDADE"

        # clean sequencial_coligacao
        if "sequencial_coligacao" in df.columns:
            df.loc[
                df["sequencial_coligacao"].isin(["-3", "-1"]),
                "sequencial_coligacao",
            ] = ""
        if "numero_federacao" in df.columns and ano >= 2022:
            df.loc[
                df["numero_federacao"].isin(["-3", "-1"]), "numero_federacao"
            ] = ""

        # clean NE/NULO in coalition columns
        for col in [
            "nome_coligacao",
            "nome_federacacao",
            "sigla_federacao",
            "composicao_federacao",
            "composicao_coligacao",
        ]:
            if col in df.columns:
                df.loc[df[col].isin(["#NE#", "#NULO#"]), col] = ""

        # clean strings
        for col in [
            "tipo_eleicao",
            "cargo",
            "tipo_agremiacao",
            "situacao_legenda",
        ]:
            if col in df.columns:
                df[col] = clean_string_series(df[col])

        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla"] = clean_party_series(df["sigla"], ano)

        # parse dates
        if "data_eleicao" in df.columns:
            df["data_eleicao"] = parse_date_br(df["data_eleicao"])

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # dedup
    result = result.drop_duplicates()

    # Handle coligacao vs partido isolado duplicates
    dup_keys = [
        "ano",
        "turno",
        "tipo_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    if "tipo_agremiacao" in result.columns:
        result["_has_coligacao"] = (
            result["tipo_agremiacao"] == "coligacao"
        ).astype(int)
        result["_max_coligacao"] = result.groupby(dup_keys)[
            "_has_coligacao"
        ].transform("max")
        result["_dup_count"] = result.groupby(dup_keys)["ano"].transform(
            "count"
        )
        # Drop partido isolado when coligacao exists for same key
        result = result[
            ~(
                (result["_dup_count"] > 1)
                & (result["_max_coligacao"] == 1)
                & (result["tipo_agremiacao"] == "partido isolado")
            )
        ]
        result = result.drop(
            columns=["_has_coligacao", "_max_coligacao", "_dup_count"]
        )

    # Final dedup by stricter key
    strict_keys = [
        "ano",
        "turno",
        "id_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "numero",
    ]
    result = result.drop_duplicates(subset=strict_keys, keep="first")

    return result


def build_all():
    for ano in sorted(UFS_PARTIDOS.keys()):
        print(f"  partidos {ano}")
        df = build_partidos(ano)
        out = OUTPUT_PYTHON / f"partidos_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
