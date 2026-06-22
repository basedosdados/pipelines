"""
Build: partidos (parties/coalitions).
Equivalent of sub/partidos.do.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON, UFS_PARTIDOS
from utils.clean_election_type import clean_election_type_series
from utils.clean_party import clean_party_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio
from utils.layout import resolve_columns


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
                    encoding="latin-1",
                    keep_default_na=False,
                    quotechar='"',
                    on_bad_lines="warn",
                )
    msg = f"No file found for partidos {ano} {uf}"
    raise FileNotFoundError(msg)


def build_partidos(ano: int) -> pd.DataFrame:
    """Build parties for a single year."""
    frames = []

    for uf in UFS_PARTIDOS[ano]:
        raw = _try_read_partidos(ano, uf)
        raw.columns = [f"v{i + 1}" for i in range(len(raw.columns))]
        # The modern republished files (1994+) carry a header row; 1990 is the
        # only headerless legacy file.
        if ano != 1990:
            raw = raw.iloc[1:].reset_index(drop=True)
        raw = resolve_columns(raw, "consulta_coligacao", ano)

        # Header-based selection by official TSE name. SG_UE maps to
        # id_municipio_tse for every year; for federal elections SG_UE is the
        # alphabetic UF/BR code, which destrings to empty downstream (so the old
        # federal/municipal split is unnecessary). Three layout generations:
        # 1990 (legacy headerless names), 1994-2016 (n=23), 2018-2024 (n=28 with
        # the federação block + DS_SITUACAO).
        if ano == 1990:
            col_map = {
                "ANO_ELEICAO": "ano",
                "NUM_TURNO": "turno",
                "DESCRICAO_ELEICAO": "tipo_eleicao",
                "SIGLA_UF": "sigla_uf",
                "SIGLA_UE": "id_municipio_tse",
                "DESCRICAO_CARGO": "cargo",
                "TIPO_LEGENDA": "tipo_agremiacao",
                "NUMERO_PARTIDO": "numero",
                "SIGLA_PARTIDO": "sigla",
                "NOME_PARTIDO": "nome",
                "SEQUENCIAL_COLIGACAO": "sequencial_coligacao",
                "NOME_COLIGACAO": "nome_coligacao",
                "COMPOSICAO_COLIGACAO": "composicao_coligacao",
            }
        elif ano >= 2018:
            col_map = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "SG_UE": "id_municipio_tse",
                "DS_CARGO": "cargo",
                "TP_AGREMIACAO": "tipo_agremiacao",
                "NR_PARTIDO": "numero",
                "SG_PARTIDO": "sigla",
                "NM_PARTIDO": "nome",
                "NR_FEDERACAO": "numero_federacao",
                "NM_FEDERACAO": "nome_federacacao",
                "SG_FEDERACAO": "sigla_federacao",
                "DS_COMPOSICAO_FEDERACAO": "composicao_federacao",
                "SQ_COLIGACAO": "sequencial_coligacao",
                "NM_COLIGACAO": "nome_coligacao",
                "DS_COMPOSICAO_COLIGACAO": "composicao_coligacao",
                "DS_SITUACAO": "situacao_legenda",
            }
        else:  # 1994-2016
            col_map = {
                "ANO_ELEICAO": "ano",
                "NR_TURNO": "turno",
                "CD_ELEICAO": "id_eleicao",
                "DS_ELEICAO": "tipo_eleicao",
                "DT_ELEICAO": "data_eleicao",
                "SG_UF": "sigla_uf",
                "SG_UE": "id_municipio_tse",
                "DS_CARGO": "cargo",
                "TP_AGREMIACAO": "tipo_agremiacao",
                "NR_PARTIDO": "numero",
                "SG_PARTIDO": "sigla",
                "NM_PARTIDO": "nome",
                "SQ_COLIGACAO": "sequencial_coligacao",
                "NM_COLIGACAO": "nome_coligacao",
                "DS_COMPOSICAO_COLIGACAO": "composicao_coligacao",
            }

        available = {k: v for k, v in col_map.items() if k in raw.columns}
        df = raw[list(available.keys())].rename(columns=available)
        for c in ("id_eleicao", "data_eleicao", "id_municipio_tse"):
            if c not in df.columns:
                df[c] = ""

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

    # Column reorder to match Stata (order commands at lines 149, 177, 217):
    # Final order: ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf
    #   id_municipio_tse cargo id_municipio ... then rest with tipo_agremiacao after nome
    cols = list(result.columns)

    # Move tipo_agremiacao after nome
    if "tipo_agremiacao" in cols and "nome" in cols:
        cols.remove("tipo_agremiacao")
        idx = cols.index("nome") + 1
        cols.insert(idx, "tipo_agremiacao")

    # Build desired prefix order
    prefix = []
    for c in [
        "ano",
        "turno",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "id_municipio_tse",
        "cargo",
        "id_municipio",
    ]:
        if c in cols:
            prefix.append(c)
            cols.remove(c)
    result = result[prefix + cols]

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
