"""
Build: candidatos (candidates).
Equivalent of sub/candidatos.do. The most complex table.
"""

from datetime import date

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON, UFS_CANDIDATOS
from utils.clean_education import clean_education_series
from utils.clean_election_type import clean_election_type_series
from utils.clean_marital_status import clean_marital_status_series
from utils.clean_party import clean_party_series
from utils.clean_result import clean_result_series
from utils.clean_string import clean_string_series
from utils.fix_candidate import fix_candidate
from utils.helpers import (
    clean_nulls,
    merge_municipio,
    pad_cpf,
    pad_titulo,
    parse_date_br,
    read_raw_csv,
)


def _parse_schema(df: pd.DataFrame, ano: int) -> pd.DataFrame:
    """Select and rename columns by official TSE name (header-based).

    Columns are addressed by their official TSE variable name (the frame is
    renamed from positional ``vN`` by :func:`utils.layout.resolve_columns`
    before this point), so the mapping is immune to the column-insertion drift
    that broke the old positional schema.

    The year split tracks the two republished layout generations: the FULL
    layout years (below) carry the CD/DS_DETALHE_SITUACAO_CAND, nacionalidade
    and NM_MUNICIPIO_NASCIMENTO block; the remaining even years carry the
    REDUCED layout (federação block inserted, those columns dropped). 2024 is
    reduced but enriches nacionalidade/municipio_nascimento/situacao from its
    complementar file, so its main schema omits those (see build_candidatos).
    The membership tuple is inlined (not a module constant) so the diagnostics
    tier-1 AST audit can resolve the exact years each branch covers.
    """
    if ano in (1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2016):
        cols = {
            "ANO_ELEICAO": "ano",
            "NR_TURNO": "turno",
            "CD_ELEICAO": "id_eleicao",
            "DS_ELEICAO": "tipo_eleicao",
            "DT_ELEICAO": "data_eleicao",
            "SG_UF": "sigla_uf",
            "SG_UE": "id_municipio_tse",
            "DS_CARGO": "cargo",
            "SQ_CANDIDATO": "sequencial",
            "NR_CANDIDATO": "numero",
            "NM_CANDIDATO": "nome",
            "NM_URNA_CANDIDATO": "nome_urna",
            "NR_CPF_CANDIDATO": "cpf",
            "NM_EMAIL": "email",
            "DS_DETALHE_SITUACAO_CAND": "situacao",
            "NR_PARTIDO": "numero_partido",
            "SG_PARTIDO": "sigla_partido",
            "DS_NACIONALIDADE": "nacionalidade",
            "SG_UF_NASCIMENTO": "sigla_uf_nascimento",
            "NM_MUNICIPIO_NASCIMENTO": "municipio_nascimento",
            "DT_NASCIMENTO": "data_nascimento",
            "NR_TITULO_ELEITORAL_CANDIDATO": "titulo_eleitoral",
            "DS_GENERO": "genero",
            "DS_GRAU_INSTRUCAO": "instrucao",
            "DS_ESTADO_CIVIL": "estado_civil",
            "DS_COR_RACA": "raca",
            "DS_OCUPACAO": "ocupacao",
            "DS_SIT_TOT_TURNO": "resultado",
        }
    elif ano == 2024:
        # nacionalidade / municipio_nascimento / situacao come from the
        # complementar file (merged in build_candidatos), not the main file.
        cols = {
            "ANO_ELEICAO": "ano",
            "NR_TURNO": "turno",
            "CD_ELEICAO": "id_eleicao",
            "DS_ELEICAO": "tipo_eleicao",
            "DT_ELEICAO": "data_eleicao",
            "SG_UF": "sigla_uf",
            "SG_UE": "id_municipio_tse",
            "DS_CARGO": "cargo",
            "SQ_CANDIDATO": "sequencial",
            "NR_CANDIDATO": "numero",
            "NM_CANDIDATO": "nome",
            "NM_URNA_CANDIDATO": "nome_urna",
            "NR_CPF_CANDIDATO": "cpf",
            "DS_EMAIL": "email",
            "NR_PARTIDO": "numero_partido",
            "SG_PARTIDO": "sigla_partido",
            "SG_UF_NASCIMENTO": "sigla_uf_nascimento",
            "DT_NASCIMENTO": "data_nascimento",
            "NR_TITULO_ELEITORAL_CANDIDATO": "titulo_eleitoral",
            "DS_GENERO": "genero",
            "DS_GRAU_INSTRUCAO": "instrucao",
            "DS_ESTADO_CIVIL": "estado_civil",
            "DS_COR_RACA": "raca",
            "DS_OCUPACAO": "ocupacao",
            "DS_SIT_TOT_TURNO": "resultado",
        }
    else:  # 1994, 1996, 2014, 2018, 2020, 2022 — reduced layout, no complementar
        cols = {
            "ANO_ELEICAO": "ano",
            "NR_TURNO": "turno",
            "CD_ELEICAO": "id_eleicao",
            "DS_ELEICAO": "tipo_eleicao",
            "DT_ELEICAO": "data_eleicao",
            "SG_UF": "sigla_uf",
            "SG_UE": "id_municipio_tse",
            "DS_CARGO": "cargo",
            "SQ_CANDIDATO": "sequencial",
            "NR_CANDIDATO": "numero",
            "NM_CANDIDATO": "nome",
            "NM_URNA_CANDIDATO": "nome_urna",
            "NR_CPF_CANDIDATO": "cpf",
            "DS_EMAIL": "email",
            "DS_SITUACAO_CANDIDATURA": "situacao",
            "NR_PARTIDO": "numero_partido",
            "SG_PARTIDO": "sigla_partido",
            "SG_UF_NASCIMENTO": "sigla_uf_nascimento",
            "DT_NASCIMENTO": "data_nascimento",
            "NR_TITULO_ELEITORAL_CANDIDATO": "titulo_eleitoral",
            "DS_GENERO": "genero",
            "DS_GRAU_INSTRUCAO": "instrucao",
            "DS_ESTADO_CIVIL": "estado_civil",
            "DS_COR_RACA": "raca",
            "DS_OCUPACAO": "ocupacao",
            "DS_SIT_TOT_TURNO": "resultado",
        }

    available = {k: v for k, v in cols.items() if k in df.columns}
    return df[list(available.keys())].rename(columns=available)


def build_candidatos(ano: int) -> pd.DataFrame:
    """Build candidates for a single year."""
    frames = []

    for uf in UFS_CANDIDATOS[ano]:
        base = (
            INPUT_DIR
            / f"consulta_cand/consulta_cand_{ano}/consulta_cand_{ano}_{uf}"
        )
        df = read_raw_csv(
            str(base), drop_first_row=True, family="consulta_cand", ano=ano
        )

        df = _parse_schema(df, ano)

        # 2024: merge complementar file
        if ano == 2024:
            comp_base = (
                INPUT_DIR
                / f"consulta_cand/consulta_cand_complementar_{ano}/consulta_cand_complementar_{ano}_{uf}"
            )
            try:
                comp = read_raw_csv(
                    str(comp_base),
                    drop_first_row=True,
                    family="consulta_cand_complementar",
                    ano=ano,
                )
                col_map = {
                    "CD_ELEICAO": "id_eleicao",
                    "SQ_CANDIDATO": "sequencial",
                    "DS_NACIONALIDADE": "nacionalidade",
                    "NM_MUNICIPIO_NASCIMENTO": "municipio_nascimento",
                    "DS_SITUACAO_CANDIDATO_TOT": "situacao",
                }
                comp = comp[list(col_map.keys())].rename(columns=col_map)
                df = df.merge(
                    comp,
                    on=["id_eleicao", "sequencial"],
                    how="left",
                    suffixes=("", "_comp"),
                )
                # Use complementar values where main is missing
                if "situacao_comp" in df.columns:
                    df["situacao"] = df["situacao"].fillna(df["situacao_comp"])
                    df = df.drop(columns=["situacao_comp"])
            except FileNotFoundError:
                if "nacionalidade" not in df.columns:
                    df["nacionalidade"] = ""
                if "municipio_nascimento" not in df.columns:
                    df["municipio_nascimento"] = ""
                if "situacao" not in df.columns:
                    df["situacao"] = ""

        # destring
        for col in ["ano", "turno", "id_municipio_tse"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # merge municipio
        df["id_municipio_tse"] = (
            df["id_municipio_tse"]
            .astype("Int64")
            .astype(str)
            .replace("<NA>", "")
        )
        df = merge_municipio(df)

        # clean nulls
        df = clean_nulls(df)

        # clean strings
        for col in [
            "tipo_eleicao",
            "cargo",
            "nacionalidade",
            "genero",
            "instrucao",
            "estado_civil",
            "raca",
            "ocupacao",
            "situacao",
            "resultado",
        ]:
            if col in df.columns:
                df[col] = clean_string_series(df[col])

        for col in ["nome", "nome_urna", "municipio_nascimento"]:
            if col in df.columns:
                # Use str.title() which matches Stata's ustrtitle() for most
                # chars (capitalizes after hyphens, dots, parens, slashes).
                # Then fix U+00B4 (acute accent): Stata treats it as part
                # of the word, so D\u00b4Avila should be D\u00b4avila.
                df[col] = (
                    df[col]
                    .str.title()
                    .str.replace(
                        r"(?<=[A-Za-zÀ-ÿ])\u00b4([A-Z])",
                        lambda m: "\u00b4" + m.group(1).lower(),
                        regex=True,
                    )
                )

        if "email" in df.columns:
            df["email"] = df["email"].str.lower()

        # apply cleaning functions
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df["sigla_partido"] = clean_party_series(df["sigla_partido"], ano)
        df = fix_candidate(df)
        if "instrucao" in df.columns:
            df["instrucao"] = clean_education_series(df["instrucao"])
        if "estado_civil" in df.columns:
            df["estado_civil"] = clean_marital_status_series(
                df["estado_civil"]
            )
        if "resultado" in df.columns:
            df["resultado"] = clean_result_series(df["resultado"])

        # pad CPF and titulo
        if "cpf" in df.columns:
            df["cpf"] = pad_cpf(df["cpf"])
        if "titulo_eleitoral" in df.columns:
            df["titulo_eleitoral"] = pad_titulo(df["titulo_eleitoral"])

        # cargo fixes
        if "cargo" in df.columns:
            df["cargo"] = df["cargo"].replace(
                {
                    "vice presidente": "vice-presidente",
                    "vice prefeito": "vice-prefeito",
                }
            )

        # genero fixes
        if "genero" in df.columns:
            df.loc[
                df["genero"].isin(["nao divulgavel", "nao informado"]),
                "genero",
            ] = ""

        # nacionalidade fixes
        if "nacionalidade" in df.columns:
            df["nacionalidade"] = df["nacionalidade"].replace(
                {
                    "brasileira nata": "brasileira",
                }
            )
            df.loc[
                df["nacionalidade"].isin(
                    ["nao divulgavel", "nao informado", "nao informada"]
                ),
                "nacionalidade",
            ] = ""

        # sigla_uf_nascimento fix
        if "sigla_uf_nascimento" in df.columns:
            df.loc[df["sigla_uf_nascimento"] == " ", "sigla_uf_nascimento"] = (
                ""
            )

        # raca fixes
        if "raca" in df.columns:
            df.loc[
                df["raca"].isin(
                    [
                        "sem informacao",
                        "nao divulgavel",
                        "nao informado",
                        "nao informada",
                    ]
                ),
                "raca",
            ] = ""

        # resultado fixes
        if "resultado" in df.columns:
            df.loc[df["resultado"].isin(["-1", "1", "4"]), "resultado"] = ""

        # sigla_uf fix for president
        if "cargo" in df.columns:
            df.loc[
                df["cargo"].isin(["presidente", "vice-presidente"]), "sigla_uf"
            ] = ""

        # parse dates
        for col in ["data_eleicao", "data_nascimento"]:
            if col in df.columns:
                df[col] = parse_date_br(df[col])

        # compute age
        if "data_nascimento" in df.columns:
            ref_date = date(ano, 10, 1)

            def _calc_age(dob_str, ref_date=ref_date):
                if not dob_str or len(dob_str) < 10:
                    return pd.NA
                try:
                    y, m, d = (
                        int(dob_str[:4]),
                        int(dob_str[5:7]),
                        int(dob_str[8:10]),
                    )
                    dob = date(y, m, d)
                    age = round((ref_date - dob).days / 365.25)
                    if age < 15 or age > 100:
                        return pd.NA
                    return age
                except (ValueError, OverflowError):
                    return pd.NA

            df["idade"] = df["data_nascimento"].map(_calc_age)

        # drop missing ano
        df = df[df["ano"].notna()]

        df = df.drop_duplicates()

        # Reorder: idade after data_nascimento (Stata: order idade, a(data_nascimento))
        cols = list(df.columns)
        if "idade" in cols:
            cols.remove("idade")
            idx = cols.index("data_nascimento") + 1
            cols.insert(idx, "idade")
            df = df[cols]

        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    return result


def build_all():
    for ano in sorted(UFS_CANDIDATOS.keys()):
        print(f"  candidatos {ano}")
        df = build_candidatos(ano)
        out = OUTPUT_PYTHON / f"candidatos_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
