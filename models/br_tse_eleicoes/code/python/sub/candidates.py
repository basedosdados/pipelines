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
    select_named,
)


def _parse_schema(df: pd.DataFrame, ano: int) -> pd.DataFrame:
    """Select columns and rename based on the file's schema.

    Files with a header row (every TSE republication since the current
    generations — verified back to 1994) are read by official column name,
    which is immune to the positional drift TSE introduces when it silently
    re-republishes historical files. The positional year-blocks below remain
    only as the fallback for headerless vintages.
    """
    if df.attrs.get("tse_has_header"):
        # Official TSE header name -> BD column, in BD output order.
        # Covers both header generations: the 63-column one (NM_EMAIL,
        # DS_DETALHE_SITUACAO_CAND, DS_NACIONALIDADE, ...) and the current
        # 50-column one (DS_EMAIL; nationality/birthplace/situacao moved to
        # the complementar file, merged in build_candidatos).
        keep_cols = {
            "ano_eleicao": "ano",
            "aa_eleicao": "ano",
            "nr_turno": "turno",
            "cd_eleicao": "id_eleicao",
            "ds_eleicao": "tipo_eleicao",
            "dt_eleicao": "data_eleicao",
            "sg_uf": "sigla_uf",
            "sg_ue": "id_municipio_tse",
            "ds_cargo": "cargo",
            "sq_candidato": "sequencial",
            "nr_candidato": "numero",
            "nm_candidato": "nome",
            "nm_urna_candidato": "nome_urna",
            "nr_cpf_candidato": "cpf",
            "nm_email": "email",
            "ds_email": "email",
            "ds_detalhe_situacao_cand": "situacao",
            "nr_partido": "numero_partido",
            "sg_partido": "sigla_partido",
            "ds_nacionalidade": "nacionalidade",
            "sg_uf_nascimento": "sigla_uf_nascimento",
            "nm_municipio_nascimento": "municipio_nascimento",
            "dt_nascimento": "data_nascimento",
            "nr_titulo_eleitoral_candidato": "titulo_eleitoral",
            "ds_genero": "genero",
            "ds_grau_instrucao": "instrucao",
            "ds_estado_civil": "estado_civil",
            "ds_cor_raca": "raca",
            "ds_ocupacao": "ocupacao",
            "ds_sit_tot_turno": "resultado",
        }
        return select_named(df, keep_cols)

    if ano <= 2014 or ano == 2018:
        cols = {
            "v3": "ano",
            "v6": "turno",
            "v7": "id_eleicao",
            "v8": "tipo_eleicao",
            "v9": "data_eleicao",
            "v11": "sigla_uf",
            "v12": "id_municipio_tse",
            "v15": "cargo",
            "v16": "sequencial",
            "v17": "numero",
            "v18": "nome",
            "v19": "nome_urna",
            "v21": "cpf",
            "v22": "email",
            "v26": "situacao",
            "v28": "numero_partido",
            "v29": "sigla_partido",
            "v35": "nacionalidade",
            "v36": "sigla_uf_nascimento",
            "v38": "municipio_nascimento",
            "v39": "data_nascimento",
            "v41": "titulo_eleitoral",
            "v43": "genero",
            "v45": "instrucao",
            "v47": "estado_civil",
            "v49": "raca",
            "v51": "ocupacao",
            "v54": "resultado",
        }
    elif ano == 2016 or (2020 <= ano <= 2022):
        cols = {
            "v3": "ano",
            "v6": "turno",
            "v7": "id_eleicao",
            "v8": "tipo_eleicao",
            "v9": "data_eleicao",
            "v11": "sigla_uf",
            "v12": "id_municipio_tse",
            "v15": "cargo",
            "v16": "sequencial",
            "v17": "numero",
            "v18": "nome",
            "v19": "nome_urna",
            "v21": "cpf",
            "v22": "email",
            "v26": "situacao",
            "v28": "numero_partido",
            "v29": "sigla_partido",
            "v39": "nacionalidade",
            "v40": "sigla_uf_nascimento",
            "v42": "municipio_nascimento",
            "v43": "data_nascimento",
            "v45": "titulo_eleitoral",
            "v47": "genero",
            "v49": "instrucao",
            "v51": "estado_civil",
            "v53": "raca",
            "v55": "ocupacao",
            "v58": "resultado",
        }
    elif ano == 2024:
        cols = {
            "v3": "ano",
            "v6": "turno",
            "v7": "id_eleicao",
            "v8": "tipo_eleicao",
            "v9": "data_eleicao",
            "v11": "sigla_uf",
            "v12": "id_municipio_tse",
            "v15": "cargo",
            "v16": "sequencial",
            "v17": "numero",
            "v18": "nome",
            "v19": "nome_urna",
            "v21": "cpf",
            "v22": "email",
            "v26": "numero_partido",
            "v27": "sigla_partido",
            "v36": "sigla_uf_nascimento",
            "v37": "data_nascimento",
            "v38": "titulo_eleitoral",
            "v40": "genero",
            "v42": "instrucao",
            "v44": "estado_civil",
            "v46": "raca",
            "v48": "ocupacao",
            "v50": "resultado",
        }
    else:
        msg = f"Unsupported year: {ano}"
        raise ValueError(msg)

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
        df = read_raw_csv(str(base), drop_first_row=True)

        df = _parse_schema(df, ano)

        # Merge the complementar file when the main file lacks the
        # demographic/status block. The current 50-column consulta_cand
        # generation (all of 2024, and any year TSE republishes in it) moved
        # situacao / nacionalidade / municipio_nascimento there; the older
        # 63-column generation carries them in the main file, so no merge.
        comp_cols = ["nacionalidade", "municipio_nascimento", "situacao"]
        missing = [c for c in comp_cols if c not in df.columns]
        if missing:
            comp_base = (
                INPUT_DIR
                / f"consulta_cand/consulta_cand_complementar_{ano}/consulta_cand_complementar_{ano}_{uf}"
            )
            try:
                comp = read_raw_csv(str(comp_base))
                if comp.attrs.get("tse_has_header"):
                    keep_cols = {
                        "cd_eleicao": "id_eleicao",
                        "sq_candidato": "sequencial",
                        "ds_nacionalidade": "nacionalidade",
                        "nm_municipio_nascimento": "municipio_nascimento",
                        "ds_situacao_candidato_tot": "situacao",
                    }
                    comp = select_named(comp, keep_cols)
                else:
                    comp = comp[["v4", "v5", "v9", "v11", "v28"]].copy()
                    comp.columns = [
                        "id_eleicao",
                        "sequencial",
                        "nacionalidade",
                        "municipio_nascimento",
                        "situacao",
                    ]
                comp = comp[["id_eleicao", "sequencial", *missing]]
                df = df.merge(
                    comp,
                    on=["id_eleicao", "sequencial"],
                    how="left",
                )
            except FileNotFoundError:
                pass
            for col in comp_cols:
                if col not in df.columns:
                    df[col] = ""

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
