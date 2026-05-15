"""
Build: bens_candidato, receitas_candidato, despesas_candidato.
Equivalent of sub/prestacao_contas.do.
"""

import pandas as pd
from config import INPUT_DIR, OUTPUT_PYTHON
from utils.clean_election_type import clean_election_type_series
from utils.clean_string import clean_string_series
from utils.helpers import merge_municipio, parse_date_br, read_raw_csv

# fmt: off
_UFS_MUN = ["AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
_UFS_FED = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
_UFS_FED_BR = ["AC", "AL", "AM", "AP", "BA", "BR", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
# fmt: on


def _ufs_for_year(ano: int) -> list[str]:
    return _UFS_FED if ano % 4 == 2 else _UFS_MUN


def _parse_date_receipt(s: pd.Series) -> pd.Series:
    """Parse date with the receipt/expense date logic: truncate to 10 chars, pad if 9."""

    def _convert(val):
        if not isinstance(val, str) or not val.strip():
            return ""
        val = val.strip()[:10]
        if len(val) == 9:
            val = "0" + val
        if len(val) < 10:
            return ""
        # DD/MM/YYYY -> YYYY-MM-DD
        day, month, year = val[:2], val[3:5], val[6:10]
        try:
            if int(year) < 1900:
                return ""
        except ValueError:
            return ""
        return f"{year}-{month}-{day}"

    return s.map(_convert)


def _clean_valor(s: pd.Series) -> pd.Series:
    """Replace comma with dot and convert to numeric."""
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _clean_br(s: pd.Series) -> pd.Series:
    """Replace 'BR' with empty string in UF columns."""
    return s.where(s != "BR", "")


def _clean_nulo_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Replace #NULO#, #NULO, -1 with empty string for specific columns."""
    nulls = {"#NULO#", "#NULO", "-1"}
    for col in cols:
        if col in df.columns:
            df[col] = df[col].where(~df[col].isin(nulls), "")
    return df


def _pad_cpf_cnpj_2012(s: pd.Series) -> pd.Series:
    """Special CPF/CNPJ padding for 2012 receitas."""

    def _pad(val):
        if not isinstance(val, str):
            return val
        n = len(val)
        if n == 8:
            return "000" + val
        if n == 9:
            return "00" + val
        if n == 10:
            return "0" + val
        if n < 8:
            return ""
        return val

    return s.map(_pad)


# ---------------------------------------------------------------------------
# bens_candidato
# ---------------------------------------------------------------------------


def build_bens(ano: int) -> pd.DataFrame:
    """Build bens_candidato for a single year (2006-2024)."""
    frames = []
    for uf in _ufs_for_year(ano):
        base = (
            INPUT_DIR
            / f"bem_candidato/bem_candidato_{ano}/bem_candidato_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=(ano >= 2014))

        if ano <= 2012:
            df = df[["v3", "v4", "v5", "v6", "v9", "v10"]].copy()
            df.columns = [
                "ano",
                "tipo_eleicao",
                "sigla_uf",
                "sequencial_candidato",
                "descricao_item",
                "valor_item",
            ]
            df["tipo_item"] = ""
            df["id_eleicao"] = ""
            df["data_eleicao"] = ""
        else:  # >= 2014
            df = df[
                ["v3", "v6", "v7", "v8", "v9", "v12", "v15", "v16", "v17"]
            ].copy()
            df.columns = [
                "ano",
                "id_eleicao",
                "tipo_eleicao",
                "data_eleicao",
                "sigla_uf",
                "sequencial_candidato",
                "tipo_item",
                "descricao_item",
                "valor_item",
            ]

        df["tipo_eleicao"] = clean_string_series(df["tipo_eleicao"])
        df["tipo_eleicao"] = clean_election_type_series(
            df["tipo_eleicao"], ano
        )
        df.loc[df["descricao_item"] == "#NULO#", "descricao_item"] = ""
        # Stata truncates multiline fields at the first newline
        df["descricao_item"] = df["descricao_item"].str.split("\n").str[0]
        df["valor_item"] = _clean_valor(df["valor_item"])
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
        df["data_eleicao"] = parse_date_br(df["data_eleicao"])
        df = df[df["ano"].notna()]
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    col_order = [
        "ano",
        "id_eleicao",
        "tipo_eleicao",
        "data_eleicao",
        "sigla_uf",
        "sequencial_candidato",
        "tipo_item",
        "descricao_item",
        "valor_item",
    ]
    return result[[c for c in col_order if c in result.columns]]


# ---------------------------------------------------------------------------
# receitas_candidato
# ---------------------------------------------------------------------------


def _build_receitas_2002() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2002/2002/Candidato/Receita/ReceitaCandidato"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        ["v1", "v2", "v3", "v4", "v6", "v7", "v8", "v9", "v10", "v11", "v12"]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "sigla_uf",
        "sigla_partido",
        "cargo",
        "numero_candidato",
        "data_receita",
        "cpf_cnpj_doador",
        "sigla_uf_doador",
        "nome_doador",
        "valor_receita",
        "especie_receita",
    ]
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df["sigla_uf_doador"] = _clean_br(df["sigla_uf_doador"])
    df = _clean_nulo_cols(
        df, ["cpf_cnpj_doador", "sigla_uf_doador", "nome_doador"]
    )
    for col in ["cargo", "especie_receita"]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_receitas_2004() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2004/2004/Candidato/Receita/ReceitaCandidato"
    )
    path = None
    for ext in [".csv", ".txt"]:
        p = base.with_suffix(ext)
        if p.exists():
            path = p
            break
    if path is None:
        raise FileNotFoundError("No file found for receitas 2004")

    df = pd.read_csv(
        path,
        sep=";",
        header=None,
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
        quotechar='"',
        on_bad_lines="warn",
        quoting=3,  # QUOTE_NONE - bindquotes(nobind)
    )
    df.columns = [f"v{i + 1}" for i in range(len(df.columns))]
    # Remove quotes from all cells
    for col in df.columns:
        df[col] = df[col].str.replace('"', "", regex=False)
    df = df.iloc[1:].reset_index(drop=True)
    df = df[
        [
            "v2",
            "v4",
            "v5",
            "v7",
            "v9",
            "v10",
            "v11",
            "v12",
            "v14",
            "v16",
            "v17",
            "v18",
        ]
    ].copy()
    df.columns = [
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "id_municipio_tse",
        "sigla_partido",
        "valor_receita",
        "data_receita",
        "origem_receita",
        "especie_receita",
        "nome_doador",
        "cpf_cnpj_doador",
        "situacao_receita",
    ]
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df = _clean_nulo_cols(
        df, ["cpf_cnpj_doador", "nome_doador", "origem_receita"]
    )
    for col in [
        "cargo",
        "origem_receita",
        "especie_receita",
        "situacao_receita",
    ]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_receitas_2006() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2006/2006/Candidato/Receita/ReceitaCandidato"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        [
            "v1",
            "v3",
            "v5",
            "v6",
            "v7",
            "v9",
            "v10",
            "v11",
            "v12",
            "v14",
            "v16",
            "v17",
            "v18",
            "v19",
        ]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "cnpj_candidato",
        "sigla_partido",
        "valor_receita",
        "data_receita",
        "origem_receita",
        "especie_receita",
        "nome_doador",
        "cpf_cnpj_doador",
        "sigla_uf_doador",
        "situacao_receita",
    ]
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df["sigla_uf_doador"] = _clean_br(df["sigla_uf_doador"])
    df = _clean_nulo_cols(
        df,
        [
            "cpf_cnpj_doador",
            "sigla_uf_doador",
            "nome_doador",
            "origem_receita",
        ],
    )
    for col in [
        "cargo",
        "origem_receita",
        "especie_receita",
        "situacao_receita",
    ]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_receitas_2008() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2008/receitas_candidatos_2008_brasil"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        [
            "v1",
            "v4",
            "v6",
            "v7",
            "v9",
            "v12",
            "v14",
            "v15",
            "v16",
            "v17",
            "v19",
            "v21",
            "v22",
            "v23",
            "v25",
            "v26",
            "v27",
            "v28",
        ]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "id_municipio_tse",
        "cnpj_candidato",
        "sigla_partido",
        "valor_receita",
        "data_receita",
        "origem_receita",
        "especie_receita",
        "nome_doador",
        "cpf_cnpj_doador",
        "sigla_uf_doador",
        "id_municipio_tse_doador",
        "situacao_receita",
        "nome_administrador",
        "cpf_administrador",
    ]
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df["sigla_uf_doador"] = _clean_br(df["sigla_uf_doador"])
    df = _clean_nulo_cols(
        df,
        [
            "cpf_cnpj_doador",
            "sigla_uf_doador",
            "nome_doador",
            "id_municipio_tse_doador",
            "origem_receita",
        ],
    )
    for col in [
        "cargo",
        "origem_receita",
        "especie_receita",
        "situacao_receita",
    ]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_municipio_tse_doador"] = pd.to_numeric(
        df["id_municipio_tse_doador"], errors="coerce"
    )
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_receitas_2010() -> pd.DataFrame:
    ufs = _UFS_FED_BR
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_contas_2010/candidato/{uf}/ReceitasCandidatos"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v2",
                "v3",
                "v4",
                "v5",
                "v6",
                "v9",
                "v10",
                "v11",
                "v12",
                "v13",
                "v14",
                "v15",
                "v16",
                "v17",
                "v18",
                "v19",
            ]
        ].copy()
        df.columns = [
            "sequencial_candidato",
            "sigla_uf",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "entrega_conjunto",
            "numero_recibo_eleitoral",
            "numero_documento",
            "cpf_cnpj_doador",
            "nome_doador",
            "data_receita",
            "valor_receita",
            "origem_receita",
            "fonte_receita",
            "natureza_receita",
            "descricao_receita",
        ]
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df["descricao_receita"] = (
        df["descricao_receita"].str.strip().str.replace('"', "", regex=False)
    )
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df = _clean_nulo_cols(
        df,
        [
            "numero_documento",
            "origem_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador",
            "nome_doador",
        ],
    )
    for col in [
        "cargo",
        "entrega_conjunto",
        "origem_receita",
        "fonte_receita",
        "natureza_receita",
    ]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_receitas_2012() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_final_2012/receitas_candidatos_2012_brasil"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        [
            "v1",
            "v2",
            "v4",
            "v5",
            "v6",
            "v8",
            "v9",
            "v10",
            "v13",
            "v14",
            "v15",
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
            "v28",
        ]
    ].copy()
    df.columns = [
        "id_eleicao",
        "tipo_eleicao",
        "sequencial_candidato",
        "sigla_uf",
        "id_municipio_tse",
        "sigla_partido",
        "numero_candidato",
        "cargo",
        "numero_recibo_eleitoral",
        "numero_documento",
        "cpf_cnpj_doador",
        "nome_doador",
        "nome_doador_rf",
        "numero_partido_doador",
        "numero_candidato_doador",
        "cnae_2_doador",
        "descricao_cnae_2_doador",
        "data_receita",
        "valor_receita",
        "origem_receita",
        "fonte_receita",
        "natureza_receita",
        "descricao_receita",
    ]
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df = _clean_nulo_cols(
        df,
        [
            "numero_documento",
            "origem_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
        ],
    )
    df["cpf_cnpj_doador"] = _pad_cpf_cnpj_2012(df["cpf_cnpj_doador"])
    for col in [
        "tipo_eleicao",
        "cargo",
        "descricao_cnae_2_doador",
        "origem_receita",
        "fonte_receita",
        "natureza_receita",
    ]:
        df[col] = clean_string_series(df[col])
    df["data_receita"] = df["data_receita"].str[:10]
    df["data_receita"] = _parse_date_receipt(df["data_receita"])
    df["valor_receita"] = _clean_valor(df["valor_receita"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["tipo_eleicao"] = clean_election_type_series(df["tipo_eleicao"], 2012)
    df["data_eleicao"] = ""
    return df


def _build_receitas_2014() -> pd.DataFrame:
    ufs = _UFS_FED_BR
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_final_2014/receitas_candidatos_2014_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v1",
                "v2",
                "v4",
                "v5",
                "v6",
                "v7",
                "v8",
                "v9",
                "v12",
                "v13",
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
                "v26",
                "v27",
                "v28",
                "v29",
                "v30",
                "v31",
                "v32",
            ]
        ].copy()
        df.columns = [
            "id_eleicao",
            "tipo_eleicao",
            "cnpj_prestador_contas",
            "sequencial_candidato",
            "sigla_uf",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "numero_recibo_eleitoral",
            "numero_documento",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "sigla_uf_doador",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
            "data_receita",
            "valor_receita",
            "origem_receita",
            "fonte_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador_orig",
            "nome_doador_orig",
            "tipo_doador_orig",
            "descricao_cnae_2_doador_orig",
            "nome_doador_orig_rf",
        ]
        df["data_eleicao"] = ""
        df["data_receita"] = df["data_receita"].str[:10]
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # Append supplementary file
    sup_base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_final_sup_2014/receitas_candidatos_prestacao_contas_final_2014_sup"
    )
    try:
        sup = read_raw_csv(str(sup_base), drop_first_row=True)
        sup = sup[
            [
                "v1",
                "v2",
                "v4",
                "v5",
                "v6",
                "v9",
                "v10",
                "v11",
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
                "v26",
                "v27",
                "v28",
                "v29",
                "v30",
                "v31",
                "v32",
                "v33",
                "v34",
                "v35",
            ]
        ].copy()
        sup.columns = [
            "id_eleicao",
            "tipo_eleicao",
            "cnpj_prestador_contas",
            "sequencial_candidato",
            "sigla_uf",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "numero_recibo_eleitoral",
            "numero_documento",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "sigla_uf_doador",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
            "data_receita",
            "valor_receita",
            "origem_receita",
            "fonte_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador_orig",
            "nome_doador_orig",
            "tipo_doador_orig",
            "descricao_cnae_2_doador_orig",
            "nome_doador_orig_rf",
        ]
        sup["data_eleicao"] = ""
        sup["data_receita"] = sup["data_receita"].str[:10]
        result = pd.concat([result, sup], ignore_index=True)
    except FileNotFoundError:
        pass

    # Common cleaning
    for col in ["sigla_uf", "sigla_uf_doador"]:
        result[col] = _clean_br(result[col])
    result = _clean_nulo_cols(
        result,
        [
            "numero_documento",
            "origem_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "sigla_uf_doador",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
            "cpf_cnpj_doador_orig",
            "nome_doador_orig",
            "tipo_doador_orig",
            "descricao_cnae_2_doador_orig",
            "nome_doador_orig_rf",
        ],
    )
    for col in [
        "tipo_eleicao",
        "cargo",
        "descricao_cnae_2_doador",
        "origem_receita",
        "fonte_receita",
        "natureza_receita",
    ]:
        result[col] = clean_string_series(result[col])
    result["data_receita"] = _parse_date_receipt(result["data_receita"])
    result["valor_receita"] = _clean_valor(result["valor_receita"])
    result["tipo_eleicao"] = clean_election_type_series(
        result["tipo_eleicao"], 2014
    )
    return result


def _build_receitas_2016() -> pd.DataFrame:
    ufs = _UFS_MUN
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_contas_2016/receitas_candidatos_2016_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v1",
                "v2",
                "v4",
                "v5",
                "v6",
                "v7",
                "v9",
                "v10",
                "v11",
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
                "v26",
                "v27",
                "v28",
                "v29",
                "v30",
                "v31",
                "v32",
                "v33",
                "v34",
                "v35",
            ]
        ].copy()
        df.columns = [
            "id_eleicao",
            "tipo_eleicao",
            "cnpj_prestador_contas",
            "sequencial_candidato",
            "sigla_uf",
            "id_municipio_tse",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "numero_recibo_eleitoral",
            "numero_documento",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "ue_doador",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
            "data_receita",
            "valor_receita",
            "origem_receita",
            "fonte_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador_orig",
            "nome_doador_orig",
            "tipo_doador_orig",
            "descricao_cnae_2_doador_orig",
            "nome_doador_orig_rf",
        ]
        df["data_eleicao"] = ""
        df["data_receita"] = df["data_receita"].str[:10]
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    # Split ue_doador into sigla_uf_doador and id_municipio_tse_doador
    result["sigla_uf_doador"] = result["ue_doador"].where(
        result["ue_doador"].str.len() == 2, ""
    )
    result["id_municipio_tse_doador"] = result["ue_doador"].where(
        result["ue_doador"].str.len() > 2, ""
    )
    result = result.drop(columns=["ue_doador"])

    for col in ["sigla_uf", "sigla_uf_doador"]:
        result[col] = _clean_br(result[col])
    result = _clean_nulo_cols(
        result,
        [
            "numero_documento",
            "origem_receita",
            "natureza_receita",
            "descricao_receita",
            "cpf_cnpj_doador",
            "nome_doador",
            "nome_doador_rf",
            "sigla_uf_doador",
            "id_municipio_tse_doador",
            "numero_partido_doador",
            "numero_candidato_doador",
            "cnae_2_doador",
            "descricao_cnae_2_doador",
            "cpf_cnpj_doador_orig",
            "nome_doador_orig",
            "tipo_doador_orig",
            "descricao_cnae_2_doador_orig",
            "nome_doador_orig_rf",
        ],
    )
    for col in [
        "tipo_eleicao",
        "cargo",
        "descricao_cnae_2_doador",
        "origem_receita",
        "fonte_receita",
        "natureza_receita",
    ]:
        result[col] = clean_string_series(result[col])
    result["data_receita"] = _parse_date_receipt(result["data_receita"])
    result["valor_receita"] = _clean_valor(result["valor_receita"])
    result["tipo_eleicao"] = clean_election_type_series(
        result["tipo_eleicao"], 2016
    )
    result["id_municipio_tse"] = pd.to_numeric(
        result["id_municipio_tse"], errors="coerce"
    )
    return result


def _build_receitas_2018_plus(ano: int) -> pd.DataFrame:
    """Build receitas for 2018-2024."""
    if ano == 2018:
        ufs = _UFS_FED_BR
    elif ano % 4 == 0:
        ufs = _UFS_MUN
    else:
        ufs = _UFS_FED_BR

    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}/receitas_candidatos_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        # 2018 does not have v14 (id_municipio_tse); 2020+ does
        if ano == 2018:
            keep = [
                "v6",
                "v7",
                "v8",
                "v9",
                "v10",
                "v11",
                "v12",
                "v13",
                "v16",
                "v18",
                "v19",
                "v20",
                "v24",
                "v25",
                "v28",
                "v30",
                "v32",
                "v34",
                "v35",
                "v36",
                "v37",
                "v38",
                "v39",
                "v41",
                "v42",
                "v43",
                "v45",
                "v46",
                "v48",
                "v49",
                "v50",
                "v52",
                "v53",
                "v54",
                "v55",
                "v56",
                "v57",
            ]
        else:
            keep = [
                "v6",
                "v7",
                "v8",
                "v9",
                "v10",
                "v11",
                "v12",
                "v13",
                "v14",
                "v16",
                "v18",
                "v19",
                "v20",
                "v24",
                "v25",
                "v28",
                "v30",
                "v32",
                "v34",
                "v35",
                "v36",
                "v37",
                "v38",
                "v39",
                "v41",
                "v42",
                "v43",
                "v45",
                "v46",
                "v48",
                "v49",
                "v50",
                "v52",
                "v53",
                "v54",
                "v55",
                "v56",
                "v57",
            ]

        available = [c for c in keep if c in df.columns]
        df = df[available].copy()

        # Build column name mapping
        col_map = {
            "v6": "id_eleicao",
            "v7": "tipo_eleicao",
            "v8": "data_eleicao",
            "v9": "turno",
            "v10": "tipo_prestacao_contas",
            "v11": "data_prestacao_contas",
            "v12": "sequencial_prestador_contas",
            "v13": "sigla_uf",
            "v14": "id_municipio_tse",
            "v16": "cnpj_prestador_contas",
            "v18": "cargo",
            "v19": "sequencial_candidato",
            "v20": "numero_candidato",
            "v24": "numero_partido",
            "v25": "sigla_partido",
            "v28": "fonte_receita",
            "v30": "origem_receita",
            "v32": "natureza_receita",
            "v34": "especie_receita",
            "v35": "cnae_2_doador",
            "v36": "descricao_cnae_2_doador",
            "v37": "cpf_cnpj_doador",
            "v38": "nome_doador",
            "v39": "nome_doador_rf",
            "v41": "esfera_partidaria_doador",
            "v42": "sigla_uf_doador",
            "v43": "id_municipio_tse_doador",
            "v45": "sequencial_candidato_doador",
            "v46": "numero_candidato_doador",
            "v48": "cargo_candidato_doador",
            "v49": "numero_partido_doador",
            "v50": "sigla_partido_doador",
            "v52": "numero_recibo_doacao",
            "v53": "numero_documento_doacao",
            "v54": "sequencial_receita",
            "v55": "data_receita",
            "v56": "descricao_receita",
            "v57": "valor_receita",
        }
        df = df.rename(
            columns={k: v for k, v in col_map.items() if k in df.columns}
        )
        df["data_receita"] = df["data_receita"].str[:10]
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    for col in ["sigla_uf", "sigla_uf_doador"]:
        if col in result.columns:
            result[col] = _clean_br(result[col])

    nulo_cols = [
        "origem_receita",
        "natureza_receita",
        "descricao_receita",
        "cpf_cnpj_doador",
        "nome_doador",
        "nome_doador_rf",
        "esfera_partidaria_doador",
        "sigla_uf_doador",
        "id_municipio_tse_doador",
        "sequencial_candidato_doador",
        "numero_candidato_doador",
        "cargo_candidato_doador",
        "numero_partido_doador",
        "sigla_partido_doador",
        "numero_recibo_doacao",
        "numero_documento_doacao",
        "cnae_2_doador",
        "descricao_cnae_2_doador",
    ]
    result = _clean_nulo_cols(result, nulo_cols)

    clean_cols = [
        "tipo_eleicao",
        "tipo_prestacao_contas",
        "cargo",
        "fonte_receita",
        "origem_receita",
        "natureza_receita",
        "especie_receita",
        "descricao_cnae_2_doador",
        "cargo_candidato_doador",
        "esfera_partidaria_doador",
    ]
    for col in clean_cols:
        if col in result.columns:
            result[col] = clean_string_series(result[col])

    for col in ["data_receita", "data_prestacao_contas"]:
        if col in result.columns:
            result[col] = _parse_date_receipt(result[col])
    result["valor_receita"] = _clean_valor(result["valor_receita"])
    result["tipo_eleicao"] = clean_election_type_series(
        result["tipo_eleicao"], ano
    )
    for col in ["id_municipio_tse", "id_municipio_tse_doador"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def build_receitas(ano: int) -> pd.DataFrame:
    """Build receitas_candidato for a single year (2002-2024)."""
    builders = {
        2002: _build_receitas_2002,
        2004: _build_receitas_2004,
        2006: _build_receitas_2006,
        2008: _build_receitas_2008,
        2010: _build_receitas_2010,
        2012: _build_receitas_2012,
        2014: _build_receitas_2014,
        2016: _build_receitas_2016,
    }
    if ano in builders:
        df = builders[ano]()
    elif ano >= 2018:
        df = _build_receitas_2018_plus(ano)
    else:
        raise ValueError(f"Unsupported year for receitas: {ano}")

    # Common post-processing: parse data_eleicao and merge municipio
    df["data_eleicao"] = parse_date_br(df["data_eleicao"])
    if "id_municipio_tse" not in df.columns:
        df["id_municipio_tse"] = pd.NA
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_municipio_tse"] = (
        df["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    df = merge_municipio(df)
    df["ano"] = ano
    return df


# ---------------------------------------------------------------------------
# despesas_candidato
# ---------------------------------------------------------------------------


def _build_despesas_2002() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2002/2002/Candidato/Despesa/DespesaCandidato"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        ["v1", "v2", "v3", "v4", "v6", "v7", "v8", "v10", "v11", "v12"]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "sigla_uf",
        "sigla_partido",
        "cargo",
        "numero_candidato",
        "data_despesa",
        "cpf_cnpj_fornecedor",
        "nome_fornecedor",
        "valor_despesa",
        "tipo_despesa",
    ]
    df = _clean_nulo_cols(df, ["tipo_despesa", "cpf_cnpj_fornecedor"])
    for col in ["cargo", "tipo_despesa"]:
        df[col] = clean_string_series(df[col])
    df["numero_partido"] = df["numero_candidato"].str[:2]
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2004() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2004/2004/Candidato/Despesa/DespesaCandidato"
    )
    path = None
    for ext in [".csv", ".txt"]:
        p = base.with_suffix(ext)
        if p.exists():
            path = p
            break
    if path is None:
        raise FileNotFoundError("No file found for despesas 2004")

    df = pd.read_csv(
        path,
        sep=";",
        header=None,
        dtype=str,
        encoding="latin-1",
        keep_default_na=False,
        quotechar='"',
        on_bad_lines="warn",
        quoting=3,
    )
    df.columns = [f"v{i + 1}" for i in range(len(df.columns))]
    # Stata stripquotes(yes): remove embedded quotes from all fields
    for col in df.columns:
        df[col] = df[col].str.replace('"', "", regex=False)
    df = df.iloc[1:].reset_index(drop=True)
    df = df[
        [
            "v2",
            "v4",
            "v5",
            "v7",
            "v8",
            "v9",
            "v10",
            "v11",
            "v12",
            "v14",
            "v16",
            "v17",
            "v19",
            "v20",
        ]
    ].copy()
    df.columns = [
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "id_municipio_tse",
        "numero_partido",
        "sigla_partido",
        "valor_despesa",
        "data_despesa",
        "tipo_despesa",
        "especie_recurso",
        "numero_documento",
        "tipo_documento",
        "nome_fornecedor",
        "cpf_cnpj_fornecedor",
    ]
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "tipo_despesa",
            "cpf_cnpj_fornecedor",
        ],
    )
    for col in ["tipo_documento", "cargo", "tipo_despesa", "especie_recurso"]:
        df[col] = clean_string_series(df[col])
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2006() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2006/2006/Candidato/Despesa/DespesaCandidato"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        [
            "v1",
            "v3",
            "v5",
            "v6",
            "v7",
            "v8",
            "v9",
            "v10",
            "v11",
            "v12",
            "v14",
            "v16",
            "v17",
            "v19",
            "v20",
        ]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "cnpj_candidato",
        "numero_partido",
        "sigla_partido",
        "valor_despesa",
        "data_despesa",
        "tipo_despesa",
        "especie_recurso",
        "numero_documento",
        "tipo_documento",
        "nome_fornecedor",
        "cpf_cnpj_fornecedor",
    ]
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "tipo_despesa",
            "cpf_cnpj_fornecedor",
        ],
    )
    for col in ["tipo_documento", "cargo", "tipo_despesa", "especie_recurso"]:
        df[col] = clean_string_series(df[col])
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2008() -> pd.DataFrame:
    base = (
        INPUT_DIR
        / "prestacao_contas/prestacao_contas_2008/despesas_candidatos_2008_brasil"
    )
    df = read_raw_csv(str(base), drop_first_row=True)
    df = df[
        [
            "v1",
            "v3",
            "v5",
            "v6",
            "v8",
            "v9",
            "v10",
            "v11",
            "v12",
            "v13",
            "v14",
            "v16",
            "v18",
            "v19",
            "v21",
            "v22",
        ]
    ].copy()
    df.columns = [
        "sequencial_candidato",
        "cargo",
        "numero_candidato",
        "sigla_uf",
        "id_municipio_tse",
        "cnpj_candidato",
        "numero_partido",
        "sigla_partido",
        "valor_despesa",
        "data_despesa",
        "tipo_despesa",
        "especie_recurso",
        "numero_documento",
        "tipo_documento",
        "nome_fornecedor",
        "cpf_cnpj_fornecedor",
    ]
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "tipo_despesa",
            "cpf_cnpj_fornecedor",
        ],
    )
    for col in ["tipo_documento", "cargo", "tipo_despesa", "especie_recurso"]:
        df[col] = clean_string_series(df[col])
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2010() -> pd.DataFrame:
    # Note: 2010 despesas does NOT include DF in state list
    ufs = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "BR",
        "CE",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_contas_2010/candidato/{uf}/DespesasCandidatos"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v2",
                "v3",
                "v4",
                "v5",
                "v6",
                "v10",
                "v11",
                "v12",
                "v13",
                "v14",
                "v15",
                "v16",
                "v17",
                "v18",
                "v19",
            ]
        ].copy()
        df.columns = [
            "sequencial_candidato",
            "sigla_uf",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "tipo_documento",
            "numero_documento",
            "cpf_cnpj_fornecedor",
            "nome_fornecedor",
            "data_despesa",
            "valor_despesa",
            "tipo_despesa",
            "fonte_recurso",
            "especie_recurso",
            "descricao_despesa",
        ]
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "descricao_despesa",
            "cpf_cnpj_fornecedor",
        ],
    )
    for col in ["tipo_documento", "cargo", "tipo_despesa", "especie_recurso"]:
        df[col] = clean_string_series(df[col])
    df["numero_partido"] = df["numero_candidato"].str[:2]
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2012() -> pd.DataFrame:
    ufs = _UFS_MUN
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_final_2012/despesas_candidatos_2012_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v2",
                "v3",
                "v4",
                "v6",
                "v7",
                "v8",
                "v11",
                "v12",
                "v13",
                "v14",
                "v15",
                "v16",
                "v17",
                "v18",
                "v19",
                "v20",
            ]
        ].copy()
        df.columns = [
            "sequencial_candidato",
            "sigla_uf",
            "id_municipio_tse",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "tipo_documento",
            "numero_documento",
            "cpf_cnpj_fornecedor",
            "nome_fornecedor",
            "nome_fornecedor_rf",
            "descricao_cnae_2_fornecedor",
            "data_despesa",
            "valor_despesa",
            "tipo_despesa",
            "descricao_despesa",
        ]
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "descricao_despesa",
            "cpf_cnpj_fornecedor",
            "descricao_cnae_2_fornecedor",
        ],
    )
    for col in [
        "tipo_documento",
        "cargo",
        "descricao_cnae_2_fornecedor",
        "tipo_despesa",
    ]:
        df[col] = clean_string_series(df[col])
    df["numero_partido"] = df["numero_candidato"].str[:2]
    # 2012 special: prepend "20" if date is 8 chars
    df["data_despesa"] = df["data_despesa"].apply(
        lambda x: "20" + x if isinstance(x, str) and len(x) == 8 else x
    )
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_eleicao"] = ""
    df["tipo_eleicao"] = "eleicao ordinaria"
    df["data_eleicao"] = ""
    return df


def _build_despesas_2014() -> pd.DataFrame:
    ufs = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "BR",
        "CE",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_final_2014/despesas_candidatos_2014_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v1",
                "v2",
                "v4",
                "v5",
                "v6",
                "v7",
                "v8",
                "v9",
                "v12",
                "v13",
                "v14",
                "v15",
                "v16",
                "v17",
                "v18",
                "v19",
                "v20",
                "v21",
                "v22",
            ]
        ].copy()
        df.columns = [
            "id_eleicao",
            "tipo_eleicao",
            "cnpj_prestador_contas",
            "sequencial_candidato",
            "sigla_uf",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "tipo_documento",
            "numero_documento",
            "cpf_cnpj_fornecedor",
            "nome_fornecedor",
            "nome_fornecedor_rf",
            "cnae_2_fornecedor",
            "descricao_cnae_2_fornecedor",
            "data_despesa",
            "valor_despesa",
            "tipo_despesa",
            "descricao_despesa",
        ]
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "descricao_despesa",
            "cpf_cnpj_fornecedor",
            "cnae_2_fornecedor",
            "descricao_cnae_2_fornecedor",
        ],
    )
    for col in [
        "tipo_eleicao",
        "tipo_documento",
        "cargo",
        "descricao_cnae_2_fornecedor",
        "tipo_despesa",
    ]:
        df[col] = clean_string_series(df[col])
    df["numero_partido"] = df["numero_candidato"].str[:2]
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["tipo_eleicao"] = clean_election_type_series(df["tipo_eleicao"], 2014)
    df["data_eleicao"] = ""
    return df


def _build_despesas_2016() -> pd.DataFrame:
    ufs = _UFS_MUN
    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_contas_final_2016/despesas_candidatos_prestacao_contas_final_2016_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)
        df = df[
            [
                "v1",
                "v2",
                "v4",
                "v5",
                "v6",
                "v7",
                "v9",
                "v10",
                "v11",
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
            ]
        ].copy()
        df.columns = [
            "id_eleicao",
            "tipo_eleicao",
            "cnpj_prestador_contas",
            "sequencial_candidato",
            "sigla_uf",
            "id_municipio_tse",
            "sigla_partido",
            "numero_candidato",
            "cargo",
            "tipo_documento",
            "numero_documento",
            "cpf_cnpj_fornecedor",
            "nome_fornecedor",
            "nome_fornecedor_rf",
            "cnae_2_fornecedor",
            "descricao_cnae_2_fornecedor",
            "data_despesa",
            "valor_despesa",
            "tipo_despesa",
            "descricao_despesa",
        ]
        df["data_eleicao"] = ""
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df = _clean_nulo_cols(
        df,
        [
            "tipo_documento",
            "numero_documento",
            "descricao_despesa",
            "cpf_cnpj_fornecedor",
            "cnae_2_fornecedor",
            "descricao_cnae_2_fornecedor",
        ],
    )
    for col in [
        "tipo_eleicao",
        "tipo_documento",
        "cargo",
        "descricao_cnae_2_fornecedor",
        "tipo_despesa",
    ]:
        df[col] = clean_string_series(df[col])
    df["numero_partido"] = df["numero_candidato"].str[:2]
    df["data_despesa"] = _parse_date_receipt(df["data_despesa"])
    df["valor_despesa"] = _clean_valor(df["valor_despesa"])
    df["tipo_eleicao"] = clean_election_type_series(df["tipo_eleicao"], 2016)
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    return df


def _build_despesas_2018_plus(ano: int) -> pd.DataFrame:
    """Build despesas for 2018-2024."""
    if ano == 2018:
        ufs = _UFS_FED_BR
    elif ano == 2020:
        ufs = _UFS_MUN
    elif ano == 2022:
        ufs = _UFS_FED_BR
    elif ano == 2024:
        ufs = _UFS_MUN
    else:
        ufs = _UFS_FED_BR if ano % 4 == 2 else _UFS_MUN

    frames = []
    for uf in ufs:
        base = (
            INPUT_DIR
            / f"prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}/despesas_contratadas_candidatos_{ano}_{uf}"
        )
        df = read_raw_csv(str(base), drop_first_row=True)

        keep = [
            "v6",
            "v7",
            "v8",
            "v9",
            "v10",
            "v11",
            "v12",
            "v13",
            "v14",
            "v16",
            "v18",
            "v19",
            "v20",
            "v24",
            "v25",
            "v28",
            "v29",
            "v30",
            "v31",
            "v32",
            "v33",
            "v35",
            "v36",
            "v37",
            "v39",
            "v40",
            "v42",
            "v43",
            "v44",
            "v46",
            "v47",
            "v49",
            "v50",
            "v51",
            "v52",
            "v53",
        ]
        available = [c for c in keep if c in df.columns]
        df = df[available].copy()

        col_map = {
            "v6": "id_eleicao",
            "v7": "tipo_eleicao",
            "v8": "data_eleicao",
            "v9": "turno",
            "v10": "tipo_prestacao_contas",
            "v11": "data_prestacao_contas",
            "v12": "sequencial_prestador_contas",
            "v13": "sigla_uf",
            "v14": "id_municipio_tse",
            "v16": "cnpj_prestador_contas",
            "v18": "cargo",
            "v19": "sequencial_candidato",
            "v20": "numero_candidato",
            "v24": "numero_partido",
            "v25": "sigla_partido",
            "v28": "tipo_fornecedor",
            "v29": "cnae_2_fornecedor",
            "v30": "descricao_cnae_2_fornecedor",
            "v31": "cpf_cnpj_fornecedor",
            "v32": "nome_fornecedor",
            "v33": "nome_fornecedor_rf",
            "v35": "esfera_partidaria_fornecedor",
            "v36": "sigla_uf_fornecedor",
            "v37": "id_municipio_tse_fornecedor",
            "v39": "sequencial_candidato_fornecedor",
            "v40": "numero_candidato_fornecedor",
            "v42": "cargo_fornecedor",
            "v43": "numero_partido_fornecedor",
            "v44": "sigla_partido_fornecedor",
            "v46": "tipo_documento",
            "v47": "numero_documento",
            "v49": "origem_despesa",
            "v50": "sequencial_despesa",
            "v51": "data_despesa",
            "v52": "descricao_despesa",
            "v53": "valor_despesa",
        }
        df = df.rename(
            columns={k: v for k, v in col_map.items() if k in df.columns}
        )
        frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    nulo_cols = [
        "cpf_cnpj_fornecedor",
        "tipo_fornecedor",
        "cnae_2_fornecedor",
        "descricao_cnae_2_fornecedor",
        "nome_fornecedor",
        "nome_fornecedor_rf",
        "esfera_partidaria_fornecedor",
        "sigla_uf_fornecedor",
        "id_municipio_tse_fornecedor",
        "sequencial_candidato_fornecedor",
        "numero_candidato_fornecedor",
        "cargo_fornecedor",
        "numero_partido_fornecedor",
        "sigla_partido_fornecedor",
        "tipo_documento",
        "numero_documento",
        "descricao_despesa",
    ]
    result = _clean_nulo_cols(result, nulo_cols)

    clean_cols = [
        "tipo_eleicao",
        "cargo",
        "tipo_fornecedor",
        "origem_despesa",
        "tipo_documento",
        "tipo_prestacao_contas",
        "descricao_cnae_2_fornecedor",
    ]
    for col in clean_cols:
        if col in result.columns:
            result[col] = clean_string_series(result[col])

    for col in ["data_despesa", "data_prestacao_contas"]:
        if col in result.columns:
            result[col] = _parse_date_receipt(result[col])
    result["valor_despesa"] = _clean_valor(result["valor_despesa"])
    result["tipo_eleicao"] = clean_election_type_series(
        result["tipo_eleicao"], ano
    )
    if "turno" in result.columns:
        result["turno"] = pd.to_numeric(result["turno"], errors="coerce")
    for col in ["id_municipio_tse", "id_municipio_tse_fornecedor"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def build_despesas(ano: int) -> pd.DataFrame:
    """Build despesas_candidato for a single year (2002-2024)."""
    builders = {
        2002: _build_despesas_2002,
        2004: _build_despesas_2004,
        2006: _build_despesas_2006,
        2008: _build_despesas_2008,
        2010: _build_despesas_2010,
        2012: _build_despesas_2012,
        2014: _build_despesas_2014,
        2016: _build_despesas_2016,
    }
    if ano in builders:
        df = builders[ano]()
    elif ano >= 2018:
        df = _build_despesas_2018_plus(ano)
    else:
        raise ValueError(f"Unsupported year for despesas: {ano}")

    # Common post-processing
    df["sigla_uf"] = _clean_br(df["sigla_uf"])
    df["data_eleicao"] = parse_date_br(df["data_eleicao"])
    if "id_municipio_tse" not in df.columns:
        df["id_municipio_tse"] = pd.NA
    df["id_municipio_tse"] = pd.to_numeric(
        df["id_municipio_tse"], errors="coerce"
    )
    df["id_municipio_tse"] = (
        df["id_municipio_tse"].astype("Int64").astype(str).replace("<NA>", "")
    )
    df = merge_municipio(df)
    df["ano"] = ano
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_all():
    """Build all campaign finance tables."""
    # bens
    for ano in range(2006, 2025, 2):
        print(f"  bens_candidato {ano}")
        df = build_bens(ano)
        out = OUTPUT_PYTHON / f"bens_candidato_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)

    # receitas
    for ano in range(2002, 2025, 2):
        print(f"  receitas_candidato {ano}")
        df = build_receitas(ano)
        out = OUTPUT_PYTHON / f"receitas_candidato_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)

    # despesas
    for ano in range(2002, 2025, 2):
        print(f"  despesas_candidato {ano}")
        df = build_despesas(ano)
        out = OUTPUT_PYTHON / f"despesas_candidato_{ano}.parquet"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False)


if __name__ == "__main__":
    build_all()
