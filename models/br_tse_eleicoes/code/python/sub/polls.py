"""
Build: pesquisas eleitorais (electoral poll registrations).

Three tables from the TSE "pesquisas eleitorais" open-data packages:

- pesquisa_eleitoral              one row per registered poll
- pesquisa_eleitoral_contratante  one row per poll x contracting party
- pesquisa_eleitoral_pagante      one row per poll x contracting party x payer

Each yearly zip carries per-UF CSVs plus a ``_BRASIL.csv``; neither is complete
on its own (2026 has BR/DF only in BRASIL, 2016/2018 have no BRASIL, the 2022
and 2024 pagante BRASIL files are empty). Every CSV in the zip is therefore
read, concatenated, and exact duplicates are dropped (ignoring the generation
timestamp). The questionnaire, neighbourhood and invoice families are PDF
attachments and are not tabulated here.

See ``models/br_tse_eleicoes/code/PESQUISAS_PROPOSAL.md`` for the research
behind every rule below.

Usage:
    TSE_DATA_DIR=... uv run -m models.br_tse_eleicoes.code.python.sub.polls [--download] [YEAR ...]
"""

import re
import shutil
import sys
import urllib.request
import zipfile

import pandas as pd

from models.br_tse_eleicoes.code.python.config import INPUT_DIR, OUTPUT_PYTHON
from models.br_tse_eleicoes.code.python.utils.clean_election_type import (
    clean_election_type_series,
)
from models.br_tse_eleicoes.code.python.utils.clean_string import (
    clean_string,
    clean_string_series,
)
from models.br_tse_eleicoes.code.python.utils.helpers import (
    merge_municipio,
    save_partitioned,
)

YEARS = list(range(2012, 2027, 2))
POLL_DIR = INPUT_DIR / "pesquisa_eleitoral"
CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele/pesquisa_eleitoral"
FAMILIES = ["pesquisa_eleitoral", "pesquisa_contratante", "pesquisa_pagante"]

TABLE_MAIN = "pesquisa_eleitoral"
TABLE_CONTRATANTE = "pesquisa_eleitoral_contratante"
TABLE_PAGANTE = "pesquisa_eleitoral_pagante"

# Architecture column order (ano first; it becomes the Hive partition).
COLUMNS = {
    TABLE_MAIN: [
        "ano",
        "sigla_uf",
        "id_municipio",
        "id_municipio_tse",
        "id_eleicao",
        "tipo_eleicao",
        "id_pesquisa",
        "cnpj_empresa",
        "nome_empresa",
        "nome_fantasia_empresa",
        "pesquisa_propria",
        "cargos",
        "data_registro",
        "hora_registro",
        "data_inicio",
        "data_fim",
        "data_divulgacao",
        "quantidade_entrevistados",
        "valor_pesquisa",
        "registro_conre_estatistico",
        "nome_estatistico",
        "descricao_metodologia",
        "descricao_plano_amostral",
        "descricao_sistema_controle",
        "descricao_area_abrangencia",
    ],
    TABLE_CONTRATANTE: [
        "ano",
        "sigla_uf",
        "id_pesquisa",
        "id_contratante",
        "cpf_cnpj_contratante",
        "nome_contratante",
        "contratante_pagante",
        "valor_pago",
        "origem_recurso",
    ],
    TABLE_PAGANTE: [
        "ano",
        "sigla_uf",
        "id_pesquisa",
        "id_contratante",
        "cpf_cnpj_pagante",
        "nome_pagante",
        "origem_recurso",
    ],
}

# Source → target names. 2016/2018 spell two columns differently.
RENAME = {
    TABLE_MAIN: {
        "AA_ELEICAO": "ano",
        "SG_UF": "sigla_uf",
        "SG_UE": "id_municipio_tse",
        "CD_ELEICAO": "id_eleicao",
        "NM_ELEICAO": "tipo_eleicao",
        "NR_PROTOCOLO_REGISTRO": "id_pesquisa",
        "NR_CNPJ_EMPRESA": "cnpj_empresa",
        "NM_EMPRESA": "nome_empresa",
        "NM_EMPRESA_FANTASIA": "nome_fantasia_empresa",
        "ST_PESQUISA_PROPRIA": "pesquisa_propria",
        "DS_CARGO": "cargos",
        "DS_CARGOS": "cargos",
        "DT_REGISTRO": "data_registro",
        "DT_INICIO_PESQUISA": "data_inicio",
        "DT_FIM_PESQUISA": "data_fim",
        "DT_DIVULGACAO": "data_divulgacao",
        "QT_ENTREVISTADO": "quantidade_entrevistados",
        "QT_ENTREVISTADOS": "quantidade_entrevistados",
        "CD_CONRE": "registro_conre_estatistico",
        "NM_ESTATISTICO_RESP": "nome_estatistico",
        "VR_PESQUISA": "valor_pesquisa",
        "DS_METODOLOGIA_PESQUISA": "descricao_metodologia",
        "DS_PLANO_AMOSTRAL": "descricao_plano_amostral",
        "DS_SISTEMA_CONTROLE": "descricao_sistema_controle",
        "DS_DADO_MUNICIPIO": "descricao_area_abrangencia",
    },
    TABLE_CONTRATANTE: {
        "AA_ELEICAO": "ano",
        "NR_PROTOCOLO_REGISTRO": "id_pesquisa",
        "CD_CONTRATANTE": "id_contratante",
        "NR_CPF_CNPJ_CONTRATANTE": "cpf_cnpj_contratante",
        "NM_CONTRATANTE": "nome_contratante",
        "VR_PAGO_CONTRATANTE": "valor_pago",
        "ST_CONTRATANTE_PAGANTE": "contratante_pagante",
        "DS_ORIGEM_RECURSO": "origem_recurso",
    },
    TABLE_PAGANTE: {
        "AA_ELEICAO": "ano",
        "NR_PROTOCOLO_REGISTRO": "id_pesquisa",
        "CD_CONTRATANTE": "id_contratante",
        "NR_CPF_CNPJ_PAGANTE": "cpf_cnpj_pagante",
        "NM_PAGANTE": "nome_pagante",
        "DS_ORIGEM_RECURSO": "origem_recurso",
    },
}
FAMILY_TABLE = {
    "pesquisa_eleitoral": TABLE_MAIN,
    "pesquisa_contratante": TABLE_CONTRATANTE,
    "pesquisa_pagante": TABLE_PAGANTE,
}

# Text sentinels (leiame: #NULO = blank, #NE = not collected that year).
TEXT_SENTINELS = {"", "#NULO#", "#NULO", "#NE#", "#NE"}
# Numeric sentinels (#NULO -> -1, #NE -> -3); only applied to codes,
# documents and amounts, never to free text.
NUMERIC_SENTINELS = TEXT_SENTINELS | {"-1", "-3", "-4"}

CARGO_ORDER = [
    "presidente",
    "governador",
    "senador",
    "deputado federal",
    "deputado estadual",
    "deputado distrital",
    "prefeito",
    "vereador",
]

LONG_TEXT = [
    "descricao_metodologia",
    "descricao_plano_amostral",
    "descricao_sistema_controle",
    "descricao_area_abrangencia",
]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def download(years: list[int] = YEARS, force: bool = False) -> None:
    """Download the three tabular zips per year into INPUT_DIR/pesquisa_eleitoral."""
    POLL_DIR.mkdir(parents=True, exist_ok=True)
    for ano in years:
        for fam in FAMILIES:
            dest = POLL_DIR / f"{fam}_{ano}.zip"
            if dest.exists() and dest.stat().st_size > 0 and not force:
                continue
            url = f"{CDN}/{fam}_{ano}.zip"
            tmp = dest.with_suffix(".zip.part")
            with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
                shutil.copyfileobj(r, f)
            tmp.rename(dest)
            print(f"downloaded {dest.name}")


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_family(fam: str, ano: int) -> pd.DataFrame:
    """Read every CSV in the zip, concatenate, drop exact duplicates.

    Duplicates are judged ignoring DT_GERACAO/HH_GERACAO, so a row present
    in both a UF file and the BRASIL file counts once.
    """
    path = POLL_DIR / f"{fam}_{ano}.zip"
    parts = []
    with zipfile.ZipFile(path) as z:
        for name in sorted(z.namelist()):
            if not name.lower().endswith(".csv"):
                continue
            with z.open(name) as fh:
                df = pd.read_csv(
                    fh,
                    sep=";",
                    encoding="latin-1",
                    dtype=str,
                    keep_default_na=False,
                )
            parts.append(df)
    df = pd.concat(parts, ignore_index=True)
    df = df.drop(columns=["DT_GERACAO", "HH_GERACAO"], errors="ignore")
    return df.drop_duplicates(ignore_index=True)


# ---------------------------------------------------------------------------
# Pure cleaning helpers (Series -> Series; NULL is None)
# ---------------------------------------------------------------------------


def _null_text(s: pd.Series) -> pd.Series:
    s = s.str.strip()
    return s.where(~s.isin(TEXT_SENTINELS), None)


def _null_code(s: pd.Series) -> pd.Series:
    s = s.str.strip()
    return s.where(~s.isin(NUMERIC_SENTINELS), None)


def clean_document(s: pd.Series) -> pd.Series:
    """CPF/CNPJ: digits only; sentinels and underscore masks become NULL."""
    s = _null_code(s)
    return s.where(s.str.fullmatch(r"\d{11}|\d{14}", na=False), None)


def clean_flag(s: pd.Series) -> pd.Series:
    """S/N flags; anything else (including #NE) becomes NULL."""
    s = s.str.strip().str.upper()
    return s.where(s.isin({"S", "N"}), None)


def clean_money(s: pd.Series) -> pd.Series:
    """'46204,00' -> '46204.00' by string manipulation (no float rounding)."""
    s = _null_code(s)
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return s.where(s.str.fullmatch(r"-?\d+(\.\d+)?", na=False), None)


def clean_count(s: pd.Series) -> pd.Series:
    """Non-negative integer counts; negatives (4 rows) and junk become NULL."""
    s = s.str.strip()
    return s.where(s.str.fullmatch(r"\d+", na=False), None).map(
        lambda v: str(int(v)) if v is not None else None
    )


_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}:\d{2}:\d{2}))?$")
_BR = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _split_datetime(val: str | None) -> tuple[str | None, str | None]:
    if not isinstance(val, str):
        return None, None
    val = val.strip()
    m = _ISO.match(val)
    if m:
        return f"{m[1]}-{m[2]}-{m[3]}", m[4]
    m = _BR.match(val)
    if m:
        return f"{m[3]}-{m[2]}-{m[1]}", None
    return None, None


def clean_date(s: pd.Series) -> pd.Series:
    """'YYYY-MM-DD HH:MM:SS' or 'DD/MM/YYYY' -> 'YYYY-MM-DD'."""
    return s.map(lambda v: _split_datetime(v)[0])


def null_implausible_date(s: pd.Series, ref: pd.Series) -> pd.Series:
    """NULL a fieldwork date more than one year away from the registration.

    The 2012 file holds typed-in years such as 0002, 0201, 2101, 2912 and
    8201 (≈90 cells). Registration dates are system-generated and clean, so
    they anchor the check; supplementary polls registered years after the
    ordinary election are therefore not affected.
    """
    y = pd.to_numeric(s.str.slice(0, 4), errors="coerce")
    r = pd.to_numeric(ref.str.slice(0, 4), errors="coerce")
    return s.where(~((y - r).abs() > 1), None)


def clean_time(s: pd.Series) -> pd.Series:
    """Time part of an ISO timestamp, or NULL when the source has none."""
    return s.map(lambda v: _split_datetime(v)[1])


def clean_long_text(s: pd.Series) -> pd.Series:
    """Free text kept verbatim apart from CRLF -> LF and outer whitespace."""
    s = s.str.replace("\r\n", "\n", regex=False).str.replace(
        "\r", "\n", regex=False
    )
    return _null_text(s)


def clean_cargos(s: pd.Series) -> pd.Series:
    """'Vereador, Prefeito' -> 'prefeito, vereador' (dedupe, fixed order)."""

    def _one(val):
        if not isinstance(val, str):
            return None
        items = {
            clean_string(x.strip())
            for x in val.split(",")
            if x.strip() and x.strip() not in TEXT_SENTINELS
        }
        if not items:
            return None
        rank = {c: i for i, c in enumerate(CARGO_ORDER)}
        return ", ".join(sorted(items, key=lambda c: (rank.get(c, 99), c)))

    return s.map(_one)


def clean_categorical(s: pd.Series) -> pd.Series:
    s = clean_string_series(_null_text(s))
    return s.where(s.notna() & (s != ""), None)


def clean_municipio_tse(s: pd.Series) -> pd.Series:
    """SG_UE: 5-digit TSE code (municipal polls) without leading zeros, as
    in the directory and the other tables; UF/BR codes become NULL."""
    s = s.str.strip()
    return s.where(s.str.fullmatch(r"\d{1,5}", na=False), None).map(
        lambda v: str(int(v)) if v is not None else None
    )


def uf_from_protocol(s: pd.Series) -> pd.Series:
    """Protocols look like 'RN022772026': UF (or BR) + sequence + year."""
    return s.str.slice(0, 2)


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------


def build_main(raw: pd.DataFrame, ano: int) -> pd.DataFrame:
    df = raw.rename(columns=RENAME[TABLE_MAIN])
    # 2016/2018 embed contracting-party and payer columns; they live in the
    # child tables, so drop them and collapse the poll x party fan-out.
    df = df[[c for c in df.columns if c in COLUMNS[TABLE_MAIN]]]
    df = df.drop_duplicates(ignore_index=True)

    out = pd.DataFrame(index=df.index)
    out["ano"] = str(ano)
    out["sigla_uf"] = _null_text(df["sigla_uf"])
    out["id_municipio_tse"] = clean_municipio_tse(df["id_municipio_tse"])
    out["id_eleicao"] = _null_code(df["id_eleicao"])
    out["tipo_eleicao"] = clean_election_type_series(
        clean_categorical(df["tipo_eleicao"]), ano
    )
    out["id_pesquisa"] = _null_text(df["id_pesquisa"])
    out["cnpj_empresa"] = clean_document(df["cnpj_empresa"])
    out["nome_empresa"] = _null_text(df["nome_empresa"])
    out["nome_fantasia_empresa"] = _null_text(df["nome_fantasia_empresa"])
    out["pesquisa_propria"] = (
        clean_flag(df["pesquisa_propria"])
        if "pesquisa_propria" in df
        else None
    )
    out["cargos"] = clean_cargos(df["cargos"])
    out["data_registro"] = clean_date(df["data_registro"])
    # Registration times are real only from 2022 (2020: 31 rows); earlier
    # files carry 00:00:00 or no time at all.
    hora = clean_time(df["data_registro"])
    if ano < 2022:
        hora = hora.where(hora != "00:00:00", None)
    out["hora_registro"] = hora
    out["data_inicio"] = null_implausible_date(
        clean_date(df["data_inicio"]), out["data_registro"]
    )
    out["data_fim"] = null_implausible_date(
        clean_date(df["data_fim"]), out["data_registro"]
    )
    out["data_divulgacao"] = (
        clean_date(df["data_divulgacao"]) if "data_divulgacao" in df else None
    )
    out["quantidade_entrevistados"] = clean_count(
        df["quantidade_entrevistados"]
    )
    out["valor_pesquisa"] = clean_money(df["valor_pesquisa"])
    out["registro_conre_estatistico"] = _null_text(
        df["registro_conre_estatistico"]
    )
    out["nome_estatistico"] = _null_text(df["nome_estatistico"])
    for col in LONG_TEXT:
        out[col] = clean_long_text(df[col])

    out["id_municipio_tse"] = out["id_municipio_tse"].fillna("")
    out = merge_municipio(out)
    for col in ["id_municipio", "id_municipio_tse"]:
        out[col] = out[col].where(out[col].notna() & (out[col] != ""), None)
    return out[COLUMNS[TABLE_MAIN]]


def build_contratante(raw: pd.DataFrame, ano: int) -> pd.DataFrame:
    df = raw.rename(columns=RENAME[TABLE_CONTRATANTE])
    out = pd.DataFrame(index=df.index)
    out["ano"] = str(ano)
    out["id_pesquisa"] = _null_text(df["id_pesquisa"])
    out["sigla_uf"] = uf_from_protocol(out["id_pesquisa"])
    out["id_contratante"] = _null_code(df["id_contratante"])
    out["cpf_cnpj_contratante"] = clean_document(df["cpf_cnpj_contratante"])
    out["nome_contratante"] = _null_text(df["nome_contratante"])
    out["contratante_pagante"] = clean_flag(df["contratante_pagante"])
    out["valor_pago"] = clean_money(df["valor_pago"])
    out["origem_recurso"] = clean_categorical(df["origem_recurso"])
    return out[COLUMNS[TABLE_CONTRATANTE]].drop_duplicates(ignore_index=True)


def build_pagante(raw: pd.DataFrame, ano: int) -> pd.DataFrame:
    df = raw.rename(columns=RENAME[TABLE_PAGANTE])
    out = pd.DataFrame(index=df.index)
    out["ano"] = str(ano)
    out["id_pesquisa"] = _null_text(df["id_pesquisa"])
    out["sigla_uf"] = uf_from_protocol(out["id_pesquisa"])
    out["id_contratante"] = _null_code(df["id_contratante"])
    out["cpf_cnpj_pagante"] = clean_document(df["cpf_cnpj_pagante"])
    out["nome_pagante"] = _null_text(df["nome_pagante"])
    out["origem_recurso"] = clean_categorical(df["origem_recurso"])
    return out[COLUMNS[TABLE_PAGANTE]].drop_duplicates(ignore_index=True)


BUILDERS = {
    "pesquisa_eleitoral": build_main,
    "pesquisa_contratante": build_contratante,
    "pesquisa_pagante": build_pagante,
}


def build_year(ano: int) -> dict[str, pd.DataFrame]:
    return {
        FAMILY_TABLE[fam]: BUILDERS[fam](read_family(fam, ano), ano)
        for fam in FAMILIES
    }


def write_table(df: pd.DataFrame, table: str, ano: int) -> None:
    """Hive-partitioned CSV, ano in the path only, NULL as empty string."""
    dest = OUTPUT_PYTHON / table / f"ano={ano}"
    if dest.exists():
        shutil.rmtree(dest)
    # Every column is already str or None; to_csv writes None as "".
    save_partitioned(df, table, ["ano"], OUTPUT_PYTHON)


def build_all(years: list[int] = YEARS) -> dict[tuple[str, int], int]:
    counts = {}
    for ano in years:
        for table, df in build_year(ano).items():
            write_table(df, table, ano)
            counts[(table, ano)] = len(df)
            print(f"{table} ano={ano}: {len(df):,} rows")
    return counts


if __name__ == "__main__":
    args = sys.argv[1:]
    do_download = "--download" in args
    years = [int(a) for a in args if a.isdigit()] or YEARS
    if do_download:
        download(years)
    build_all(years)
