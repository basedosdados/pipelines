"""Convert the TCE-MG source packages to all-STRING staging parquet.

Four mirrors, one parquet per municipality per exercise, named exactly as the 27,294
objects already in `gs://basedosdados/staging/world_wb_mides/` are named:

    raw_empenho_mg/empenho_<year>_<ibge7>.parquet
    raw_liquidacao_mg/liquidacao_<year>_<ibge7>.parquet
    raw_pagamento_mg/pagamento_<year>_<ibge7>.parquet
    raw_rsp_mg/rsp_<year>_<ibge7>.parquet

WHERE THE COLUMN NAMES COME FROM.

The staging column names are NOT the source CSV names. The original (never-ported) MiDES
ingest renamed them -- `seq_empenho` became `id_empenho`, `vlr_anulempenho` became
`valor_anulacao` -- and the dbt models read the renamed ones. `SPECS` below is that
rename map, reconstructed by reading the actual staging parquet schemas rather than by
inference, and it reproduces each mirror's column set, order and STRING type exactly.

The map turned out to have one governing rule, which is why it can be trusted: the
staging layout is *the source CSV order, with a few columns dropped, renames applied in
place, and the synthesised columns appended*. Applying that rule to the September 2026
source headers reproduces the 2021 staging order for all four mirrors, field for field.

FOUR THINGS THAT ARE EASY TO GET WRONG AND SILENT WHEN WRONG.

*   **`id_municipio` for liquidação, pagamento and restos a pagar is in the ZIP FILE
    NAME, not in any column.** Those three CSVs carry no municipality field at all. Lose
    it and three of four mirrors lose the municipality key -- which the dbt models
    `concat` into `id_empenho_bd`, the join key of the entire MiDES pipeline. Empenho
    does publish `cod_municipio`; it is cross-checked against the file name and the file
    name wins, because the file name is what the object naming is built from.
*   **Dates are `YYYYMMDD` at source and ISO `YYYY-MM-DD` in staging.** Verified: every
    `data`, `dat_empenho`, `dat_liquidacao` and `data_origem` value in the staging
    parquet matches `^\\d{4}-\\d{2}-\\d{2}$`. The dbt models apply a bare
    `safe_cast(data as date)`, and BigQuery's cast of `'20211217'` is NULL -- so a
    verbatim reload would empty `data` and `mes` for every row and still pass every test.
*   **The empenho value columns are float-normalised; the other three mirrors' are
    verbatim.** Staging holds `'3840.0'` for empenho and `' 0000000230.3800'` for
    liquidação, pagamento and restos a pagar -- the old ingest parsed one and not the
    others. Reproduced here rather than harmonised, so the column does not change shape
    halfway through its history. The normalisation is idempotent either way: the source
    could publish either form and `str(float(...))` yields the same result.
*   **Empty means NULL, not the empty string.** Verified in staging: `dat_liquidacao`
    carries genuine nulls and no column anywhere carries `''`. Arrow's CSV reader is
    given `null_values=['']` explicitly -- the default list also swallows `NA`, `null`
    and `None`, which are legitimate values in a free-text Brazilian name field.

Values are otherwise byte-faithful. Nothing is trimmed: `elemento_despesa` carries
trailing spaces and the value columns carry a leading one, and the dbt models slice those
strings by position (`left(elemento_despesa, 12)`, `substring(dsc_modalidade, 5, 1)`).

Staging is all-STRING by house convention and must be: the recurring-pipeline upload path
stringifies its header, so a typed external table left by onboarding collides with the
pipeline's later overwrite. See .claude/rules/prefect-pipeline-conventions.md.

Usage:
    uv run python models/world_wb_mides/code/clean_mg.py [--year 2022]
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import time
import zipfile
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]  # sibling module via sys.path
from constants import INPUT_DIR, OUTPUT_DIR

MG_INPUT = INPUT_DIR / "mg"

# How a column in the staging mirror is produced.
COPY = "copy"  # verbatim from the named source column
DATE = "date"  # YYYYMMDD -> YYYY-MM-DD
FLOATSTR = "floatstr"  # str(float(x)); empenho only, see the module docstring
MUNI = "muni"  # the IBGE-7 code parsed out of the zip file name
CONST = "const"  # a value the source does not publish at all

# (staging column, kind, source column or constant).
#
# Order IS the staging order and must not be reshuffled. Verified against
# gs://basedosdados/staging/world_wb_mides/<mirror>/<phase>_2021_3100104.parquet.
SPECS: dict[str, list[tuple[str, str, str | None]]] = {
    "empenho": [
        ("id_empenho", COPY, "seq_empenho"),
        # Cross-checked against `cod_municipio`; the file name is authoritative.
        ("id_municipio", MUNI, "cod_municipio"),
        # `orgao` IS `cod_orgao`, NOT `seq_orgao`, in all four phases. Deliberate
        # decision, 2026-09-23; do not "fix" it back without reading this.
        #
        # The dbt models build the pipeline's join key as
        #     concat(id_empenho, ' ', orgao, ' ', id_municipio, ' ', right(ano, 2))
        # so this choice decides `id_empenho_bd` for every MG row.
        #
        # Published MiDES uses `seq_orgao` (verified: 2021, Abaete 3100104,
        # seq_empenho 70395270, source seq_orgao='7' / cod_orgao='02'; prod key is
        # '70395270 7 3100104 21'). Matching it was the reason this file used
        # `seq_orgao`. That reason no longer holds:
        #
        #   `id_empenho` comes from `seq_empenho`, and TCE-MG REGENERATES
        #   `seq_empenho` between extractions. Measured on MG 2021 empenho, the
        #   fresh harvest overlaps published prod on only 2,941,850 of 5,076,760
        #   keys (58%). Re-joining the 2,134,910 prod-only keys on a natural key
        #   (id_municipio|orgao|numero|data|valor_inicial) recovers 2,120,604 of
        #   them -- 99.3%. Same records, new sequence numbers.
        #
        # So `id_empenho_bd` is NOT stable across vintages no matter what `orgao`
        # holds, and keeping `seq_orgao` bought compatibility that does not exist.
        # Given that, `cod_orgao` is the better field on its merits: it is the
        # administrative organ code, whereas every `seq_*` column is a portal
        # sequence id of exactly the kind the source has been shown to regenerate.
        #
        # CONSEQUENCE, and it is not optional: the municipalities TCE-MG withdrew
        # from 2017 and 2018 cannot be built on this key, because the portal no
        # longer serves their source rows and `cod_orgao` exists only in the
        # source. Their published-vintage parquet files must be DELETED from the
        # staging bucket, not overlaid -- 1,002 objects (2017: 142 empenho-derived
        # / 157 despesa-derived; 2018: 101 each). Leaving them would put two key
        # schemes inside one exercise and give `orgao` two different meanings in
        # the same column. See upload_mg.py for why the overlay otherwise never
        # deletes.
        ("orgao", COPY, "cod_orgao"),
        ("id_unidade_gestora", COPY, "seq_unidade"),
        ("cod_unidade", COPY, "cod_unidade"),
        ("cod_subunidade", COPY, "cod_subunidade"),
        ("ano", COPY, "num_anoexercicio"),
        ("mes", COPY, "num_mesexercicio"),
        ("numero_empenho", COPY, "num_empenho"),
        ("data", DATE, "dat_empenho"),
        ("dsc_modalidade", COPY, "dsc_modalidade"),
        ("dsc_tipo_empenho", COPY, "dsc_tipo_empenho"),
        ("descricao", COPY, "dsc_empenho"),
        ("ind_dec_contrato", COPY, "ind_dec_contrato"),
        ("ind_dec_convenio", COPY, "ind_dec_convenio"),
        ("ind_dec_licitacao", COPY, "ind_dec_licitacao"),
        ("ind_dec_instr_conge", COPY, "ind_dec_instr_conge"),
        ("seq_contrato", COPY, "seq_contrato"),
        ("seq_termo_aditivo", COPY, "seq_termo_aditivo"),
        ("seq_convenio", COPY, "seq_convenio"),
        ("id_licitacao", COPY, "seq_licitacao"),
        ("seq_dispensa", COPY, "seq_dispensa"),
        ("seq_instr_conge", COPY, "seq_instr_conge"),
        ("dsc_dotacao", COPY, "dsc_dotacao"),
        ("dsc_funcao", COPY, "dsc_funcao"),
        ("dsc_subfuncao", COPY, "dsc_subfuncao"),
        ("dsc_programa", COPY, "dsc_programa"),
        # `dsc_subacao` is dropped between here and `dsc_naturezadespesa`.
        ("dsc_acao", COPY, "dsc_acao"),
        ("elemento_despesa", COPY, "dsc_naturezadespesa"),
        ("valor_empenho_original", FLOATSTR, "vlr_empenhado"),
        ("valor_reforco", FLOATSTR, "vlr_reforco"),
        ("valor_anulacao", FLOATSTR, "vlr_anulempenho"),
        # Present in all four staging mirrors, in no source CSV in any exercise checked.
        # Constant '1' wherever it was sampled. No dbt model reads it.
        ("num_versao_arq", CONST, "1"),
    ],
    "liquidacao": [
        ("id_liquidacao", COPY, "seq_liquidacao"),
        ("orgao", COPY, "cod_orgao"),
        ("id_unidade_gestora", COPY, "seq_unidade"),
        ("id_empenho", COPY, "seq_empenho"),
        ("id_rsp", COPY, "seq_rsp"),
        ("ano", COPY, "num_ano_referencia"),
        ("mes", COPY, "num_mes_referencia"),
        ("numero_liquidacao", COPY, "num_liquidacao"),
        ("data", DATE, "dat_liquidacao"),
        ("dsc_tipo_liquidacao", COPY, "dsc_tipo_liquidacao"),
        ("documento_responsavel", COPY, "num_doc_liquidante"),
        ("nome_responsavel", COPY, "nom_liquidante"),
        ("valor_liquidacao_original", COPY, "vlr_liquidacao"),
        ("valor_anulado", COPY, "vlr_anu_liquidacao"),
        ("num_versao_arq", CONST, "1"),
        ("id_municipio", MUNI, None),
    ],
    "pagamento": [
        ("id_pagamento", COPY, "seq_pagamento"),
        ("orgao", COPY, "cod_orgao"),
        ("id_unidade_gestora", COPY, "seq_unidade"),
        ("id_empenho", COPY, "seq_empenho"),
        ("id_rsp", COPY, "seq_rsp"),
        ("id_liquidacao", COPY, "seq_liquidacao"),
        ("seq_orgao_empenho", COPY, "seq_orgao_empenho"),
        ("seq_unid_empenho", COPY, "seq_unid_empenho"),
        ("ano", COPY, "num_ano_referencia"),
        ("mes", COPY, "num_mes_referencia"),
        ("num_doc_resp", COPY, "num_doc_resp"),
        ("nom_resp", COPY, "nom_resp"),
        ("documento_credor", COPY, "num_doc_credor"),
        ("nome_credor", COPY, "nom_credor"),
        ("numero_empenho", COPY, "num_empenho"),
        ("dat_empenho", DATE, "dat_empenho"),
        ("numero_liquidacao", COPY, "num_liquidacao"),
        ("dat_liquidacao", DATE, "dat_liquidacao"),
        ("numero_pagamento", COPY, "num_ord_pagamento"),
        # `dsc_pagamento` is dropped between `dat_pagamento` and `dsc_tipo_pagamento`,
        # and `dsc_cod_orcamentario` between `dsc_fonte_recurso` and `vlr_pag_fonte`.
        ("data", DATE, "dat_pagamento"),
        ("dsc_tipo_pagamento", COPY, "dsc_tipo_pagamento"),
        ("fonte", COPY, "dsc_fonte_recurso"),
        ("valor_pagamento_original", COPY, "vlr_pag_fonte"),
        ("vlr_ret_fonte", COPY, "vlr_ret_fonte"),
        ("vlr_ant_fonte", COPY, "vlr_ant_fonte"),
        ("vlr_anu_fonte", COPY, "vlr_anu_fonte"),
        ("num_versao_arq", CONST, "1"),
        # Constant. Present in this mirror only, not in the other three.
        ("sigla_uf", CONST, "MG"),
        ("id_municipio", MUNI, None),
    ],
    "rsp": [
        ("id_rsp", COPY, "seq_rsp"),
        ("orgao", COPY, "cod_orgao"),
        ("id_unidade_gestora", COPY, "seq_unidade"),
        # NOT renamed to ano/mes here, unlike liquidação and pagamento, which rename the
        # identically named source columns. The dbt liquidação and pagamento models join
        # this mirror and read `num_ano_emp_origem`; renaming these would be invisible
        # to dbt and would still break the staging schema.
        ("num_ano_referencia", COPY, "num_ano_referencia"),
        ("num_mes_referencia", COPY, "num_mes_referencia"),
        ("id_empenho_origem", COPY, "seq_empenho_origem"),
        ("numero_empenho", COPY, "num_empenho_origem"),
        ("data_origem", DATE, "dat_empenho_origem"),
        ("num_ano_emp_origem", COPY, "num_ano_emp_origem"),
        ("dsc_dotacao_ori", COPY, "dsc_dotacao_ori"),
        ("valor_original", COPY, "vlr_original"),
        ("valor_processado", COPY, "vlr_saldo_ant_proc"),
        ("valor_nao_processado", COPY, "vlr_saldo_ant_nao_proc"),
        ("num_versao_arq", CONST, "1"),
        ("id_municipio", MUNI, None),
    ],
}


# Which package carries which phase, and which member of the per-municipality zip. The
# member suffix must be matched whole: `.empenho.empenho.csv` and
# `.empenho.credorEmpenho.csv` both contain "empenho", and picking the wrong one loads a
# different table under the right name.
# (category, SOURCE MEMBER, phase). The member locates the CSV inside the archive;
# the phase names the staging mirror. They were identical for the original four, so
# one field did both jobs; the wholesale streams below need them separated
# (`contratos` -> `contrato`, `julgLicitacao` -> `licitacao_julgamento`).
MEMBERS: dict[str, list[tuple[str, str, str]]] = {
    "empenho": [
        ("empenho", "empenho", "empenho"),
        ("empenho", "rsp", "rsp"),
    ],
    "despesa": [
        ("despesa", "liquidacao", "liquidacao"),
        ("despesa", "pagamento", "pagamento"),
    ],
}


# ---------------------------------------------------------------------------
# Streams onboarded wholesale.
#
# The four phases above are hand-pinned because they must REPRODUCE a staging
# layout that already exists in the bucket. These do not: the tables are new, so
# their staging layout is defined right here as *the source header order with a
# small mechanical rename*.
#
# That is only safe because the source header is a fixed contract.
# `mg_source_headers.json` holds it, generated from 16,575 member headers over
# 13 exercises x 4 categories x 25 municipalities, with ZERO variation. If a
# header ever moves, `build()` raises on the missing column rather than silently
# writing a shifted table.
#
# `restos_pagar` is deliberately absent: the existing `rsp` mirror already
# carries every column of that stream, so the new table is a dbt model over it
# rather than a second copy of the same data.
WHOLESALE: dict[str, tuple[str, str]] = {
    # --- licitacao ---
    "dispensa": ("licitacao", "dispensa"),
    "dispensa_cotacao": ("licitacao", "cotDispensa"),
    "dispensa_credenciado": ("licitacao", "credDispensa"),
    "dispensa_fornecedor": ("licitacao", "fornDispensa"),
    "dispensa_item": ("licitacao", "itemDispensa"),
    "dispensa_dotacao": ("licitacao", "recDispensa"),
    "dispensa_responsavel": ("licitacao", "respDispensa"),
    "licitacao": ("licitacao", "licitacao"),
    "licitacao_comissao": ("licitacao", "comissaoLicitacao"),
    "licitacao_cotacao": ("licitacao", "cotacaoLicitacao"),
    "licitacao_homologacao": ("licitacao", "homologLicitacao"),
    "licitacao_item": ("licitacao", "itemLicitacao"),
    "licitacao_julgamento": ("licitacao", "julgLicitacao"),
    "licitacao_parecer": ("licitacao", "parecLicitacao"),
    "licitacao_participante": ("licitacao", "habLicitacao"),
    "licitacao_quadro_societario": ("licitacao", "quadroSocLicitacao"),
    "licitacao_dotacao": ("licitacao", "recLicitacao"),
    "licitacao_responsavel": ("licitacao", "respLicitacao"),
    "registro_preco_adesao": ("licitacao", "regadesao"),
    "registro_preco_adesao_cotacao": ("licitacao", "cotRegadesao"),
    "registro_preco_adesao_item": ("licitacao", "itemRegadeso"),
    "registro_preco_adesao_vencedor": ("licitacao", "vencRegadesao"),
    # --- contrato ---
    "contrato": ("contrato", "contratos"),
    "contrato_apostilamento": ("contrato", "apostContrato"),
    "contrato_contabilizacao": ("contrato", "contContrato"),
    "contrato_credito": ("contrato", "creditoContrato"),
    "contrato_item": ("contrato", "itemContrato"),
    "contrato_rescisao": ("contrato", "resContrato"),
    "contrato_termo_aditivo": ("contrato", "termoContrato"),
    "contrato_termo_aditivo_item": ("contrato", "itemTrmContrato"),
    # --- despesa ---
    "alteracao_orcamentaria": ("despesa", "altOrcamentaria"),
    "decreto": ("despesa", "decretos"),
    "despesa_dotacao": ("despesa", "despesa"),
    "lei_decreto": ("despesa", "leisDecretos"),
    "liquidacao_fonte": ("despesa", "liquidacaoFonte"),
    "liquidacao_nota_fiscal": ("despesa", "liquidacaoNfs"),
    "nota_fiscal": ("despesa", "notasfiscais"),
    "nota_fiscal_item": ("despesa", "itemNfs"),
    "pagamento_movimento": ("despesa", "movPagamento"),
    # --- empenho ---
    "empenho_credor": ("empenho", "credorEmpenho"),
    "empenho_fonte": ("empenho", "empenhoFonte"),
    "restos_pagar_credor": ("empenho", "credorRsp"),
    "restos_pagar_movimentacao": ("empenho", "movimentacaoRsp"),
    "restos_pagar_movimentacao_credor": ("empenho", "credorMovResp"),
    "restos_pagar_movimentacao_fonte": ("empenho", "movFonteRsp"),
}

# Renames applied to the wholesale streams. Everything not listed keeps its
# source name -- staging is a thin mirror and the dbt models do the real naming.
_WHOLESALE_RENAME = {
    "cod_orgao": "orgao",  # `orgao` IS cod_orgao; see the long note above
    "seq_unidade": "id_unidade_gestora",
    "num_ano_referencia": "ano",
    "num_anoexercicio": "ano",
    "num_mes_referencia": "mes",
    "num_mesexercicio": "mes",
}
# `seq_orgao` is redundant with `cod_orgao`; the municipality comes from the file
# name, which is authoritative. Both are reported under `unused_columns`, so the
# EXPECTED unused set for every wholesale phase is exactly these two -- anything
# else there means a source header moved.
_WHOLESALE_DROP = ("seq_orgao", "cod_municipio")

_HEADERS: dict[str, list[str]] = json.loads(
    (Path(__file__).resolve().parent / "mg_source_headers.json").read_text()
)


def _wholesale_spec(
    category: str, member: str
) -> list[tuple[str, str, str | None]]:
    """Source header order, renamed in place, with `id_municipio` appended."""
    header = _HEADERS[f"{category}/{member}"]
    spec: list[tuple[str, str, str | None]] = []
    for column in header:
        if column in _WHOLESALE_DROP:
            continue
        if column.startswith("dat_"):
            spec.append(("data_" + column[4:], DATE, column))
        elif column.startswith("vlr_"):
            # Normalised, unlike the original four, which reproduce the old
            # ingest's verbatim form. These tables have no history to match.
            spec.append(("valor_" + column[4:], FLOATSTR, column))
        else:
            spec.append((_WHOLESALE_RENAME.get(column, column), COPY, column))
    spec.append(
        (
            "id_municipio",
            MUNI,
            "cod_municipio" if "cod_municipio" in header else None,
        )
    )
    names = [name for name, _, _ in spec]
    if len(names) != len(set(names)):
        raise AssertionError(
            f"{category}/{member}: duplicate staging column in {names}"
        )
    return spec


for _phase, (_category, _member) in WHOLESALE.items():
    if _phase in SPECS:
        raise AssertionError(
            f"wholesale phase {_phase} collides with a pinned phase"
        )
    SPECS[_phase] = _wholesale_spec(_category, _member)
    MEMBERS.setdefault(_category, []).append((_category, _member, _phase))

# Built AFTER the wholesale phases are registered -- it has to cover every phase,
# and computing it earlier silently left the new mirrors out.
MIRROR = {phase: f"raw_{phase}_mg" for phase in SPECS}

# `SICOM.<exercicio>.<ibge7>.<categoria>.zip`
NESTED_RE = re.compile(
    r"^SICOM\.(?P<year>\d{4})\.(?P<ibge>\d{7})\.(?P<cat>[^.]+)\.zip$"
)
# `<categoria>_<exercicio>.zip`, as download_mg.py writes it.
PACKAGE_RE = re.compile(r"^(?P<label>[a-z]+)_(?P<year>\d{4})\.zip$")
LABEL_CATEGORY = {"empenhos": "empenho", "despesas": "despesa"}

BOM = "﻿"

# Source members that were not valid UTF-8; expected to stay empty.
encoding_fallbacks: Counter = Counter()


def package_year(path: Path) -> int | None:
    """The exercise a downloaded package covers, or None if it is not one of ours."""
    match = PACKAGE_RE.match(path.name)
    return int(match.group("year")) if match else None


def schema_for(phase: str) -> pa.Schema:
    """All-STRING, in staging order. Pinned so column order cannot drift by accident."""
    return pa.schema(
        [pa.field(name, pa.string()) for name, _, _ in SPECS[phase]]
    )


def _iso(value: str | None, bad: Counter) -> str | None:
    """`YYYYMMDD` -> `YYYY-MM-DD`. Already-ISO and dd/mm/yyyy pass through.

    An all-zero date is the source's own missing-value sentinel and becomes NULL, which
    is what staging holds. Anything else unparseable is counted and reported rather than
    quietly nulled: a whole column going NULL is precisely the failure this file exists
    to prevent.
    """
    if value is None:
        return None
    text = value.strip()
    if not text or set(text) <= {"0", "-", "/"}:
        return None
    if len(text) == 8 and text.isdigit():
        year, month, day = text[:4], text[4:6], text[6:]
    elif len(text) == 10 and text[4] == "-" and text[7] == "-":
        year, month, day = text[:4], text[5:7], text[8:]
    elif len(text) == 10 and text[2] == "/" and text[5] == "/":
        day, month, year = text[:2], text[3:5], text[6:]
    else:
        bad[text[:12]] += 1
        return None
    if not ("01" <= month <= "12" and "01" <= day <= "31"):
        bad[text[:12]] += 1
        return None
    return f"{year}-{month}-{day}"


def _floatstr(value: str | None, bad: Counter) -> str | None:
    """`' 0000003840.0000'` -> `'3840.0'`, reproducing the empenho mirror's form.

    Idempotent over the plain form, so it is correct whichever the source publishes.
    A value that is not a number is kept verbatim and counted, never dropped.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return str(float(text))
    except ValueError:
        bad[text[:16]] += 1
        return value


# A source stream whose published header is WRONG.
#
# `.empenho.movimentacaoRsp.csv` ships a 9-column header that is a byte-for-byte
# copy of `movFonteRsp`'s, while every data row carries 19 columns. Measured over
# 13 exercises: header 9 columns in 260 of 260 archives, data 19 columns in
# 52,670 of 52,670 rows. The data is well formed; only the header is wrong.
#
# Without this the delimiter-arithmetic guard rejects every row and the table
# lands EMPTY -- which is exactly what happened on the first full-source run
# (raw_restos_pagar_movimentacao_mg:2021, 0 rows, 87,576 ragged).
#
# !! THE NAMES BELOW ARE INFERRED FROM THE DATA, NOT PUBLISHED BY TCE-MG. !!
# Positions 1-12 align field for field with the `rsp` stream's own layout, and
# position 7 is read as `cod_unidade` because its value is the second component
# of `dsc_dotacao_ori` on the same row (orgao.unidade.funcao...). Positions 13-19
# are read from their content: type, date, value, document, reason. Treat them as
# provisional until checked against the SICOM layout specification.
_HEADER_OVERRIDE: dict[str, list[str]] = {
    "empenho/movimentacaoRsp": [
        "seq_mov_rsp",
        "seq_rsp",
        "seq_orgao",
        "cod_orgao",
        "num_ano_referencia",
        "num_mes_referencia",
        "cod_unidade",
        "cod_subunidade",
        "num_empenho_origem",
        "dat_empenho_origem",
        "num_ano_emp_origem",
        "dsc_dotacao_ori",
        "dsc_tipo_rsp",
        "dsc_tipo_movimentacao",
        "dat_movimentacao",
        "vlr_movimentacao",
        "dsc_documento",
        "dat_documento",
        "dsc_motivo",
    ],
}


def read_source_csv(
    raw: bytes, path_for_errors: str, header_override: list[str] | None = None
) -> pa.Table:
    """One source CSV as an all-STRING arrow table.

    **UTF-8 at source, not latin-1.** An earlier version of this file decoded as
    latin-1 on the stated grounds that the source was latin-1. It is not. Measured
    over 3,978 CSV members spanning 13 exercises x 4 categories: every one decodes
    as valid UTF-8, none fails. Decoding UTF-8 bytes as latin-1 does not raise --
    it silently mojibakes every accented string, turning `PRESTAÇÃO` into
    `PRESTAÃ\x87ÃO`, and a byte comparison against other output from the same bad
    decoder still passes. The tell is the published data: the original staging in
    the bucket holds `À PRESTAÇÃO DE SERVIÇOS MECÂNICOS` and prod holds
    `PRESTAÇÃO DE SERVIÇO DE INFORMÁTICA`, while the latin-1 output held
    `prestaãão de serviã`.

    A member that is genuinely not UTF-8 falls back to latin-1 and is COUNTED
    (`bad_encoding`), never silently mangled.

    TWO DIALECT FACTS, BOTH MEASURED, BOTH COSTLY TO GET WRONG
    ----------------------------------------------------------
    **The source is QUOTE_NONE.** The `"` characters that appear are literal data
    inside free-text fields, not field delimiters. Reading with `quote_char='"'`
    makes arrow swallow delimiters until the next quote and silently merge
    records: measured on 2021 pagamento, 2 of 94 quote-bearing files lost rows
    that way (11,853 -> 11,849 and 8,005 -> 7,983), and one municipality
    (3126109) failed outright with "Expected 29 columns, got 22", costing all
    22,684 of its rows. Verified across 416 files spanning 2014-2026: no record
    ever spans two lines, so nothing needs quoting to be parsed.

    **Some rows carry unescaped delimiters.** A free-text field in `pagamento`
    can contain `;` -- e.g. `ART. 5º; INCISO V; EC Nº 123/2022` yields 30
    delimiters where the header has 28. Arrow cannot represent a ragged row, and
    letting it raise costs the whole municipality. Measured prevalence, sampling
    60 municipalities per exercise per stream:

        pagamento 2023   0.437% of rows   (29 of 60 files)
        pagamento 2024   0.001%
        pagamento 2026   0.008%
        everything else  none

    **2014-2021 -- the range MiDES publishes -- is entirely clean**, so this is a
    source change from 2023 onward, not a latent defect in what is already out.

    Ragged rows are dropped and COUNTED (`ragged_rows`), never silently. Dropping
    ~0.4% of one exercise's payments is a real loss and is reported as such; it is
    strictly better than the alternative, which is losing 100% of a municipality
    because one clerk typed a semicolon. Repairing them would mean guessing which
    column the extra delimiters fell in, and a wrongly re-joined payment row is
    worse than an absent one.

    A FEW MUNICIPALITIES SHIP ALREADY-MOJIBAKED TEXT, AND THAT IS NOT OURS TO FIX.
    Municipality 3157906's 2020 contratos carry the literal bytes
    `jur\\xc3\\x83\\xc2\\xaddica` -- that is `Ã` + soft hyphen, correctly UTF-8 encoded.
    The municipality took `í` (\\xc3\\xad), read it as latin-1 and re-encoded each byte
    before submitting. Decoding as UTF-8 reproduces `jurÃ\\xadica`, which is exactly
    what the file says. Measured 2026-09-24: ~102 of 98,404 `contrato` 2020 rows
    (0.10%), concentrated in one municipality. Repairing it would mean guessing
    which of several encodings a given string went through, and a wrongly-repaired
    string is worse than a faithfully-reproduced bad one -- so it is left alone.
    Do not mistake it for the latin-1 bug this function fixes: that one affected
    EVERY accented string in EVERY municipality, not a handful in one.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        # Not observed in 3,978 members, but a silent mangle is exactly the failure
        # this function exists to prevent, so the fallback is recorded.
        encoding_fallbacks[path_for_errors.rsplit("/", 1)[-1]] += 1
        text = raw.decode("latin-1")
    if text.startswith(BOM):
        text = text[len(BOM) :]
    first = text.find("\n")
    if first == -1:
        raise ValueError(f"{path_for_errors}: no newline, file is not a CSV")
    header = [h.strip() for h in text[:first].rstrip("\r").split(";")]
    if header_override is not None:
        # The published header is wrong for this stream -- see _HEADER_OVERRIDE.
        # Line 0 is still a header line and is still skipped; it is replaced so
        # the ragged-row arithmetic and arrow both see the real column count.
        header = list(header_override)

    # Drop ragged rows before arrow sees them, so one bad row cannot take the
    # municipality with it.
    expected = len(header) - 1
    lines = text.split("\n")
    if header_override is not None:
        lines[0] = ";".join(header)
    kept = [lines[0]]
    ragged = 0
    for line in lines[1:]:
        if not line or line == "\r":
            continue
        if line.rstrip("\r").count(";") != expected:
            ragged += 1
            continue
        kept.append(line)
    text = "\n".join(kept) + "\n"

    table = pacsv.read_csv(
        io.BytesIO(text.encode("utf-8")),
        parse_options=pacsv.ParseOptions(
            delimiter=";",
            # QUOTE_NONE. See the docstring: the source's quotes are data.
            quote_char=False,
            newlines_in_values=False,
        ),
        convert_options=pacsv.ConvertOptions(
            column_types={name: pa.string() for name in header},
            # The default null list also swallows 'NA', 'null', 'None' and 'n/a', every
            # one of which is a legitimate value in a free-text credor name. Only the
            # empty field is null, which is what the staging parquet holds.
            null_values=[""],
            strings_can_be_null=True,
        ),
    )
    return table, ragged


def build(phase: str, table: pa.Table, ibge: str, counters: dict) -> pa.Table:
    """Apply the rename map to one source table, in staging order."""
    present = set(table.schema.names)
    spec = SPECS[phase]
    required = {
        src
        for _, kind, src in spec
        if kind in (COPY, DATE, FLOATSTR) and src is not None
    }
    missing = sorted(required - present)
    if missing:
        raise KeyError(
            f"{phase}: source CSV is missing {missing}. The staging schema cannot be "
            f"reproduced without them. Header was: {sorted(present)}"
        )
    unused = sorted(present - required - {"cod_municipio"})
    if unused:
        counters["unused_columns"].update(unused)

    columns: list[pa.Array] = []
    rows = table.num_rows
    for _name, kind, src in spec:
        if kind == COPY:
            columns.append(table.column(src).combine_chunks())
        elif kind == DATE:
            values = table.column(src).to_pylist()
            columns.append(
                pa.array(
                    [_iso(v, counters["bad_dates"]) for v in values],
                    type=pa.string(),
                )
            )
        elif kind == FLOATSTR:
            values = table.column(src).to_pylist()
            columns.append(
                pa.array(
                    [_floatstr(v, counters["bad_values"]) for v in values],
                    type=pa.string(),
                )
            )
        elif kind == MUNI:
            if src and src in present:
                # Empenho publishes the municipality. Agreement has held everywhere it
                # was checked; a disagreement means one of the two is not the IBGE-7
                # code and the mirrors would stop joining to each other.
                distinct = {v for v in table.column(src).to_pylist() if v}
                if distinct - {ibge}:
                    counters["muni_mismatch"][
                        f"{ibge}:{sorted(distinct)[:3]}"
                    ] += 1
            columns.append(pa.array([ibge] * rows, type=pa.string()))
        elif kind == CONST:
            columns.append(pa.array([src] * rows, type=pa.string()))
        else:
            raise AssertionError(kind)
    return pa.Table.from_arrays(columns, schema=schema_for(phase))


def write(phase: str, table: pa.Table, year: int, ibge: str) -> None:
    """One parquet, named exactly as the existing staging objects are named."""
    directory = OUTPUT_DIR / MIRROR[phase]
    directory.mkdir(parents=True, exist_ok=True)
    dest = directory / f"{phase}_{year}_{ibge}.parquet"
    tmp = dest.with_suffix(".part")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(dest)


def clean_municipality(
    archive: zipfile.ZipFile,
    wanted: list[tuple[str, str, str]],
    year: int,
    ibge: str,
    label: str,
    counters: dict,
) -> None:
    """Convert one municipality's archive into its parquet mirrors.

    Shared by both input layouts. `download_mg.py` fetches whole-state packages
    with 853 nested zips inside; `harvest_mg.py` fetches those same
    per-municipality zips directly. The archives are byte-identical either way --
    only the walk above differs -- so all of the transformation, the IBGE key and
    the value/date normalisation live here and are reached by both.
    """
    members = archive.namelist()
    for category, member, phase in wanted:
        suffix = f".{category}.{member}.csv"
        hit = [m for m in members if m.endswith(suffix)]
        if not hit:
            counters["missing_member"][f"{phase}:{year}"] += 1
            continue
        if len(hit) > 1:
            raise ValueError(f"{label}: {len(hit)} members match {suffix}")
        try:
            source, ragged = read_source_csv(
                archive.read(hit[0]),
                hit[0],
                _HEADER_OVERRIDE.get(f"{category}/{member}"),
            )
            if ragged:
                counters["ragged_rows"][f"{phase}:{year}"] += ragged
        except (pa.ArrowInvalid, ValueError) as exc:
            # A malformed row aborts the whole file in arrow. Record which
            # municipality and keep going; the run still exits non-zero.
            counters["unreadable"][f"{phase}:{year}:{ibge}"] += 1
            print(f"  UNREADABLE {hit[0]}: {str(exc)[:160]}")
            continue
        built = build(phase, source, ibge, counters)
        write(phase, built, year, ibge)
        counters["rows"][f"{MIRROR[phase]}:{year}"] += built.num_rows
        counters["files"][f"{MIRROR[phase]}:{year}"] += 1


def clean_tree(year: int, category: str, counters: dict) -> int:
    """Clean one exercise/category from the per-municipality tree on disk.

    `harvest_mg.py` writes `<MG_INPUT>/<year>/<categoria>/SICOM.<year>.<ibge>.<cat>.zip`.
    Each file is exactly what a nested member of the bulk package would be, so
    this only has to locate them and hand each to `clean_municipality`.
    """
    directory = MG_INPUT / str(year) / category
    files = sorted(directory.glob("*.zip")) if directory.is_dir() else []
    if not files:
        return 0
    wanted = MEMBERS[category]
    print(f"{year} {category}: {len(files)} municipalities", flush=True)
    for index, path in enumerate(files, 1):
        nested = NESTED_RE.match(path.name)
        if not nested:
            counters["unparsed_nested"][path.name] += 1
            continue
        if (
            nested.group("cat") != category
            or int(nested.group("year")) != year
        ):
            # The tree is laid out by year and category, so a file disagreeing
            # with its own directory is a harvest bug, not a source quirk.
            counters["year_mismatch"][f"{year}/{category}<-{path.name}"] += 1
            continue
        with zipfile.ZipFile(path) as archive:
            clean_municipality(
                archive,
                wanted,
                year,
                nested.group("ibge"),
                path.name,
                counters,
            )
        if index % 200 == 0:
            print(f"  {index}/{len(files)}", flush=True)
    return len(files)


def clean_package(path: Path, counters: dict) -> None:
    """Unpack one whole-state package: 853 nested zips, two phases out of each."""
    match = PACKAGE_RE.match(path.name)
    if not match:
        raise ValueError(
            f"{path.name}: not a package name written by download_mg.py"
        )
    year = int(match.group("year"))
    category = LABEL_CATEGORY.get(match.group("label"))
    if category is None:
        raise ValueError(f"{path.name}: unknown category label")
    wanted = MEMBERS[category]
    print(f"{path.name}: exercise {year}, category {category}")

    seen_municipios: set[str] = set()
    with zipfile.ZipFile(path) as outer:
        names = sorted(
            n for n in outer.namelist() if n.lower().endswith(".zip")
        )
        for index, name in enumerate(names, 1):
            nested = NESTED_RE.match(Path(name).name)
            if not nested:
                counters["unparsed_nested"][Path(name).name] += 1
                continue
            if nested.group("cat") != category:
                counters["wrong_category"][nested.group("cat")] += 1
                continue
            if int(nested.group("year")) != year:
                # The package is fetched per exercise; a stray year inside it would
                # otherwise be written under the package's year and corrupt both.
                counters["year_mismatch"][
                    f"{year}<-{nested.group('year')}"
                ] += 1
                continue
            ibge = nested.group("ibge")
            seen_municipios.add(ibge)
            blob = outer.read(name)
            with zipfile.ZipFile(io.BytesIO(blob)) as inner:
                clean_municipality(inner, wanted, year, ibge, name, counters)
            if index % 100 == 0:
                print(f"  {index}/{len(names)} municipalities", flush=True)
    print(f"  {len(seen_municipios)} municipalities in {path.name}")


def main(years: set[int] | None = None) -> None:
    if not MG_INPUT.is_dir():
        raise SystemExit(
            f"nothing to clean: {MG_INPUT} does not exist. Run download_mg.py first "
            "(it needs a TCE-MG token; see TCE_MG_CREDENTIAL_REQUEST.md)."
        )
    # Two input layouts, both produced by this repo:
    #   bulk  -- `<MG_INPUT>/<label>_<year>.zip`, written by download_mg.py
    #   tree  -- `<MG_INPUT>/<year>/<categoria>/SICOM.*.zip`, by harvest_mg.py
    # The tree is preferred when present: it is the layout that can actually be
    # completed, since the gateway truncates bulk packages above ~55 MB.
    packages = sorted(p for p in MG_INPUT.glob("*.zip") if package_year(p))
    if years is not None:
        packages = [p for p in packages if package_year(p) in years]
    tree_years = sorted(
        int(p.name)
        for p in MG_INPUT.iterdir()
        if p.is_dir() and p.name.isdigit()
    )
    if years is not None:
        tree_years = [y for y in tree_years if y in years]
    if not packages and not tree_years:
        raise SystemExit(
            f"no packages or per-municipality tree under {MG_INPUT} "
            f"for {years or 'any year'}"
        )

    counters = {
        key: Counter()
        for key in (
            "rows",
            "files",
            "bad_dates",
            "bad_values",
            "muni_mismatch",
            "missing_member",
            "unparsed_nested",
            "wrong_category",
            "year_mismatch",
            "unreadable",
            "unused_columns",
            "ragged_rows",
        )
    }
    for package in packages:
        clean_package(package, counters)
    for year in tree_years:
        for category in MEMBERS:
            clean_tree(year, category, counters)

    print("\n=== written ===")
    for key in sorted(counters["files"]):
        print(
            f"  {key:34} {counters['files'][key]:>5} files "
            f"{counters['rows'][key]:>12,} rows"
        )

    # Everything below is either a defect or a source change. None of it is routine.
    problems = 0
    for key in (
        "unreadable",
        "muni_mismatch",
        "unparsed_nested",
        "year_mismatch",
    ):
        if counters[key]:
            problems += sum(counters[key].values())
            print(f"\n{key}: {dict(list(counters[key].items())[:10])}")
    # Reported, not fatal. A handful of junk dates is normal in SICOM, a municipality
    # that filed no restos a pagar has no member, and a package legitimately carries
    # nested zips for categories this dataset does not consume. None of them may pass
    # unremarked, though: `bad_dates` at scale is the source changing its date format,
    # and every `data` in BigQuery is one `safe_cast` away from NULL when that happens.
    for key in (
        "bad_dates",
        "bad_values",
        "missing_member",
        "unused_columns",
        "wrong_category",
        "ragged_rows",
    ):
        if counters[key]:
            print(
                f"\n{key} (reported): {dict(list(counters[key].items())[:10])}"
            )
    # A completion sentinel, so a later step can tell "the clean finished" from
    # "the clean is still writing". `upload_mg.py` refuses to run without it.
    # This exists because an upload was once started against a half-written tree:
    # the mirror count and total size both looked right, because every mirror
    # directory is created early and only fills up later, so neither is evidence
    # of completion. Only the process exiting is.
    # The sentinel means "this process reached the end", NOT "the data is
    # perfect" -- so it is written even when `problems` is non-zero, with the
    # count recorded. The two are different questions and conflating them is how
    # a half-written tree gets uploaded: a run that dies mid-way leaves no
    # sentinel at all, which is exactly the case the guard has to catch.
    sentinel = OUTPUT_DIR / ".clean_complete"
    sentinel.write_text(
        json.dumps(
            {
                "finished": time.time(),
                "problems": problems,
                "mirrors": sorted(MIRROR.values()),
            }
        ),
        encoding="utf-8",
    )
    if problems:
        print(f"\nFAILED: {problems} structural problems, see above")
        raise SystemExit(1)
    print("\nMG clean complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year", type=int, action="append", help="restrict to this exercise"
    )
    args = parser.parse_args()
    main(years=set(args.year) if args.year else None)
