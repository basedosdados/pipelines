"""Source constants for br_bd_execucao_estadual.

State-government budget execution and procurement. One section per UF, because the
sources have nothing in common beyond what they describe.
"""

import os
import re
import unicodedata
from pathlib import Path

DATASET_ID = "br_bd_execucao_estadual"


def normalise_column(name: str) -> str:
    """A BigQuery-legal column name.

    Several sources publish human column headings, and BigQuery accepts none of them:
    spaces and accents are rejected outright ("N° da Licitação", BA), a '.' in a parquet
    field name fails the load with `Character '.' found in field name` ("Cod. Acao", PE's
    legacy export), and a name may not begin with a digit ("13.02 - Razao Social", also
    PE). Accents fold to ASCII, everything else becomes an underscore, a leading digit
    gets an underscore prefix, and the result is lowercased.
    """
    folded = unicodedata.normalize("NFKD", name)
    ascii_only = folded.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_only).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    if not slug:
        return "coluna"
    return f"_{slug}" if slug[0].isdigit() else slug


# Scratch lives outside the repo and outside Dropbox: multi-GB, fully reproducible,
# and deleted at the end of onboarding (see .claude/rules/onboarding-workflow.md).
#
# EXEC_ESTADUAL_DATA_DIR overrides it, which is what the Prefect flow sets: a worker
# has no ~/Downloads worth writing to, and each flow run wants its own temp dir so a
# retry cannot inherit a half-written file from the run before it.
DATA_DIR = Path(
    os.environ.get(
        "EXEC_ESTADUAL_DATA_DIR",
        str(Path.home() / "Downloads" / "br_state_budget_data"),
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# dados.mg.gov.br and several sibling portals return 403 to a bare curl/requests UA.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

# --------------------------------------------------------------------------- MG

MG_CKAN = "https://dados.mg.gov.br/api/3/action/package_show"

# CKAN packages that make up MG's execution + procurement model.
MG_PACKAGES = {
    "despesa": "eb709e1d-c19e-4371-b1ea-436920cf537a",
    "compras_contratos": "86e157db-d2c5-4151-9b16-9c5987462cba",
    "portal_licitacoes_mg": "ce9dbef9-085c-4450-994f-08ba72e2316e",
    "portal_contratos": "b27999c9-6151-4b86-8327-baa40b6d8983",
}

MG_FIRST_YEAR, MG_LAST_YEAR = 2002, 2026

# Fact tables are per-year; dimensions are not. Keys are the source file stem, values
# are the staging table the file lands in.
MG_YEARLY_TABLES = {
    "ft_despesa": "mg_ft_despesa",
    "dm_empenho_desp": "mg_dm_empenho",
}

MG_STATIC_TABLES = {
    "dm_favorecido": "mg_dm_favorecido",
    "dm_funcao_desp": "mg_dm_funcao",
    "dm_subfuncao_desp": "mg_dm_subfuncao",
    "dm_programa": "mg_dm_programa",
    "dm_acao": "mg_dm_acao",
    "dm_elemento_desp": "mg_dm_elemento",
    "dm_item_desp": "mg_dm_item",
    "dm_fonte": "mg_dm_fonte",
    "dm_unidade_orc": "mg_dm_unidade_orc",
    "dm_categ_econ": "mg_dm_categoria",
    "dm_grupo_desp": "mg_dm_grupo",
    "dm_modalidade_aplic": "mg_dm_modalidade_aplic",
    "dm_procedencia": "mg_dm_procedencia",
    "dm_tipo_documento": "mg_dm_tipo_documento",
    "dm_situacao_op_desp": "mg_dm_situacao_op",
    "fl_despesa_pgto": "mg_fl_despesa_pgto",
    # procurement (compras_contratos). `ft_compras` is the item-level fact: 2.1M rows,
    # one per (process, item), with quantity, unit reference and homologated prices, and
    # the winning supplier -- so it feeds both `licitacao_item` and the winner side of
    # `licitacao_participante`, neither of which the despesa model can produce.
    "ft_compras": "mg_ft_compras",
    "ft_compras_contrato": "mg_ft_compras_contrato",
    "dm_processo": "mg_dm_processo",
    "dm_contratado": "mg_dm_contratado",
    "dm_contrato": "mg_dm_contrato",
    "dm_item_matserv": "mg_dm_item_matserv",
    "dm_material_servico": "mg_dm_material_servico",
    "dm_grupo_matserv": "mg_dm_grupo_matserv",
    "dm_classe_matserv": "mg_dm_classe_matserv",
    "dm_unidade_medida": "mg_dm_unidade_medida",
    "dm_linha_fornec": "mg_dm_linha_fornec",
    "dm_tipo_licitacao": "mg_dm_tipo_licitacao",
    "dm_procedimento": "mg_dm_procedimento",
    "dm_situacao_proc": "mg_dm_situacao_proc",
    "dm_situacao_cont": "mg_dm_situacao_cont",
    "dm_orgao_demanda": "mg_dm_orgao_demanda",
    "dm_orgao_contrato": "mg_dm_orgao_contrato",
    "dm_municipio": "mg_dm_municipio",
    "dm_tempo_diario": "mg_dm_tempo",
    "fl_compras_empenho": "mg_fl_compras_empenho",
    "dm_empenho_desp_compras_empenho": "mg_dm_empenho_compras",
}

# The portal_* flat files are annual CSVs (not gz) covering 2022+.
MG_PORTAL_TABLES = {
    "licitacoes": "mg_licitacao",
    "item": "mg_licitacao_item",
    "contratos": "mg_contrato",
    "itens": "mg_contrato_item",
}
MG_PORTAL_FIRST_YEAR = 2022

MG_SEP = ";"
MG_ENCODING = "utf-8-sig"  # the files carry a BOM

# MG withholds the identity of some natural persons rather than dropping the row.
MG_ANONYMISED = "INFORMACAO COM RESTRICAO DE ACESSO"

# --------------------------------------------------------------------------- SP

SP_SIGEO_FORM = (
    "https://www.fazenda.sp.gov.br/SigeoLei131/Paginas/FlexConsDespesa.aspx"
)
SP_FIRST_YEAR, SP_LAST_YEAR = 2010, 2026

# cblFase index -> phase. Ticking at least one execution phase is what makes SIGEO
# reveal the Credor / Licitação / Item / Município controls; without it they are
# absent from the DOM entirely.
SP_FASES = {
    0: "dotacao_inicial",
    1: "dotacao_atual",
    2: "empenhado",
    3: "liquidado",
    4: "pago",
}

SP_CTL = "ctl00$ContentPlaceHolder1$"
SP_CREDOR_TODOS = SP_CTL + "ckbT"

# --------------------------------------------------------------------------- BA

BA_CKAN = "https://dados.ba.gov.br/api/3/action/package_show"
# The `contratos` package is deliberately NOT listed. Its archive is 676 MB and holds
# six views, of which exactly one -- VW_PROCESSO_SEI -- is used here, and `despesas`
# ships that same view. Verified by reading each archive's central directory over HTTP
# range requests: despesas covers VW_PAINEL_DESPESA and VW_PROCESSO_SEI, licitacoes
# covers the four VW_PROC_AQUISICAO_*, and contratos adds only VW_ADITIVOS_APOSTILA,
# VW_DOCUMENTOS_ANEXOS, VW_INSTRUMENTO_DESPESA, VW_PAGAMENTOS_NOTA_ORDEM_BANCARIA and
# VW_PAINEL_CONTRATOS_FISCAIS -- none of them in BA_TABLES.
#
# It also does not download reliably: three consecutive attempts truncated and failed
# the archive integrity check, which aborted the whole flow run. The onboarding load
# never fetched it either, so `ba_empenho_sei` has always been built from the despesas
# copy of VW_PROCESSO_SEI and dropping the package changes no published data.
#
# Add it back only alongside a model that actually reads a contracts view.
BA_PACKAGES = {
    "despesas": "518569da-ccaa-4621-b8d2-e7424ec3f1ea",
    "licitacoes": "36c792f9-1999-4f21-a669-752a178b06b7",
}
BA_TABLES = {
    "VW_PAINEL_DESPESA": "ba_despesa",
    "VW_PROCESSO_SEI": "ba_empenho_sei",
    "VW_PROC_AQUISICAO_LIC_REQ": "ba_licitacao",
    "VW_PROC_AQUISICAO_ITEM": "ba_licitacao_item",
    "VW_PROC_AQUISICAO_FORNEC": "ba_licitacao_participante",
    "VW_PROC_AQUISICAO_ITEM_INSTRUMENTO": "ba_licitacao_empenho",
}

# --------------------------------------------------------------------------- PE

PE_CKAN = "https://dados.pe.gov.br/api/3/action/package_show"
PE_PACKAGES = {
    "todas-despesas-detalhadas": "0fad561c-e79b-40c4-babf-b0b8189273ec",
    "all-pagamentos": "57c99440-751c-47da-8cf8-75ab7c76ed74",
}
PE_FIRST_YEAR, PE_LAST_YEAR = 2008, 2026

# --------------------------------------------------------------------------- ES

ES_CKAN = "https://dados.es.gov.br/api/3/action/package_show"

# Slugs rather than UUIDs, unlike the other states: ES's are descriptive and were
# verified against the live catalogue, and `package_show` accepts either.
ES_PACKAGES = {
    "despesas": "portal-da-transparencia-despesas-execucao-orcamentaria-e-financeira",
    "compras": "portal-da-transparencia-compras-publicas",
    "contratos": "portal-da-transparencia-contratos",
}

# Source file stem -> staging table. Matched on the EXACT stem before `-<ano>`, never
# as a prefix: `ItensLotes` is a prefix of `ItensLotesDisputas`, so a prefix match
# silently files every bidder row into the item table. Same family as the MG
# `dm_empenho_desp_*` glob collision, and just as quiet.
ES_TABLES = {
    "Despesas": "es_despesa",
    "Licitacoes": "es_licitacao",
    "Lotes": "es_lote",
    "ItensLotes": "es_licitacao_item",
    "ItensLotesDisputas": "es_licitacao_participante",
    "Compras": "es_compra",
    "Editais": "es_edital",
    "Contratos": "es_contrato",
    # NumeroProcesso <-> NumeroEmpenho: ES's native process->commitment bridge, the
    # analogue of MG's fl_compras_empenho. Feeds `relacionamentos`.
    "Empenhos": "es_contrato_empenho",
}

# Published but deliberately not ingested:
#   OrcamentosDespesa / OrcamentosDespesaAssembleia -- the budget law (LOA), an
#     authorisation rather than an execution. A future `orcamento` table, not `despesa`.
#   OrcamentosExecucoes -- all 18 files are 0 bytes.
#   RestosAPagar -- 0 bytes before 2023, and `despesa.ValorRap` already carries the
#     RAP value on the execution row.
ES_SKIP_STEMS = frozenset(
    {
        "OrcamentosDespesa",
        "OrcamentosDespesaAssembleia",
        "OrcamentosExecucoes",
        "RestosAPagar",
    }
)

# The despesas package advertises 2004, but 2004-2008 are NOT transaction grain: each
# is 0.6-0.9 MB of annual aggregates padded into the same 71 columns, with
# `Favorecido = 'Informação não disponivel.'`, `CpfCnpjNis = '0'` and `Data` pinned to
# 31/12 of the exercise. Real per-document rows start in 2009, where the file jumps to
# 574 MB. Ingesting the earlier years would advertise five years of coverage that
# carries no creditor and no document.
ES_DESPESA_FIRST_YEAR = 2009
ES_FIRST_YEAR, ES_LAST_YEAR = 2009, 2026

# The year in a file name means two different things, and conflating them loses data.
#
# For the execution and procurement families it is the EXERCISE, and it agrees with the
# `Ano` / `DataCriacao` inside the file. Those can be year-scoped safely.
ES_YEAR_SCOPED_STEMS = frozenset(
    {
        "Despesas",
        "Licitacoes",
        "Lotes",
        "ItensLotes",
        "ItensLotesDisputas",
        "Compras",
        "Editais",
    }
)

# For `contratos` it is the year of a DATE THAT MAY BE MISSING, so the package carries
# buckets named 1753, 1991, 2001, 2027, 2028, 2032, 2224, 3024 and 5024.
#
# **1753 is not junk.** `01/01/1753` is SQL Server's `datetime` minimum -- the sentinel
# this source writes when `DataCelebracao` was never recorded -- so `Contratos-1753.csv`
# holds 180 genuine contracts (one of them R$14,751,756.36) and `Empenhos-1753.csv` 193
# genuine commitments, all with real `NumeroEmpenho`, supplier, value and vigência
# dates. A year filter over the file name deletes them without a word.
#
# The whole contratos family is ~45 MB, so it is always fetched in full and the real
# year is taken from the date columns downstream, never from the file name. The other
# odd buckets are 0 bytes and are dropped by the zero-size check, not by a year rule.
ES_ALWAYS_FETCH_STEMS = frozenset({"Contratos", "Empenhos"})

# The sentinel itself, for the cleaners: treat as NULL, never as a date.
ES_NULL_DATE = "01/01/1753"

# In the same package but not ingested yet: `AlteracoesContratuais` (contract
# amendments) and `Comissoes` (tender commissions). Both are small and real; neither
# has a home in the current schema.

ES_SEP = ";"
# Every file carries a UTF-8 BOM on the first header cell.
ES_ENCODING = "utf-8-sig"

# ES's 2004-2008 aggregate rows name the withheld creditor this way.
ES_ANONYMISED = "Informação não disponivel."

# --------------------------------------------------------------------------- RS

RS_CKAN = "https://dados.rs.gov.br/api/3/action/package_show"
RS_PACKAGE_LIST = "https://dados.rs.gov.br/api/3/action/package_list"

# RS is only reachable over some network paths: `dados.rs.gov.br` resolves everywhere
# but refused a residential Australian ISP outright while answering fine from a
# university range. It is not a country block -- both routes tried were Australian --
# so treat a timeout here as "try another egress", not as "the source is down".
#
# Package slugs change convention FOUR times across the series
# (`despesas-do-estado-em-2012` ... `despesas-do-estado-2022` ... `despesas-2023` ...
# `2024-despesas-do-estado`), so they are discovered from the catalogue rather than
# built from a template. A single f-string pattern silently misses whole years.
RS_FIRST_YEAR, RS_LAST_YEAR = 2012, 2026

# One flat table, published as twelve monthly ZIPs per exercise. Each archive holds one
# CSV (`Gasto-RS-<ano><mes>.csv`), ~180 MB uncompressed against ~9 MB zipped -- a 20x
# ratio, so the whole series is ~1.6 GB to fetch and ~36 GB to process.
RS_TABLE = "rs_despesa"

RS_SEP = ";"
# **Windows-1252, not the ISO-8859-1 the accents suggest.** The files carry smart
# quotes and en-dashes in the C1 range (0x91-0x96), which latin-1 leaves undefined, so
# duckdb refuses every one of them with `Invalid Input Error: File is not latin-1
# encoded`. That refusal is correct and useful: Python's latin-1 decodes ANY byte
# sequence, so a "try utf-8, else latin-1" probe reports success and yields mojibake.
#
# duckdb has no cp1252 reader, so clean_rs transcodes to UTF-8 while unpacking. The
# five bytes cp1252 itself leaves undefined (0x81, 0x8D, 0x8F, 0x90, 0x9D -- 2012 has
# one) are decoded as their latin-1 characters, which is lossless, rather than replaced.
RS_ENCODING = "cp1252"

# RS publishes ONE ROW PER PHASE (`FaseGasto` = Empenho / Liquidação / Pagamento /
# Retenção), where MG, BA, PE and SP all put the phase values as columns on one row.
# That is the MiDES ledger shape and the opposite of this dataset's `despesa` design.
# Staging mirrors the source either way; the decision belongs in the dbt model.
RS_PHASE_COLUMN = "FaseGasto"

# CPFs are published as all zeros rather than partially masked, so unlike MG
# (`***.195.606-**`) and ES (`###.743.147-##`) there is no partial identifier at all
# for natural persons.
RS_NULL_DOCUMENT = "000.000.000-00"


# --------------------------------------------------------------------------- SC

# **SC is ingested from the portal's own export endpoint, NOT from its CKAN bulk
# files.** `dados.sc.gov.br` does publish `empenhos-<ano>.csv`, and those files cannot
# be parsed: `dehistoricoempenho` is free text carrying both embedded newlines and
# semicolons while the fields are effectively unquoted (705 double quotes in 400k
# lines). Splitting on `;` yields the correct 34 fields on only 32,444 of ~200,000
# physical lines. With no quoting there is no way to recover record boundaries from the
# text, so `strict_mode=false` would mis-parse rather than reject -- the BA lesson.
#
# The export endpoint returns the same data correctly quoted, and is better in three
# further ways: all three phases instead of empenho alone, 2011+ instead of 2021+, and
# two extra columns. The CKAN files are kept only as an independent cross-check.
SC_API = "https://api-portal-transparencia.apps.sm.okd4.ciasc.sc.gov.br/api"
SC_CKAN = "https://dados.sc.gov.br/api/3/action/package_show"

# `visao` is the REQUIRED discriminator. Omitting it, or sending the `tipoconsulta`
# name that reads more naturally, returns HTTP 422 from `exportcsv` -- and a bare `[]`
# from `/documentos`, which looks like "no data" rather than a bad request.
#
# Only these three exist. The portal renders cards for `pagamento-extraorcamentario`
# and `retencao` as well, but both are 422 here, so extraorçamentário is reachable only
# through the frozen CKAN snapshot.
SC_VISOES = ("empenho", "liquidacao", "pagamento")

# Period parameters, read out of the portal's Angular bundle: it builds
# `documentos/exportcsv?<solrParams>` with `anomesinifiltro`/`anomesfimfiltro` set to
# `f"{ano}{mes:02d}"`. Harvest a month at a time -- one month of empenho is ~12.6 MB
# and the whole series is ~26M rows.
SC_PERIOD_PARAMS = ("anomesinifiltro", "anomesfimfiltro")

# 2010 and earlier return 0 rows for all three visões.
SC_FIRST_YEAR, SC_LAST_YEAR = 2011, 2026

SC_TABLES = {
    "empenho": "sc_empenho",
    "liquidacao": "sc_liquidacao",
    "pagamento": "sc_pagamento",
}

SC_SEP = ";"

# **cp1252, despite `Content-Type: text/csv; charset=UTF-8`.** duckdb answers
# `Invalid Input Error: File is not latin-1 encoded`, the same signature as RS. The
# header is wrong, not the data; transcode before reading. Python's latin-1 would
# decode it silently and ship mojibake, so never "try utf-8, else latin-1" here.
# "utf-8 strict, else cp1252" IS a valid discriminator, because UTF-8 is
# self-validating and cp1252 leaves five bytes undefined -- unlike latin-1, which
# accepts anything.
SC_ENCODING = "cp1252"

# Comma decimal, no thousands separator (`25742,5`) -- the BA and ES convention, and
# the opposite of SP. `try_cast` without `replace(',', '.')` NULLs every non-integer
# value silently: on 2022-03 that turned R$2.90bn into R$632M, a wrong number that
# looked entirely plausible.
SC_DECIMAL_COMMA = True

# `cdtipoempenho` 3 (Anulação) and 4 (Estorno) are **already signed negative** in the
# export, so a plain SUM is the correct net. Do not take absolute values and do not
# filter them out.
SC_TIPO_EMPENHO = {1: "Emissão", 2: "Reforço", 3: "Anulação", 4: "Estorno"}

# The JSON sibling of `exportcsv` reports `lista.total` for the same filters, which is
# the control total for the CSV: on 2022-03 empenho both are 13,402 and the four
# `cdtipoempenho` subtotals match the export to the cent. download_sc verifies every
# month against it, so a truncated export is rejected rather than stored short.
SC_COUNT_ENDPOINT = "documentos"
SC_EXPORT_ENDPOINT = "documentos/exportcsv"

# SC ships one document per phase, like RS, and the phases are natively keyed:
# liquidação carries `nunotaempenhooriginal`, and pagamento carries `nunotaliquidacao`
# AND `nunotaempenhooriginal` AND `nuordembancaria`. Only PB otherwise publishes the
# complete chain. The pivot onto the canonical `despesa` row happens in dbt.
SC_EMPENHO_KEY = "nunotaempenho"
SC_EMPENHO_FK = "nunotaempenhooriginal"

# CPFs are masked `***.997.739-**` (the MG convention); CNPJs are published in full.
SC_MASKED_CPF_MARKER = "*"


# --------------------------------------------------------------------------- PB

# Paraíba publishes a real REST API behind its CKAN shopfront. `dados.pb.gov.br` is a
# catalogue with 8 packages; the data is here, with an OpenAPI spec at
# `/api/v1/swagger.json` describing 39 endpoints. (The `/swagger/` UI path does not
# serve the spec -- read the URL out of the HTML, or go straight to swagger.json.)
PB_API = "https://api.dados.pb.gov.br/api/v1"

# `ano` and `mes` are REQUIRED on every despesas endpoint, which is a gift rather than a
# constraint: the harvest chunks itself and an incremental refresh is free.
# `/compras/*` takes `ano` only.
PB_DESPESA_ENDPOINTS = {
    "notas_empenho": "pb_empenho",
    "liquidacoes": "pb_liquidacao",
    "ordem_cronologica_pagamentos": "pb_pagamento",
}
PB_COMPRAS_ENDPOINTS = {
    "contratacoes": "pb_contratacao",
    "itens_contratacoes": "pb_contratacao_item",
}

# `per_page` is capped at 1000; 2000 returns HTTP 400 rather than silently truncating.
PB_PER_PAGE = 1000

# Two non-200s mean "this period is not served", and they are not the same thing:
# 2014 and earlier return **HTTP 400** (before coverage begins), while a month that has
# not happened yet returns **HTTP 404**. Neither is an empty result, and an "empty means
# no data" reader would record a rejection as a genuine gap. Handling only the 400
# crashes the sweep at the first future month of the open exercise.
PB_FIRST_YEAR, PB_LAST_YEAR = 2015, 2026

# Response envelope: {"dados": [...], "paginacao": {total, pagina, itens_por_pagina,
# total_paginas}}. `paginacao.total` is the control total for the harvest, the same role
# `lista.total` plays for SC.
PB_ENVELOPE_ROWS = "dados"
PB_ENVELOPE_PAGE = "paginacao"

# **PB is the only source here with all three phase values on ONE empenho row**
# (`valorEmpenhado`, `valorLiquidado`, `valorPago`, plus `valorAnulado`,
# `valorSuplementado` and `valorPagamentoAnulado`). So unlike SC and RS it needs no
# pivot -- it maps onto `despesa` directly, the way MG and ES do.
#
# It also publishes `codigoMunicipio` / `nomeMunicipio`, the municipality of the spend,
# which only RS otherwise carries. The canonical `despesa` schema has no column for it.
PB_EMPENHO_VALUES = ("valorEmpenhado", "valorLiquidado", "valorPago")

# `numeroEmpenho` is an INTEGER that restarts per exercise and per unit. Measured over
# the whole of 2024 (320,201 rows):
#
#   numeroEmpenho                     40,626 distinct   fan-out x7.88
#   (ano, unidade, numero)           317,300 distinct   fan-out x1.01  (2,773 collisions)
#   (ano, orgao, unidade, numero)    320,201 distinct   UNIQUE
#
# So the órgão is load-bearing: a unidade code is reused across órgãos, and dropping it
# collides ~5,700 rows a year. `numeroEmpenho` alone fans out nearly eightfold.
PB_EMPENHO_KEY = ("ano", "codigoOrgao", "codigoUnidade", "numeroEmpenho")

# `participantes` and `documentos` arrive as **JSON strings**, not nested arrays, so they
# must be json.loads()'d before use -- iterating the raw value walks characters and a
# length check reports the string length (240) rather than the record count (1).
PB_JSON_STRING_FIELDS = ("participantes", "documentos")

# Participant records carry `lote, item, quantidade, cnpj, razao_social, nome_fantasia,
# valor_ofertado, valor_licitado, valor_total_licitado` and **no win/lose flag**, at
# roughly 1.8 per tender -- these are awarded suppliers, not the full bidder list. So
# `vencedor` is left NULL rather than derived. Deriving it is the BA mistake: there,
# 84% of rows labelled `Perdedor` also carried a positive homologated value.
PB_HAS_LOSING_BIDS = False


# --------------------------------------------------------------------------- RJ

# **The catalogue is at `dadosabertos.rj.gov.br`, not `dados.rj.gov.br`.** The latter is
# NXDOMAIN and `transparencia.rj.gov.br` is a WordPress brochure, which is why earlier
# surveys filed RJ as geo-blocked. It is not: the hostname was wrong.
#
# The host does, separately, require a Brazilian IP, and it drops TLS connections
# mid-download often enough that every fetch needs retries.
RJ_CKAN = "https://dadosabertos.rj.gov.br/api/3/action/package_show"

# SEFAZ's `tfe-despesa` is the only real fiscal series in a 1,119-package catalogue whose
# fiscal content is otherwise a per-agency PDF dump. It is described as a D+1 mirror of
# SIAFE-Rio.
RJ_PACKAGE = "tfe-despesa"
RJ_TABLE = "rj_despesa"

# **RJ is NOT transaction grain and must not be advertised as such.** `Posição` is a
# month (`07/2025`), there is no creditor and no empenho number. It is a month x
# budget-line aggregate carrying Empenhado / Liquidado / Pago / Dotado Atual / Dotação
# Inicial / Despesa Autorizada on one row -- exactly the `despesa_mensal` grain that
# already exists for Bahia.
RJ_FIRST_YEAR, RJ_LAST_YEAR = 2016, 2025

# The export carries **five preamble lines** before the header:
#   Governo do Estado do Rio de Janeiro / Secretaria de Estado de Fazenda /
#   Subsecretaria de Politica Fiscal / Transparência Fiscal /
#   Despesa Generica entre 01/01/2025 e 01/12/2025
# A reader that assumes row 0 is the header silently treats the first data row as names.
RJ_PREAMBLE_LINES = 5

RJ_SEP = ";"
RJ_ENCODING = "cp1252"

# Dimension columns are quoted, value columns are not, and the decimal separator is a
# comma with no thousands separator -- the BA/ES/RS/SC convention.
RJ_DECIMAL_COMMA = True

# **2019 and 2021 are each published TWICE** as separate resources of identical size.
# The ES `Despesas-2013.csv` trap: a name-keyed dict silently keeps one, and a glob over
# the resource list double-counts the year. Resources are therefore de-duplicated on the
# CKAN resource id and the exercise is taken from the file name.
RJ_DUPLICATED_YEARS = (2019, 2021)

# --------------------------------------------------------------------------- CE

# Ceará publishes SIAFE-CE's three execution documents on `cearatransparente.ce.gov.br`
# -- empenho from 2006, pagamento from 2012, liquidação from 2015. That is the
# second-longest series in this dataset after MG, and the only other complete
# empenho -> liquidação -> pagamento chain besides SC and PB.
#
# **The portal is double-gated, and the two gates are unrelated.**
#
# 1. A **Brazilian IP** is required. The same `/attachments/` URL answers 200 from São
#    Paulo and 403 from a US or Australian address. This is the one confirmed
#    country/ASN block in the dataset (RS turned out to be path-dependent, not
#    country-dependent), so CE is the canary to probe reachability with.
#
# 2. The **HTML pages carry a JS anti-bot challenge**. Every dataset detail page
#    returns HTTP 200 with an identical ~247 KB challenge body -- measured 2026-09-16
#    at 246,688 bytes for id 168 and 246,689 for id 179, a one-byte difference for two
#    completely different datasets. **The invariant length is the tell**, not the status
#    code. `curl_cffi` TLS impersonation does not pass it. The portal *homepage* is
#    served normally (41,195 bytes), so "the host answers" proves nothing about the
#    catalogue.
#
# `/attachments/` is NOT challenged, so the download needs no cookie and no browser --
# see CE_FORCE_DOWNLOAD for the parameter it does need.
CE_BASE = "https://cearatransparente.ce.gov.br"
CE_ATTACHMENTS = f"{CE_BASE}/attachments"

# **`?force_download=true` is REQUIRED on every attachment URL.** Without it the server
# answers **HTTP 403 with a 9-byte `forbidden` body** -- for a URL that returns the file
# perfectly when the parameter is present. Measured 2026-09-16 by alternating the two
# forms against one URL four times: 403 / 206 / 403 / 206, deterministic and repeatable
# within seconds, so it is the parameter and not a rate limit.
#
# This matters far more than it looks. A previous run recorded CE as IP-blocked twice
# and backed off 60/120/180/240 s between retries, because its downloader built the URL
# without the parameter and read the 403 as a block. The response is indistinguishable
# from a real block by status code alone. **Before concluding CE has blocked you, check
# that the parameter is on the URL.**
CE_FORCE_DOWNLOAD = "force_download=true"

# A blocked or refused body is still a success-looking stream: 9 bytes whose first four
# are `666f7262`, ASCII `forb`. A downloader checking only `status_code` writes 216
# small files that look like data. Check the magic bytes and refuse a short body.
CE_FORBIDDEN_MAGIC = b"forb"
CE_MIN_BODY_BYTES = 512

# **Enumeration has to happen in a browser.** The catalogue LIST page renders its table
# by AJAX, so `requests` sees it empty even with a valid cookie, and the detail pages
# are challenged (above). What works is same-origin `fetch()` loops run inside the page
# with the browser tools, ~300 ms apart and chunked to ~40 ids per call: 200 requests
# produced zero challenges. Ids run 1-200 with 52 gaps and yield exactly the 148
# datasets the portal reports, over 698 files.
#
# The result is checked in as `ce_manifest.txt` (`id|title|<sha1>/store/<sha256>/<name>`)
# so the download is reproducible without a browser. **It is a seed, not truth.**
# Attachment URLs are content-addressed and rotate when a file is republished, which is
# daily for the empenho datasets. `download_ce.py` reports any entry that 404s so it can
# be re-enumerated; the 2026-09-15 capture was still live in full 12 hours later.
CE_MANIFEST = Path(__file__).resolve().parent / "ce_manifest.txt"

# `api-dados-abertos.cearatransparente.ce.gov.br` is a **pilot covering only contratos,
# convênios and servidores** -- no empenho, liquidação or pagamento. It reads like the
# data route and is not one.
CE_API_IS_NOT_THE_DATA_ROUTE = True

CE_TABLES = {
    "empenho": "ce_empenho",
    "liquidacao": "ce_liquidacao",
    "pagamento": "ce_pagamento",
}

# Phase is read off the dataset TITLE, which is stable, rather than from the file name,
# which is not: the same phase appears as `NLD_Ano2022_1Bimestre.csv`, `NLD+4+Bimestre.csv`,
# `NLD+5+BI.xls` and `1+BI+NLD.xls` within three exercises.
CE_TITLE_PHASE = (
    ("nota de pagamento", "pagamento"),
    ("notas de liquida", "liquidacao"),
    ("notas de empenho", "empenho"),
    ("despesas por nota de empenho", "empenho"),
)

# **The year in a dataset title is a SLOT, not the data's year.** Dataset 135, titled
# "Notas de Liquidação de Despesa - 2020", contains two files named `NLD_Ano2019_*`.
# This is the RS missing-month trap in a new costume: the catalogue's own organisation
# disagrees with the data. Partition by the exercise column the rows carry, never by the
# dataset the file was filed under.
CE_SLOT_YEAR_IS_UNRELIABLE = True

# **2023 empenho is published TWICE**: dataset 145 as four quarterly files (three .xlsx
# plus one .ods) and dataset 152 as one consolidated `notas_de_empenho_2023.csv`.
# Staging both double-counts the exercise. The consolidated CSV is the ingest path --
# one file, one schema, no .ods reader needed -- and 145 is kept out of staging and used
# only as an independent cross-check.
CE_EMPENHO_DUPLICATE_SLOT = 145

# Five file formats across the series, and the split is by era, not by phase:
# `.rar` 2006-2017 · `.csv` throughout · `.csv.zip`/`.zip` · `.xlsx` 2019-2023 and the
# 2024 consolidated file · `.ods` for 2023-Q4 only · `.xls` 2024+. `.rar` needs
# `rarfile` with an external unrar/bsdtar, never `zipfile`. Dropping dataset 145 removes
# the only `.ods` from the ingest path.
CE_ARCHIVE_SUFFIXES = (".rar", ".zip")
CE_SPREADSHEET_SUFFIXES = (".xlsx", ".xls", ".ods")

# Chunking differs by phase: **trimestre for empenho, bimestre for NPD/NLD**, and a
# chunk is sometimes split into parts (`NPD+5BI1.csv` / `NPD+5BI2.csv`,
# `NPD+6bim+-+parte+1.csv` / `...+parte+2.csv`).
#
# **Dataset 170 lists `NPD+4BI.csv` TWICE with different content hashes.** A
# name-keyed dict silently keeps one of them; the download therefore keys on the
# content hash and the cleaner de-duplicates on what the rows actually are.
CE_DUPLICATE_NAME_SLOT = 170

# UTF-8 throughout -- the one encoding question this dataset does not have to ask.
CE_ENCODING = "utf-8"

# **The field separator is a COMMA and the decimal separator is ALSO a comma.** Values
# carrying a decimal are quoted (`"14145,17"`) and integers are not (`61200`), so a
# quote-aware reader places the fields correctly and the decimal fix is a separate
# step. Splitting on `,` without honouring quotes shifts every row that has a
# non-integer value.
CE_SEP = ","

# **One file of the 216 uses a different dialect entirely.**
# `notas_de_empenho_2026.csv` (dataset 179) is semicolon-separated, fully quoted, and
# **cp1252** -- its first eight bytes are `"Exerc\xedcio"`. Every other file is
# comma-separated and UTF-8. So both the delimiter and the encoding are probed per file:
# parsed as comma, that file yields a single 1-column header and the failure reads like a
# schema change rather than a parse error.
CE_SEP_CANDIDATES = (",", ";")
CE_FALLBACK_ENCODING = "cp1252"

# The decimal separator is **not** a property of CE, it is a property of the era, and
# they disagree:
#   legacy .rar 2006-2018  `8100.00`      dot
#   2018 csv               `391653.84`    dot
#   modern csv 2019+       `"14145,17"`   comma, and the value is quoted because of it
#   modern .xlsx/.xls      native numbers, rendered here with a dot
# Staging is all-STRING, so nothing is converted here and nothing is lost -- but a dbt
# model that applies one convention to the whole table will null three of the four.
CE_DECIMAL_COMMA = True

# Grain is **empenho x budget line**, not empenho: 2025 has 196,255 rows over 30,600
# distinct `Número`. Dimensions are published as **labels only, with no codes** --
# `Função`, `Subfunção`, `Programa de governo`, `Natureza da despesa` and
# `Fonte de recurso` are free text, so nothing joins to a directory.
#
# `Credor` is a NAME carrying a partial CNPJ/CPF prefix in the same field
# (`54.212.382 FELLIPE BARBOSA DA SILVA`), not two columns.
CE_EMPENHO_KEY = ("exercicio", "numero")

# The exercise column, named differently in each era: `exercicio` from 2014, `num_ano`
# in the 2006-2013 empenho and 2012-2016 pagamento exports. Rows are partitioned on
# whichever of these the file carries -- never on the dataset's slot year, because the
# catalogue files data under the wrong one (dataset 135 is titled 2020 and holds
# `NLD_Ano2019_*`).
CE_EXERCICIO_COLUMNS = ("exercicio", "num_ano")

# **Every schema in the series, measured file by file from the 2026-09-16 download.**
# Six eras for empenho, three for pagamento, two for liquidação -- and the names are not
# a restyling of one schema. The eras publish different THINGS:
#
#   2006-2013 empenho  `cod_gestora`, `cod_credor`, `cod_item_natureza`  -- codes, plus
#                      `cod_ne_original` and `cod_tipo_empenho`, i.e. a movement model
#                      like SC's, which no later era carries.
#   2014-2018 empenho  `unidadegestora` + `cpfcnpjcredor` + `razaosocialcredor` -- codes
#                      AND names, plus the tender and contract columns.
#   2019+     empenho  `unidade_gestora`, `funcao`, `natureza_da_despesa` -- LABELS only.
#                      No CNPJ, no budget code, no contract, no tender.
#
# Folding those onto shared column names would put codes and labels in one field and
# claim a continuity the source does not have. Each era therefore keeps its own names,
# and every partition is written to the superset (see `clean_ce._superset`).
#
# A header matching none of these is a hard failure. That is the PE lesson: modelling
# only the modern names there left 1,031,326 rows present and entirely NULL, and the row
# total never showed it.
CE_SCHEMAS: dict[str, dict[str, list[str]]] = {
    "empenho": {
        # 32 file(s), exercises 2006-2013, datasets [70, 71, 72, 73, 74, 75, 76, 77]
        "2006_2013": [
            "num_ano",
            "cod_gestora",
            "cod_ne",
            "num_classificacao",
            "cod_credor",
            "dsc_nome_credor",
            "cod_licitacao_modalidade",
            "cod_item_natureza",
            "dth_empenho",
            "cod_projeto",
            "num_convenio",
            "num_contrato",
            "cod_movimento",
            "cod_ne_original",
            "vlr_empenhado",
            "vlr_suplementado",
            "vlr_anulado",
            "vlr_pago",
            "vlr_anulado_pago",
            "cod_licitacao_dispositivo",
            "dsc_linha",
            "dth_processamento",
            "cod_projeto_finalistico",
            "cod_projeto_finalistico_digito",
            "cod_tipo_empenho",
        ],
        # 19 file(s), exercises 2014-2018, datasets [31, 32, 59, 67, 68]
        "2014_2018": [
            "exercicio",
            "unidadegestora",
            "numero",
            "natureza",
            "efeito",
            "dataemissao",
            "unidadeexecutora",
            "classiforcamreduz",
            "classiforcamcompl",
            "itemdespesa",
            "credor",
            "cpfcnpjcredor",
            "razaosocialcredor",
            "projeto",
            "numeroconvenio",
            "numerocontrato",
            "modalidadelicitacao",
            "modalidadesemlicitacao",
            "codigodispositivolegal",
            "numeroprocessoprotorig",
            "especificacaogeral",
            "valor",
            "valorpago",
            "valorordinario",
            "valoranulado",
            "valorsuplementado",
            "valoranuladopago",
            "dt_atual",
            "dt_etl",
            "hr_etl",
            "valorliquidado",
            "isn_seq",
        ],
        # 1 file(s), exercises 2018-2018, datasets [31]
        "2018_portal": [
            "exercicio",
            "numero",
            "data_de_emissao",
            "unidade_gestora",
            "unidade_executora",
            "unidade_orcamentaria",
            "razao_social_credor",
            "funcao",
            "subfuncao",
            "programa_de_governo",
            "acao_de_governo",
            "regiao_administrativa",
            "natureza_da_despesa",
            "fonte_de_recurso",
            "tipo_da_despesa",
            "valor_empenhado",
            "valor_pago",
            "valor_final",
            "valor_pago_final",
            "valor_suplementado",
            "valor_anulado",
            "valor_pago_anulado",
        ],
        # 1 file(s), exercises 2019-2019, datasets [117]
        "2019t4_reduzido": [
            "exercicio",
            "numero",
            "data_de_emissao",
            "secretaria_orgao",
            "unidade_executora",
            "beneficiario",
            "valor_pago",
        ],
        # 18 file(s), exercises 2019-2023, datasets [117, 123, 129, 140, 145]
        "2019_2023": [
            "exercicio",
            "numero",
            "data_de_emissao",
            "unidade_gestora",
            "unidade_executora",
            "unidade_orcamentaria",
            "razao_social_do_credor",
            "funcao",
            "subfuncao",
            "programa_de_governo",
            "acao_de_governo",
            "regiao_administrativa",
            "natureza_da_despesa",
            "fonte_de_recurso",
            "tipo_da_despesa",
            "valor_empenhado",
            "valor_pago",
            "valor_empenhado_final",
            "valor_pago_final",
            "valor_suplementado",
            "valor_anulado",
            "valor_pago_anulado",
        ],
        # 4 file(s), exercises 2023-2026, datasets [152, 159, 168, 179]
        "2023_2026": [
            "exercicio",
            "numero",
            "data_de_emissao",
            "unidade_gestora",
            "unidade_executora",
            "unidade_orcamentaria",
            "credor",
            "funcao",
            "subfuncao",
            "programa_de_governo",
            "acao_de_governo",
            "regiao_administrativa",
            "natureza_da_despesa",
            "fonte_de_recurso",
            "tipo_da_despesa",
            "valor_empenhado",
            "valor_pago",
            "valor_empenhado_final",
            "valor_pago_final",
            "valor_suplementado",
            "valor_anulado",
            "valor_pago_anulado",
        ],
    },
    "liquidacao": {
        # 12 file(s), exercises 2015-2017, datasets [35, 61, 62]
        "2015_2017": [
            "exercicio",
            "unidadegestora",
            "numero",
            "natureza",
            "efeito",
            "processoadmdespesa",
            "unidadeexecutora",
            "dataemissao",
            "valor",
            "valorretido",
            "valoranulado",
            "cpfordenadordespesa",
            "credor",
            "numeroned",
            "numeronldordinario",
            "exercicioanterior",
            "dt_etl",
            "hr_etl",
            "exerciciorestosapagar",
            "tipodocdespesa",
            "numerodocdespesa",
            "datadocdespesa",
            "isn_seq",
            "numeronpfordinaria",
        ],
        # 50 file(s), exercises 2018-2026, datasets [36, 134, 135, 136, 142, 147, 161, 172, 187]
        "2018_2026": [
            "exercicio",
            "unidade_gestora",
            "unidade_executora",
            "numero",
            "natureza",
            "tipo_de_documento_da_despesa",
            "numero_do_documento_da_despesa",
            "data_do_documento_da_despesa",
            "efeito",
            "processo_administrativo_despesa",
            "data_emissao",
            "valor",
            "valor_retido",
            "cpf_ordenador_despesa",
            "credor",
            "numero_nota_empenho_despesa",
            "exercicio_restos_a_pagar",
            "data_atual",
        ],
    },
    "pagamento": {
        # 20 file(s), exercises 2012-2016, datasets [60, 63, 64, 65, 66]
        "2012_2016": [
            "num_ano",
            "cod_gestora",
            "cod_np",
            "cod_tipo_credito",
            "cod_classificacao",
            "cod_classificacao_orc",
            "cod_credor",
            "dsc_credor",
            "cod_sit_pagamento",
            "cod_tipo_nota_pagamento",
            "cod_gestora_origem_pagamento",
            "cod_suprimento_fundo",
            "cod_ne",
            "cod_num_cheque",
            "cod_contabil_despesa",
            "cod_contabil_credito",
            "cod_grupo_financeiro",
            "cod_credor_transferencia",
            "cod_retencao_cheque_salario",
            "dth_banco",
            "cod_orgao_orcamentaria",
            "dth_movimento",
            "dth_processamento",
            "cod_liberacao_recurso_fianceiro",
            "num_mes_ano_competencia",
            "cod_banco",
            "cod_agencia",
            "cod_conta_corrente",
            "cod_forma_pagamento",
            "cod_natureza_despesa",
            "cod_fonte_recurso",
            "vlr_pagamento",
            "vlr_anulacao_pagamento",
            "cod_desconto1",
            "vlr_desconto1",
            "cod_desconto2",
            "vlr_desconto2",
            "cod_desconto3",
            "vlr_desconto3",
            "cod_desconto4",
            "vlr_desconto4",
            "cod_desconto5",
            "vlr_desconto5",
            "num_cpf_cpj",
            "cod_tipo_regime",
            "cod_gestora_emp",
            "num_ano_emp",
            "dsc_linha",
            "cod_tipo_movimento",
        ],
        # 4 file(s), exercises 2017-2017, datasets [34]
        "2017": [
            "exercicio",
            "unidadegestora",
            "numero",
            "natureza",
            "unidadeexecutora",
            "codigoretencao",
            "justificativa",
            "efeito",
            "processoadmdespesa",
            "numeronld",
            "numeronpdordinario",
            "credor",
            "documentocredor",
            "valor",
            "servicobancario",
            "bancoorigem",
            "agenciaorigem",
            "contaorigem",
            "bancopagto",
            "bancobeneficiario",
            "agenciabeneficiario",
            "contabeneficiario",
            "statusmovbancario",
            "dataretornoremessabancaria",
            "grupofin",
            "dataemissao",
            "classiforcamreduz",
            "classiforcamcompl",
            "dt_etl",
            "hr_etl",
            "isn_seq",
            "nomecredor",
            "cpfcnpjcredor",
            "numeronpf",
            "numeroned",
            "valoranulado",
        ],
        # 54 file(s), exercises 2018-2026, datasets [33, 131, 132, 133, 141, 146, 160, 170, 186]
        "2018_2026": [
            "exercicio",
            "unidade_gestora",
            "unidade_executora",
            "numero",
            "natureza",
            "justificativa",
            "efeito",
            "numero_processo_administrativo_despesa",
            "data_emissao",
            "credor",
            "documento_credor",
            "valor",
            "numero_nld_ordinaria",
            "servico_bancario",
            "banco_origem",
            "agencia_origem",
            "digito_agencia_origem",
            "conta_origem",
            "digito_conta_origem",
            "banco_pagamento",
            "banco_beneficiario",
            "agencia_beneficiario",
            "digito_agencia_beneficiario",
            "conta_beneficiario",
            "digito_conta_beneficiario",
            "status_movimento_bancario",
            "data_retorno_remessa_bancaria",
            "data_atual",
        ],
    },
}

# The column each era writes unquoted free text into. `_repair` rejoins a split row
# HERE rather than searching for a position that type-checks: most columns in a legacy
# row are empty, joining two empty fields is a no-op, and a search therefore finds
# fifteen equally "valid" answers. Declaring the column and then type-checking the
# result is the only version of this that discriminates.
CE_FREE_TEXT_COLUMNS = (
    "especificacaogeral",  # empenho 2014-2018
    "dsc_linha",  # empenho 2006-2013
    "justificativa",  # pagamento, all eras
    # The 2018 modern-schema zip splits on a creditor NAME, not a description:
    # `F, TARCISIO G. PARENTE - ME` written unquoted, 10 rows of 63,406.
    "razao_social_do_credor",
    "razao_social_credor",
    "razaosocialcredor",  # legacy spelling: `...,3301349000151,F,8825,2200010012015C,...`
    "dsc_nome_credor",
    "dsc_credor",
)

# **A header rendered by a failed i18n lookup.** In
# `notas-de-empenho-ago-dez-2018.csv.zip` the creditor column is literally named
#   translation missing: pt-BR.integration/expenses/ned.spreadsheet.worksheets
#   .default.header.razao_social_credor
# That is a rendering bug in the exporter, not a column name, and carrying it into the
# staging schema would add a 110-character column that means the same as a column the
# next exercise names properly. It is renamed here, visibly, rather than silently: the
# i18n key itself ends in `razao_social_credor`, so the intent is not in doubt.
#
# The second entry is a different breakage with the same remedy. In
# `npd-2013-quarto-trimestre.rar` the first column is named `are` -- the other three
# quarters of 2013 name it `num_ano`, the remaining 48 names are identical, and every
# value in the column is `2013`. One file's header was mangled; the data is fine.
CE_HEADER_FIXES = {
    "translation_missing_pt_br_integration_expenses_ned_spreadsheet_worksheets_default_header_razao_social_credor": "razao_social_credor",
    "are": "num_ano",
}

# **The 2018+ CSV exports write the four-character string `NULL` instead of an empty
# field.** Measured across the downloaded set: 36,760,936 occurrences in 104 files --
# every modern liquidação and pagamento file, and no empenho file. It is not confined
# to a few columns: `efeito` carries 2,214,732 of them against ~4.5M pagamento rows,
# roughly half the column, and so do `servico_bancario`, `banco_pagamento`,
# `data_atual` and the account-digit columns.
#
# Staged verbatim this is a string that is not null, so `count(x)` counts 2.2M phantom
# values and a `group by` grows a "NULL" category. It is converted to a real null on
# write -- the only value this cleaner changes, and the reason is that the source is
# spelling a null, not recording one. No legitimate value in these columns (dates,
# bank codes, single digits) is the word NULL.
CE_NULL_SENTINEL = "NULL"

# How many separators a single unquoted free-text value may contain before the cleaner
# refuses to reassemble it. It has to be generous: `especificacaogeral` sometimes holds a
# comma-separated LIST -- names of course participants, or seized equipment with model
# and serial numbers -- so one unquoted value can split a row into dozens of fields.
# The cap only bounds the search; every candidate is still type-checked, and the smallest
# absorb that validates wins.
CE_MAX_SPLIT = 60
