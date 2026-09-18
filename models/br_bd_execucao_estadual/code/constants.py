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
