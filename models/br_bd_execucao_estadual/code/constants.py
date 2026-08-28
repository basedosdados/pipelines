"""Source constants for br_bd_execucao_estadual.

State-government budget execution and procurement. One section per UF, because the
sources have nothing in common beyond what they describe.
"""

from pathlib import Path

DATASET_ID = "br_bd_execucao_estadual"

# Scratch lives outside the repo and outside Dropbox: multi-GB, fully reproducible,
# and deleted at the end of onboarding (see .claude/rules/onboarding-workflow.md).
DATA_DIR = Path.home() / "Downloads" / "br_state_budget_data"
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
    # procurement (compras_contratos)
    "dm_processo": "mg_dm_processo",
    "dm_item_matserv": "mg_dm_item_matserv",
    "dm_linha_fornec": "mg_dm_linha_fornec",
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
BA_PACKAGES = {
    "despesas": "518569da-ccaa-4621-b8d2-e7424ec3f1ea",
    "licitacoes": "36c792f9-1999-4f21-a669-752a178b06b7",
    "contratos": "edbe8b9a-c363-457e-8b06-4e1c1f693cb8",
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
