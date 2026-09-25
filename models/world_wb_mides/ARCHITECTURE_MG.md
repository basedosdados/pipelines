# MiDES / MG — full-source architecture

Onboarding **every** stream TCE-MG publishes for Minas Gerais, not just the four
MiDES has used since launch. Status: **built, pending verification**. The 43 new
models, the 3 extended ones and `mg/schema.yml` are written; the staging mirrors
are uploaded; the BigQuery build and its tests are the remaining step.

`recLicitacao` and `recDispensa` are funding-allocation tables (`dsc_dotacao`,
`valor_recurso`), not appeals, so they land as `licitacao_dotacao` and
`dispensa_dotacao`. The names below reflect that.

## Why this exists

MiDES consumes 4 of the **51** CSV members in the MG source archives. Everything
else — all contract data, all procurement detail, all invoice and funding-source
detail — is downloaded, verified on disk, and unused. Concretely, MG spending is
being refreshed to 2026 while MG procurement is frozen at 2021 and contracts have
never been onboarded at all.

## Measured facts this design rests on

| fact | measurement |
|---|---|
| Streams in the source | 51 (contrato 8, despesa 11, empenho 8, licitacao 24) |
| Already consumed | 4 — `empenho`, `rsp`, `liquidacao`, `pagamento` |
| Always empty | 2 — `credLicitacao`, `refLicitacao`: 0 rows in 2014/2017/2021/2024/2026 |
| **Schema drift across 2014-2026** | **none** — all 51 headers byte-identical in 2014/16/18/20/22/24/26 |
| New volume | ~51M rows/year, ~664M rows over 13 exercises |

Zero schema drift is the single most load-bearing fact here: it removes per-table
union logic from every one of the 46 tables.

## Table map

46 tables: 43 new, 3 extended, 2 dropped.

`empenho`, `liquidacao` and `pagamento` keep their table definitions, but their MG
arms are NOT untouched: this work rebuilds them from the current extraction, which
extends MG coverage from 2021 to 2026 and rewrites `id_empenho_bd`,
`id_liquidacao_bd` and `id_pagamento_bd` for every MG row. `orgao` is now
`cod_orgao` rather than the notebook's `seq_orgao`, so those keys do not match the
values published for MG 2014-2021. Downstream joins against the old MG keys must be
rebuilt. Rows for the other states are untouched.

### Procurement — `licitacao` category

| stream | table | status | parent | rows/yr |
|---|---|---|---|---|
| licitacao | `licitacao` | EXTEND | spine | 29,471 |
| itemLicitacao | `licitacao_item` | EXTEND | licitacao | 1,089,408 |
| habLicitacao | `licitacao_participante` | EXTEND | licitacao | 73,571 |
| julgLicitacao | `licitacao_julgamento` | NEW | licitacao_item | 2,138,982 |
| cotacaoLicitacao | `licitacao_cotacao` | NEW | licitacao_item | 1,083,523 |
| homologLicitacao | `licitacao_homologacao` | NEW | licitacao | 1,023,621 |
| respLicitacao | `licitacao_responsavel` | NEW | licitacao | 239,799 |
| quadroSocLicitacao | `licitacao_quadro_societario` | NEW | licitacao_participante | 143,623 |
| comissaoLicitacao | `licitacao_comissao` | NEW | licitacao | 111,252 |
| recLicitacao | `licitacao_dotacao` | NEW | licitacao | 79,883 |
| parecLicitacao | `licitacao_parecer` | NEW | licitacao | 52,416 |
| dispensa | `dispensa` | NEW | spine | 16,569 |
| itemDispensa | `dispensa_item` | NEW | dispensa | 98,308 |
| cotDispensa | `dispensa_cotacao` | NEW | dispensa_item | 98,286 |
| respDispensa | `dispensa_responsavel` | NEW | dispensa | 91,761 |
| fornDispensa | `dispensa_fornecedor` | NEW | dispensa | 63,783 |
| credDispensa | `dispensa_credenciado` | NEW | dispensa | 49,964 |
| recDispensa | `dispensa_dotacao` | NEW | dispensa | 26,592 |
| regadesao | `registro_preco_adesao` | NEW | spine | 4,286 |
| itemRegadeso | `registro_preco_adesao_item` | NEW | registro_preco_adesao | 35,975 |
| cotRegadesao | `registro_preco_adesao_cotacao` | NEW | ..._item | 35,036 |
| vencRegadesao | `registro_preco_adesao_vencedor` | NEW | ..._item | 34,759 |
| credLicitacao | — | DROP | — | 0 |
| refLicitacao | — | DROP | — | 0 |

`licitacao_julgamento` is the item-level award record — who bid what, per item.
It is the richest stream in the whole set and has never been in MiDES.

### Contracts — `contrato` category (none of this exists in MiDES today)

| stream | table | parent | rows/yr |
|---|---|---|---|
| contratos | `contrato` | spine | 67,621 |
| itemContrato | `contrato_item` | contrato | 612,645 |
| creditoContrato | `contrato_credito` | contrato | 161,622 |
| contContrato | `contrato_contabilizacao` | contrato | 67,088 |
| termoContrato | `contrato_termo_aditivo` | contrato | 60,562 |
| itemTrmContrato | `contrato_termo_aditivo_item` | contrato_termo_aditivo | 21,623 |
| apostContrato | `contrato_apostilamento` | contrato | 4,670 |
| resContrato | `contrato_rescisao` | contrato | 789 |

### Expenditure detail — `despesa` and `empenho` categories

| stream | table | parent | rows/yr |
|---|---|---|---|
| liquidacaoFonte | `liquidacao_fonte` | liquidacao | 8,755,597 |
| movPagamento | `pagamento_movimento` | pagamento | 7,928,144 |
| empenhoFonte | `empenho_fonte` | empenho | 7,306,179 |
| liquidacaoNfs | `liquidacao_nota_fiscal` | liquidacao | 4,706,747 |
| despesa | `despesa_dotacao` | spine | 4,537,064 |
| notasfiscais | `nota_fiscal` | spine | 4,306,988 |
| credorEmpenho | `empenho_credor` | empenho | 3,226,707 |
| altOrcamentaria | `alteracao_orcamentaria` | despesa_dotacao | 1,413,463 |
| itemNfs | `nota_fiscal_item` | nota_fiscal | 694,683 |
| rsp | `restos_pagar` | spine | 314,543 |
| movimentacaoRsp | `restos_pagar_movimentacao` | restos_pagar | 67,536 |
| movFonteRsp | `restos_pagar_movimentacao_fonte` | ..._movimentacao | 67,536 |
| decretos | `decreto` | spine | 50,305 |
| leisDecretos | `lei_decreto` | decreto | 41,285 |
| credorRsp | `restos_pagar_credor` | restos_pagar | 21,708 |
| credorMovResp | `restos_pagar_movimentacao_credor` | ..._movimentacao | 0-1,356 |

`restos_pagar` is new as a *table*: the `rsp` stream is already read, but only to
enrich liquidacao/pagamento — its own rows were never published.

## Keys

Every stream is keyed on `seq_*` sequence ids, which TCE-MG **reassigns between
extractions**. The spine keys below are therefore built from stable source fields,
and were validated to equal grain against the incumbent seq id over 150
municipalities x 2016/2021/2024.

That is not true of every table. **18 of the 43 new tables carry a `seq_*` in their
own key** because no combination of stable columns identifies their rows: the
colliding rows differ only in a measure, or are identical apart from the portal's
sequence. Those 18 keys churn between extractions by construction -- a deliberate
choice of "identifies a row" over "survives a re-extraction", recorded in each
model at the key. They are `alteracao_orcamentaria`, `contrato`,
`contrato_credito`, `contrato_item`, `contrato_termo_aditivo_item`,
`dispensa_credenciado`, `dispensa_fornecedor`, `dispensa_item`, `empenho_credor`,
`licitacao_comissao`, `licitacao_homologacao`, `licitacao_julgamento`,
`licitacao_parecer`, `licitacao_quadro_societario`, `liquidacao_nota_fiscal`,
`pagamento_movimento`, `registro_preco_adesao_cotacao` and
`registro_preco_adesao_vencedor`.

| spine table | stable key | result |
|---|---|---|
| `licitacao` | orgao + numero_processo + ano_processo + data_abertura + **unidade\*** | PARITY 28,578/28,578 |
| `dispensa` | + tipo_processo | PARITY 16,776/16,776 |
| `registro_preco_adesao` | same shape as licitacao | PARITY 3,931/3,931 |
| `contrato` | orgao + unidade + **subunidade** + numero_contrato + ano_contrato + `seq_contrato` + `seq_dispensa` | PARITY 60,488/60,488 on the stable part; the two seq columns are the tie-breakers described above |
| `decreto` | orgao + numero_decreto + data_assinatura + **tipo_decreto** | PARITY 38,828/38,828 |
| `restos_pagar` | orgao + numero_empenho_origem + ano + data + dotacao | PARITY 403,907/403,907 |
| `nota_fiscal` | orgao + doc_emitente + numero_nf + serie + data_emissao + **chave_nfe** | 1 collision in 2,048,306 |
| `despesa_dotacao` | orgao + unidade + subunidade + mes + cod_orcamentario + funcao + subfuncao + programa + acao + subacao + natureza + fonte | **no seq id exists**; 0 of 3,269,165 rows collapsed |

Child tables inherit the parent's key and append their own natural number
(`numero_item`, `numero_termo_aditivo`, `numero_documento`) -- and, in the tables
listed above, a `seq_*` as well. They are stable exactly when the parent is AND
they add no sequence of their own.

A child does not re-derive the parent's key: it reads `id_<parent>_bd` from the
parent model through `ref()`, scoped by municipality and exercise. Re-deriving it
inline is what let the two drift apart once already, and a drifted foreign key
matches no parent row at all, which no test on either table alone can see.

### \* The unidade crosswalk

`licitacao`, `dispensa` and `regadesao` carry only `seq_unidade`, never
`cod_unidade` — and `seq_unidade` is precisely what distinguishes their colliding
rows (same process number, different unit of one organ). `empenho`, `contratos`
and `despesa` all carry both, so the code is recoverable by crosswalk, the same
technique already used for `cod_orgao`.

Validated over 120 municipalities: **9,319 seq->cod entries, 0 conflicts**;
coverage **98.3%** of licitacao rows. The uncovered 1.7% are units that never
appear in any spend stream; those rows fall back to the sequence and are NOT
refresh-stable. This is the one place the design knowingly falls short.

### Two known imperfections

1. `nota_fiscal`: 1 collision per ~2M rows — the same NF-e access key recorded
   twice with different values. Arguably one invoice booked twice at source.
2. The 1.7% unidade gap above.

Both are documented rather than hidden. Neither is fixable from the data at hand.

## Coverage

All 43 new tables are **MG-only**. Every one gets a `Coverage` recording
`sigla_uf = MG`, 2014-2026, so the site states the restriction rather than
implying national scope. The 3 extended tables keep their existing multi-state
coverage and gain MG 2022-2026.

## Column naming

Source prefixes map to Data Basis conventions: `seq_` -> `id_`, `num_` ->
`numero_`, `dsc_` -> `descricao_`/bare term, `dat_` -> `data_`, `vlr_` ->
`valor_`, `nom_` -> `nome_`, `cod_` -> `codigo_`/bare term. ~600 columns; the
mechanical pass needs a human read before registration, especially the `dsc_ind_*`
booleans and the coded columns that need `covered_by_dictionary = yes`.

## Not in scope

Other states. Nothing here changes CE, PR, RS, SP, RJ, PB, PE, SC or DF.
