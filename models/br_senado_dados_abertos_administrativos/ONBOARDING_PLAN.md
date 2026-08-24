# br_senado_dados_abertos_administrativos — API study and proposed architecture

**Dataset id:** `br_senado_dados_abertos_administrativos`
**Title:** Dados Abertos Administrativos do Senado Federal
**Source:** `https://adm.senado.gov.br/adm-dadosabertos` (OpenAPI 3.1, no auth, no key)
**Sibling:** `br_senado_dados_abertos` (legislative API, `legis.senado.leg.br/dadosabertos`)
**Status:** DESIGN — nothing built. Written 2026-08-24 from a live probe of every endpoint.

**Decisions taken (2026-08-24):** separate dataset · keep the `quantitativos`/`quadro`
summary data, consolidated into one `quadro_pessoal` · keep `servidor_ativo` ·
contratação sub-resources refresh weekly, parents daily. **37 tables.**

---

## 1. What the API contains

98 documented paths. 49 of them are `/csv` twins of a JSON endpoint, so there are
**49 logical endpoints** across five tags: Senadores, Servidores, Colaboradores,
Contratações, Supridos, Gestão.

Everything is public, unauthenticated, and returns either a bare JSON array or a
`{statusCode, msg, data[]}` wrapper. `404` means "no rows", not an error.
Measured throughput: **~1.8 s/request; ~6 req/s at 10 threads.** Sustained bursts
above ~10 concurrent connections start failing — budget conservative concurrency.

### Temporal coverage (probed at the boundaries)

| Endpoint family | Granularity | First period | Notes |
|---|---|---|---|
| `/senadores/despesas_ceaps/{ano}` | year | **2008** | 2007 → 404 |
| `/servidores/remuneracoes/{ano}/{mes}` | year-month | **2013-01** | 2012 → 404 |
| `/servidores/horas-extras/{ano}/{mes}` | year-month | **2013-01** | 2012 → 404 |
| `/supridos/{ano}` | year | **2013** | 2012 → 404 |
| everything else | **snapshot, no time dimension** | — | current state only |

That split drives the whole design: four true time series partitioned by `ano`,
and the rest as stacked snapshots (`data_extracao` partition, as in
`br_cgu_sancoes` / `au_ato_abr`).

---

## 2. Redundancy found — endpoints excluded

Each verified against the live API, not inferred from the spec.

| Excluded | Why (verified) |
|---|---|
| all 49 `/csv` paths | same payload, different serialization |
| `/servidores/servidores/{ativos,inativos,efetivos,comissionados}` | strict subsets of `/servidores/servidores` (0 rows outside it). They are exactly `situacaoEquals=` / `tipoVinculoEquals=` filters — the parent carries both columns |
| `/servidores/pensionistas/remuneracoes/{ano}/{mes}` | **byte-identical payload** to `/servidores/remuneracoes/{ano}/{mes}` (same md5; 20 `RemuneracaoDto` fields, not the 14 of `RemuneracaoPensionistaDto`). Source bug — pensioner payroll is not actually exposed anywhere |
| `/supridos/{atosConcessao,empenhos,transacoes,movimentacoes}/{ano}` | flat extracts of what `/supridos/{ano}` already nests. Set-equality confirmed for empenhos (464) and transações (1,362) in 2024. One call per year replaces five |
| `/contratacoes/licitacoes/{id}/detalhamentos` (+ `/{id_detalhamento}`) | `detalhamentos[]` already nested in the `/licitacoes` list (7,479 rows, free). Avoids 2,743 N+1 calls |
| `/contratacoes/{tipo}/{id}/pagamentos/{id_pagamento}/documentos_fiscais` | **returns 0 rows even when the nested list has 5.** Endpoint is broken; take the documents from the nested field instead (see trap 4) |
| `/gestao/*/agrupado`, `/senadores/{cod}/recursos-utilizados` | aggregations / joins over endpoints already ingested |
| `/servidores/aposentados-{efetivos,comissionados}/{matricula}` | single-record lookups of the list endpoints |
| `empresas[].{contratos,notas_empenho,atas_registro_preco}` | all three keys return the **same** combined list, and it is *lossy* (drops `numero_formatado`, `unidade_gestora`). Use `/empresas` for the company dimension only |

---

## 3. Traps that will bite if not handled

1. **`/contratacoes/contratos` silently hides 70% of contracts.** The bare call
   returns 2,477 rows. Fanning out over the status enum gives
   `ENCERRADO=5,685` + `VIGENTE=2,467` + `EM_RENOVACAO=10` = **8,162 unique**.
   The pipeline must fan out and union. `notas_empenho` (3,924 = VIGENTE ∪ ENCERRADO)
   and `atas_registro_preco` (656 = 100 + 556) are complete on the bare call — verified.
2. **`id` is unique only within `tipoContratacao`.** 577 ids appear in both
   `contratos` and `notas_empenho` as different entities. The key is
   `(tipo_contratacao, id)`, matching the sub-resource route
   `/contratacoes/{tipoContratacao}/{id}/...`.
3. **`pagamentos` lives entirely in the hidden ENCERRADO space.** 0 hits across
   570 parents sampled from the bare lists; 15/120 hits once sampling ENCERRADO
   ids, at 2.5 pagamentos per contract. Trap 1 was masking the whole branch.
4. **Nested `documentos_fiscais` is the contract's document list, repeated
   identically on every pagamento** — not that payment's documents. Contract 2280
   returns the same 5 doc ids on all 4 of its pagamentos. Model it at
   `(tipo_contratacao, id)` grain and deduplicate, or row counts inflate ~20×.
   Same bug family as the `empresas[]` keys. `pagamentos/{id}/empenhos`, by
   contrast, *is* genuine — distinct empenhos per payment.
5. **`/contratacoes/empresas?pagina=` disagrees with the bare call.** Bare returns
   2,906 rows / 2,903 unique ids (3 duplicate ids); page 20 held an id absent from
   the bare response. Confirm during build whether pagination is required.

---

## 4. Proposed tables — 37

Naming mirrors `br_camara_dados_abertos` where the concept matches, so the two
houses stay comparable (`despesa`, `licitacao`, `servidor`≈`funcionario`).

### A. Senadores — administrative (5)

| table | source | partition | rows |
|---|---|---|---|
| `despesa_ceaps` | `/senadores/despesas_ceaps/{ano}` | `ano` | ~370k (2008–2026, ~21k/yr) |
| `senador_gabinete` | `/senadores` | `data_extracao` | 81 |
| `senador_escritorio_apoio` | `/senadores/escritorios-apoio` | `data_extracao` | 83 |
| `senador_auxilio_moradia` | `/senadores/auxilio-moradia-imoveis-funcionais` | `data_extracao` | 86 |
| `senador_aposentado_pensionista` | `/senadores/aposentados` + `-ipc` + `-pssc` + `/pensionistas-ipc`, stacked with `regime` + `tipo_beneficio` | `data_extracao` | 188 |

### B. Servidores (9)

| table | source | partition | rows |
|---|---|---|---|
| `servidor` | `/servidores/servidores` — full quadro, `vinculo` × `situacao` | `data_extracao` | 22,933 |
| `servidor_ativo` | `/servidores` — active only, *different columns* (`dataAdmissao`, `jornadaSemanal`, `afastamento`, `isencaoPonto`, `hierarquiaCompleta[]`) | `data_extracao` | 6,660 |
| `servidor_remuneracao` | `/servidores/remuneracoes/{ano}/{mes}` | `ano` | **~2.4M** (163 months × ~15k) |
| `servidor_hora_extra` | `/servidores/horas-extras/{ano}/{mes}`, exploded to day level | `ano` | ~490k |
| `servidor_aposentado` | `aposentados-efetivos` + `aposentados-comissionados`, stacked | `data_extracao` | 3,630 |
| `servidor_exonerado` | `/servidores/exonerados` | `data_extracao` | 4,740 |
| `servidor_cedido` | `cedidos/{para-senado,pelo-senado,infraero-para-senado}` + `exercicio-provisorio`, stacked with `tipo_cessao` | `data_extracao` | 248 |
| `pensionista` | `/servidores/pensionistas` | `data_extracao` | 2,064 |
| `dicionario` | cargos (3,707) + lotações (161) + enums | — | ~4k |

### C. Contratações (12)

| table | source | partition | rows |
|---|---|---|---|
| `contratacao` | union of `contratos` (all 3 statuses) + `notas_empenho` + `atas_registro_preco`; key `(tipo_contratacao, id)` | `data_extracao` | ~12,700 |
| `contratacao_orgao_gestor` | nested `orgaos_gestores[]` | `data_extracao` | ~15k |
| `contratacao_item` | N+1 `/{tipo}/{id}/itens` | `data_extracao` | ~33k |
| `contratacao_garantia` | N+1 `/{tipo}/{id}/garantias` | `data_extracao` | ~1.5k |
| `contratacao_pagamento` | N+1 `/{tipo}/{id}/pagamentos` | `data_extracao` | ~14.4k |
| `contratacao_documento_fiscal` | nested in the pagamentos response, **deduped to contract grain** (trap 4) | `data_extracao` | ~35k |
| `contratacao_pagamento_empenho` | N+1 `/{tipo}/{id}/pagamentos/{id}/empenhos` | `data_extracao` | ~14.4k |
| `contrato_aditivo` | N+1 `/contratos/{id}/aditivos` | `data_extracao` | ~1.5k |
| `ata_acionamento` | N+1 `/atas_registro_preco/{id}/acionamentos` | `data_extracao` | ~1.6k |
| `licitacao` | `/licitacoes` | `data_extracao` | 2,743 |
| `licitacao_detalhamento` | nested `detalhamentos[]` | `data_extracao` | 7,479 |
| `empresa` | `/contratacoes/empresas` (id, nome, cpf_cnpj only) | `data_extracao` | 2,906 |

### D. Colaboradores (3)

`terceirizado` (3,158) · `menor_aprendiz` (78) · `estagiario` (463) — all `data_extracao`.

### E. Supridos — suprimento de fundos (5, one API call per year)

`suprido_ato_concessao` (~1.5k) · `suprido_empenho` (~4k) · `suprido_transacao` (~12k)
· `suprido_transacao_objeto` (~13k) · `suprido_movimentacao` (~5k). All `ano`, 2013+.

### F. Gestão e quadros de pessoal (3)

Consolidated per the 2026-08-24 decision. The six *establishment-count* endpoints
collapse into one `quadro_pessoal`; the two that are not count tables stay as they
are, because folding them in would destroy them.

| table | source | rows | why |
|---|---|---|---|
| `quadro_pessoal` | `gestao/quadro-cargos-efetivos` (43) + `gestao/quadro-funcoes-comissionadas` (7) + `servidores/quadro-servidores-estaveis-e-nao-estaveis` (5) + `servidores/quantitativos/pessoal` (52) + `servidores/quantitativos/cargos-funcoes` (20) + `senadores/quantitativos/senadores` (13) | ~195 | all six are headcounts of posts and people over different dimension sets |
| `diretor_coordenador` | `/gestao/diretores-e-coordenadores` | 233 | a *directory* (setor × titular × substituto, with matrícula and e-mail), not counts |
| `previsao_aposentadoria` | `/servidores/previsao-aposentadoria` | 1,855 | forward-looking projection at cargo × ano/mês-de-direito grain, not current establishment |

**`quadro_pessoal` shape.** Semi-long: one row per source quadro × dimension
combination × period, with each measure in its own typed column so every numeric
carries a single unambiguous unit.

- `data_extracao` (DATE, partition) · `quadro` (dict: `cargo_efetivo`,
  `funcao_comissionada`, `servidor_estavel`, `pessoal`, `cargo_funcao`, `senador`)
  · `periodo` (dict: `ATUAL` / `ANTERIOR`, folding the `…Ant` / `…Hoje` pairs)
  · `data_referencia` (DATE, only `quantitativos/senadores`)
- dimensions, all nullable: `categoria`, `classe`, `grupo`, `cargo`, `nivel`,
  `especialidade`, `referencia`, `tabela_vencimento`, `plano_carreira`,
  `nivel_escolaridade`, `padrao_nivel_referencia`
- measures (INT64): `quantidade_cargos`, `quantidade_ocupados`,
  `quantidade_vagos`, `quantidade_estaveis`, `quantidade_nao_estaveis`,
  `quantidade_aposentados`, `quantidade_instituidores_pensao`,
  `quantidade_beneficiarios_pensao`, `quantidade_total_ativo`,
  `quantidade_total_inativo`, `quantidade_com_opcao`, `quantidade_sem_opcao`,
  `quantidade_sem_vinculo`, `quantidade_subtotal`, `quantidade_total`

**The four `variacao*` fields are dropped** — verified derivable from the
`ANTERIOR`/`ATUAL` pair they summarise (`quadro_servidor_estavel`: 717 → 825 =
+15.1%; 528 → 475 = −10.0%; both match the published value). Dropping them also
keeps every measure an integer headcount rather than mixing counts with percentages.

---

## 5. Refresh design

Two flows, mirroring `br_senado_dados_abertos` (no source-poll gate — administrative
data changes continuously).

**Daily** — parents and snapshots:
- snapshot tables: full rebuild, one new `data_extracao` partition per run
- time series (`despesa_ceaps`, `servidor_remuneracao`, `servidor_hora_extra`,
  supridos): re-extract current year + 1 prior (`REFRESH_PRIOR_YEARS=1`),
  `dump_mode="append"` so older partitions survive
- `contratacao` itself, via the status fan-out

**Weekly** — contratação sub-resources (decision, 2026-08-24):
`itens`, `garantias`, `pagamentos`, `pagamentos/{id}/empenhos`, `aditivos`,
`acionamentos`. About **12,700 parent calls + ~14,400 pagamento-child calls
≈ 27k requests ≈ 1.3 h at 6 req/s**. Sized for a weekend slot; pick an unused
cron minute per the repo convention.

---

## 6. Remaining open items

1. ~~`servidor_ativo` identifier~~ — **RESOLVED 2026-08-24: keep the table.** It
   has no stable id (only `nome`, 6,658 unique over 6,660 rows) and can only be
   name-joined to `servidor`. Documented as a known limitation in the table
   description rather than dropping `dataAdmissao` / `jornadaSemanal` /
   `hierarquiaCompleta`.
2. **`empresas` pagination** (trap 5) — settle before the architecture is frozen.
3. **BD Pro tiering.** Mirroring `br_senado_dados_abertos`: time series
   (`despesa_ceaps`, `servidor_remuneracao`, `servidor_hora_extra`, supridos) →
   `PartBdpro`; snapshot dimensions → `AllFree`. Needs a pro Coverage created on
   each `PartBdpro` table before the first armed run.
4. ~~Consolidation option for group F~~ — **RESOLVED 2026-08-24: consolidated**
   into `quadro_pessoal` (six count endpoints), with `diretor_coordenador` and
   `previsao_aposentadoria` left standalone. See group F above.
