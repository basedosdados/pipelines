# br_bd_execucao_estadual — Execução Orçamentária e Compras dos Governos Estaduais

Transaction-level budget execution and procurement for **Brazilian state governments**.
The state-level sibling of MiDES (`world_wb_mides`), which covers *municipalities* via the
state Courts of Accounts. This dataset covers the **state executives' own spending**, taken
from each state's own transparency portal over its financial system (SIAFI-MG, FIPLAN-BA,
e-Fisco-PE, SIAFEM-SP).

**Status:** MG, BA and PE built, validated and registered on staging (9 tables,
105.9M rows). SP is still being scraped and adds `despesa_anual`.

---

## 1. Why this is not just "MiDES with a different filter"

MiDES scrapes state Courts of Accounts (TCEs), which audit *municipalities*. Nothing in it
describes what a state government itself spends. The sources here are a different universe:
each state publishes its own executive's ledger, on its own portal, in its own shape. The
harmonization problem is the same; the inputs are not.

## 2. Source inventory

| UF | Source | System | Coverage | Update | Access |
|----|--------|--------|----------|--------|--------|
| MG | `dados.mg.gov.br` CKAN (`despesa`, `compras_contratos`, `portal_*`) | SIAFI/MG, SIAD | execução **2002–2026**, compras **2010+** | daily D+1 (dimensional), weekly (portal) | bulk CSV.gz, CC-BY-4.0 |
| BA | `dados.ba.gov.br` CKAN (`despesas`, `licitacoes`, `contratos`, `notas-fiscais`) | FIPLAN, SIMPAS/SAEB | despesa **2013+**, licitação **2004+** | daily D-1 | bulk ZIP |
| PE | `dados.pe.gov.br` CKAN (`todas-despesas-detalhadas`, `all-pagamentos`) | e-Fisco | **2008–2026** | annual snapshots + current year | bulk CSV, cc-by |
| SP | SIGEO Lei 131 (`fazenda.sp.gov.br/SigeoLei131`) | SIAFEM/SP | **2010–2026** | daily | WebForms scrape → CSV export |

A browser User-Agent is required on `dados.mg.gov.br` (bare curl gets 403).

## 3. The grain problem, and how this dataset answers it

The four sources do **not** share a grain:

| UF | Execution grain | Creditor **on the value row** | Empenho id | Date | Tender link |
|----|----------------|------------------------------|------------|------|-------------|
| MG | empenho × budget line × document | yes, CNPJ/CPF (0.5% anonymised) | yes | monthly | **native** (`fl_compras_empenho`, 1.1M links) |
| BA | month × budget line | **no** | **no** | monthly | **native** (item → `NUM_INSTRUMENTO_ORCAMENTO`) |
| PE | empenho document | yes (some pseudo-codes) | yes | **year only** (2011+); dates exist 2008-2010 | none (modality only) |
| SP | credor × budget line × **year** | yes, CPF/CNPJ | **no** | **year only** | none (modality only) |

**Only MG is transaction grain.** Two states fail it for different reasons, and both were
verified rather than assumed:

*SP* has no document id and no sub-annual date at all. Forcing it into `despesa` would leave
every SP row null in exactly the columns that make the table useful, while looking like
transaction data to anyone filtering `sigla_uf = 'SP'`.

*BA* looks like it qualifies and does not. It publishes empenho numbers and CNPJs — but in a
**different view from the money**. `VW_PAINEL_DESPESA` carries the values at month ×
budget-line grain with no creditor and no empenho number; `VW_PROCESSO_SEI` carries the
empenho number and CNPJ with no values, and only from 2019. The sole key between them is the
dotação, and **1,091,372 empenhos map to 185,969 dotação keys** — about six empenhos per
dotação against one value row per dotação-month. Attributing money to a creditor across that
key needs an allocation rule the source does not publish, so it would be invention.

**Decision:** one table per grain, and no table that lies about what it holds.

| Table | Grain | State |
|---|---|---|
| `despesa` | empenho document × budget line | MG 2002+, PE 2008+ |
| `despesa_mensal` | month × budget line, no creditor | BA, 2013+ |
| `despesa_anual` | credor × budget line × year | SP 2010+, pending |
| `empenho_credor` | empenho × creditor, **no values** | BA, 2019+ |

### Divergence from MiDES worth knowing

MiDES splits execution into three tables (`empenho`, `liquidacao`, `pagamento`) because its
TCE sources publish three separate ledgers. **The state expense exports here instead publish
the phases as columns on one row** (`vr_empenhado` / `vr_liquidado` / `vr_pago`). So `despesa`
keeps the three phases as columns. Splitting it three ways would triple the rows and
fabricate documents and dates the sources do not contain.

`despesa` is therefore closest to MiDES's **`empenho`**, not its `pagamento`: 68.1% of MG
rows and 21.8% of PE's carry no payment at all, so naming it `pagamento` would mislabel some
55 million rows that were committed and never paid.

**`pagamento` exists separately, and only for PE**, because PE is the one source that
publishes a payment *document*: `all-pagamentos` gives the ordem bancária with its own
number, date, value, payee and a link to the empenho. That is a real ledger, not a projection
of a column, so it is a real table. It is also the only sub-annual timing available for PE
from 2011 on, since the expense export drops the date that year.

**`liquidacao` / `verificacao` cannot be built from any source here.** No state publishes a
verification document. MG's `fl_despesa_pgto` is 29.9M rows of two columns — a payment
sequence and a status, with no value, date or empenho — and a CKAN search returns no
liquidação dataset on MG or PE. Only the `valor_liquidado` column exists, so such a table
would be a filter over `despesa` that invents a document the state never issued.

## 4. Tables

Nine tables are built. `despesa_anual` is pending the SP scrape;
`orgao_unidade_gestora` was dropped — no source publishes an organisational directory
separable from its fact tables, so the órgão fields stay denormalised on each row.

| Table | Grain | States | Rows |
|-------|-------|--------|------|
| `despesa` | empenho document × budget line, with `valor_empenhado`/`liquidado`/`pago` | MG, PE | 85,214,849 |
| `pagamento` | payment document (ordem bancária) × empenho line | PE | 10,710,893 |
| `despesa_mensal` | month × budget line, values without creditor | BA | 2,219,353 |
| `empenho_credor` | empenho × creditor, creditor without values | BA | 1,091,372 |
| `licitacao` | one tender / procurement process | MG, BA | 519,598 |
| `licitacao_item` | purchase of one item within a tender | MG, BA | 3,079,475 |
| `licitacao_participante` | bidder × item, with outcome | BA | 1,828,922 |
| `relacionamentos` | tender ↔ empenho bridge | MG, BA | 1,292,948 |
| `dicionario` | value → label for every coded column | MG | 11,962 |
| `despesa_anual` *(pending)* | credor × budget line × year | SP | — |

105.9M rows. Partitioned by `ano` (INT64), clustered by `sigla_uf`;
`dicionario` and `relacionamentos` carry no date column and are unpartitioned.

**Bahia is deliberately split across two tables rather than folded into `despesa`.** The
source publishes the values in one view (by month and appropriation, no creditor) and the
creditors in another (by empenho, no values), and the only key between them is the
appropriation, at about six empenhos per appropriation. Attributing value to creditor
through that key would need an apportionment rule the source does not provide.

## 5. Relationship to `br_pncp`

PNCP covers procurement for all government levels from **2021** (Lei 14.133). The marginal
value here is (1) **budget execution**, which PNCP does not carry at all, and (2) pre-2021
procurement history — back to 2010 (MG) and 2004 (BA). State procurement from 2021 is
retained deliberately: the state portals carry item- and bidder-level detail that PNCP does
not, so the overlap is not pure duplication.

## 6. Build order

1. **MG** — richest source, native tender→empenho bridge; fixes the harmonized schema.
2. **BA** — second full test, and the only source with bidder-level participation.
3. **PE** — long history, execution only.
4. **SP** — scraper + `despesa_anual`.
5. RS / CE / RJ / DF — blocked from a foreign IP, need a Brazilian VPN.

## 7. Known constraints

- `dados.mg.gov.br` returns **403** without a browser User-Agent.
- SP's SIGEO requires an exact postback order: year → **phase** → órgão → search → export.
  The Credor / Licitação / Item / Município fields do not exist in the DOM until an execution
  phase is ticked. Export is a `btnExcel` form POST, not a GET.
- MG anonymises 6,653 of 1.46M creditors (0.5%, all CPF) as
  `INFORMACAO COM RESTRICAO DE ACESSO`; CNPJs are always named.
- RS, CE, RJ and DF portals time out or 403 from outside Brazil.

## 8. MG validation, as built (2026-08-28, dev)

`dbt run --select br_bd_execucao_estadual` → PASS=4, ERROR=0. `despesa` builds 80,258,756
rows in 50s. `code/validate_mg.py` reconciles it against the raw staging fact table:

| check | result |
|---|---|
| row count | 80,258,756 = 80,258,756 exact |
| distinct empenhos | 14,778,423 = 14,778,423 exact |
| sum empenhado / liquidado / pago | agree to a relative 1.2e-14 |
| dimension key uniqueness | all 15 joined keys unique |
| coverage: credor, documento, funcao, subfuncao, programa, acao, elemento, fonte, orgao | 100% |
| coverage: data, numero_empenho, unidade_gestora | 99.23% |
| coverage: id_licitacao_bd | 11.72% |

Two numbers deserve their explanation rather than a footnote:

**99.23%, not 100%, on the empenho-derived columns.** 616,833 fact rows (0.7686%) carry an
`id_empenho` that is absent from MG's own `dm_empenho` dimension. The gap is spread evenly
across all 25 exercises, which rules out a missing or truncated file — it is a referential
gap in the published source. Those rows are kept, with `data`, `numero_empenho`,
`tipo_empenho`, `descricao` and `id_unidade_gestora` null; their values are intact, which is
why the money totals still reconcile.

**11.72% on the tender link is expected, not a defect.** Only spending that went through a
purchase process has a tender at all — payroll, debt service and transfers never do. The
figure is reported and not gated for that reason.

The money totals are compared on relative difference rather than exactly. Summing 80M
float64 values in a different order changes the last cent, and BigQuery promises no order;
the observed gap is R$0.01 on R$1.8 trillion.
