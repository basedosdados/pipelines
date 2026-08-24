# br_senado_dados_abertos_administrativos

Senado Federal **administrative** open data — CEAPS expenses, staff, payroll,
contracting, fund advances and establishment reports — from
`https://adm.senado.gov.br/adm-dadosabertos`.

Sibling to `br_senado_dados_abertos`, which covers the **legislative** API
(`legis.senado.leg.br/dadosabertos`). They are separate source products with
separate coverage and cadence. Backend slug is `dados_abertos_administrativos`,
mirroring the sibling's `dados_abertos_legislativos` — not the GCP dataset id.

39 tables. The design, the endpoint-by-endpoint study and the exclusions are in
[ONBOARDING_PLAN.md](ONBOARDING_PLAN.md).

## Two partition schemes, because the source has two shapes

| | tables | partition |
|---|---|---|
| genuine time series | `despesa_ceaps` (2008+), `servidor_remuneracao` (2013+), `servidor_hora_extra`(`_dia`) (2013+), the six `suprido_*` (2013+) | `ano`, INT64 |
| everything else | staff, contracting, colaboradores, gestão | `data_extracao`, DATE — stacked snapshots, as in `br_cgu_sancoes` |

The API exposes only current state for the snapshot tables; there is no time
dimension to recover.

## Source defects the transform compensates for

Each was found by probing the live API, and each has an offline regression test
in `pipelines/datasets/br_senado_dados_abertos_administrativos/tests/`. Do not
remove the compensation without re-probing.

1. **`/contratacoes/contratos` hides 70% of contracts.** The bare call returns
   2,477; fanning out over `statusContratoParam` gives 8,162. This also hides
   the entire `pagamentos` branch, which lives in the ENCERRADO space.
2. **`id` is unique only within a `tipoContratacao`** — 577 ids collide between
   `contratos` and `notas_empenho`. The key is the pair, everywhere.
3. **Nested `documentos_fiscais` repeats the contract's whole document list on
   every payment.** Modelled at contract grain and deduplicated, or the row
   count multiplies by the payment count. `pagamento` empenhos *do* differ per
   payment and keep their fan-out.
4. **`/supridos/{ano}` repeats some movimentações verbatim** inside the same
   ato — 3 of 836 in 2018 — which would double-count their value.
5. **`/servidores/pensionistas/remuneracoes` returns the servidores payload**
   byte-for-byte. Pensioner payroll is not actually exposed; the endpoint is not
   read.
6. **`quantitativos/senadores` is a historical series, not a snapshot** — 13
   rows differing only by reference date, so `data_referencia` is in
   `quadro_pessoal`'s key.
7. `404` means "no rows", not an error. Dates arrive ISO *and* dd/mm/aaaa with
   `---` as a null sentinel; money as JSON numbers *and* `16.368,74`; booleans as
   `true`/`false` *and* `S`/`N`.

## Memory: the extraction must stream

`servidor_remuneracao` costs ~2.5 KB/row as Python dicts — about 5.9 GB over
full history, and ~9 GB together with `servidor_hora_extra_dia`. `clean_all`
therefore writes and releases one table at a time, and the time series one year
at a time. Do not "simplify" it back into building a dict of all 39 tables: that
OOMs a 4 GiB worker, and a 16 GB laptop.

## Where the code lives

- `pipelines/datasets/br_senado_dados_abertos_administrativos/` —
  `senado_adm_api.py` (client + the status fan-out), `senado_adm_clean.py` (one
  builder per table), `utils.py` (registry, `clean_all`, parquet writer),
  `tests/`.
- `models/br_senado_dados_abertos_administrativos/code/` —
  `architecture_spec.py` (**the** source of truth: 39 tables, 406 columns,
  trilingual), `validate_spec.py` (house-rule gate), `gen_dbt.py`,
  `run_onboarding.py`, `upload.py`.

The cleaning transform is shared by the onboarding bootstrap and the recurring
pipeline, so both produce identical output.

## Running it

```bash
uv run python models/br_senado_dados_abertos_administrativos/code/validate_spec.py
uv run python models/br_senado_dados_abertos_administrativos/code/run_onboarding.py --sample
uv run python models/br_senado_dados_abertos_administrativos/code/gen_dbt.py
uv run python models/br_senado_dados_abertos_administrativos/code/upload.py --env dev
uv run dbt run --select br_senado_dados_abertos_administrativos
```

Scratch data goes to `~/Downloads/br_senado_dados_abertos_administrativos_data`
(override with `SENADO_ADM_DATA`) — never the repo or Dropbox. Delete it once
the dataset is published.

A full run is slow: ~163 monthly payroll requests plus a ~27k-request
contratação fan-out at roughly 6 req/s. `--sample` covers the current year and
skips the fan-out. The host starts refusing connections above ~10 concurrent
requests, so `MAX_WORKERS` is 8 deliberately.
