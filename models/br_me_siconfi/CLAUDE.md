# br_me_siconfi

## Dataset Overview

SICONFI (Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro) is published by the Brazilian Ministry of Finance (ME). It contains annual budget and balance sheet data for all government entities: municipalities, states (UF), and the federal government (Brasil).

Coverage: 2013–present (API); 1989–2012 from legacy bulk files.

Tables produced (19 total):

**Município**
- `municipio_receitas_orcamentarias` — budgetary revenues
- `municipio_despesas_orcamentarias` — budgetary expenditures
- `municipio_despesas_funcao` — expenditures by function/subfunction
- `municipio_balanco_patrimonial` — balance sheet
- `municipio_execucao_restos_pagar` — execution of carryover obligations
- `municipio_execucao_restos_pagar_funcao` — same, by function
- `municipio_variacoes_patrimoniais` — patrimonial variations

**UF (state)**
- `uf_receitas_orcamentarias`
- `uf_despesas_orcamentarias`
- `uf_despesas_funcao`
- `uf_execucao_restos_pagar`
- `uf_execucao_restos_pagar_funcao`
- `uf_variacoes_patrimoniais`

**Brasil (federal)**
- `brasil_receitas_orcamentarias`
- `brasil_despesas_orcamentarias`
- `brasil_despesas_funcao`
- `brasil_execucao_restos_pagar`
- `brasil_execucao_restos_pagar_funcao`
- `brasil_variacoes_patrimoniais`

---

## Data Sources

### API (2013–present)
- Endpoint: SICONFI DCA (Demonstrativo de Contas Anuais)
- Download script: `code/download_api.py`
- Raw files: `code/input/api/dca_{ano}_{cod_ibge}.json`
  - `cod_ibge` length determines entity level: 1 digit = Brasil, 2 = UF, 7 = município
- Annexos used:
  - `DCA-Anexo I-C` → receitas_orcamentarias
  - `DCA-Anexo I-D` → despesas_orcamentarias
  - `DCA-Anexo I-E` → despesas_funcao
  - `DCA-Anexo I-AB` → balanco_patrimonial
  - `DCA-Anexo I-F` → execucao_restos_pagar
  - `DCA-Anexo I-G` → execucao_restos_pagar_funcao
  - `DCA-Anexo I-HI` → variacoes_patrimoniais

### Legacy bulk files (1989–2012)
- Downloaded separately; processed by `code/tables_final/` alongside API data via the unified build.

---

## Build Pipeline

Entry point: `code/build.py`

```bash
~/.pyenv/versions/3.11.6/bin/python code/build.py --path_dados /path/to/dados_SICONFI
# optional filters:
#   --table municipio_receitas_orcamentarias
#   --ano 2022
#   --workers 4
```

**Output location**: `{path_dados}/output/{table}/ano={ano}/sigla_uf={uf}/{table}.csv`
(Brasil tables: `{path_dados}/output/{table}/ano={ano}/{table}.csv`)

### How the build works

1. `build.py` dispatches years to `ProcessPoolExecutor` workers
2. Each worker calls `load_year_data(ano, api_dir)` once — reads all `dca_{ano}_*.json` in a single pass, partitions into `{level: {annexo: DataFrame}}`
3. Each table builder in `code/tables_final/{table}.py` receives the pre-loaded `year_data` dict
4. Builders call `apply_conta_split(df)` to split raw `conta` into `portaria` + `conta`
5. Builders merge with the relevant crosswalk file to get BD canonical columns
6. Unmatched rows are written to `code/crosswalk/missing_{comp_file}.xlsx`; build exits with error until all rows are mapped

---

## Crosswalk Files

Location: `code/crosswalk/`

These Excel files map raw `(ano, estagio, portaria, conta)` keys → BD canonical identifiers:
- `estagio_bd`
- `id_conta_bd`
- `conta_bd`

Files:
- `receitas_orcamentarias.xlsx`
- `despesas_orcamentarias.xlsx`
- `despesas_funcao.xlsx`
- `balanco_patrimonial.xlsx` — merge key is `(ano, portaria)` only, no `estagio`
- `municipio.xlsx` — IBGE code mappings

### Updating crosswalk files

When the build fails with "unmatched rows detected":

1. Open `code/crosswalk/missing_{comp_file}.xlsx`
2. Paste the missing key rows into the corresponding `{comp_file}.xlsx`
3. Re-run the build — it should now print "All done."
4. Fill in the `*_bd` columns (`estagio_bd`, `id_conta_bd`, `conta_bd`) for the newly added rows
5. Re-run once more to confirm

A row is only flagged as missing if the key does not exist in the crosswalk file at all. Rows that exist but have empty `*_bd` columns are **not** flagged — fill those at your own pace.

---

## `apply_conta_split` — Key Logic

Raw API `conta` strings combine a portaria code and account name, e.g.:
- `"1.9.4.4.03.0.0 - Multas e Juros de Mora"` (standard)
- `"01 - Legislativa"` (bare integer, despesas_funcao)
- `"1.9.4.4.03.0.0- Name"` (no space before dash)
- `"1.9.4.4.05.0.0    Name"` (spaces only, no dash)

Split logic: `str.extract(r"^(\d+(?:\.\d+)*)\s*-?\s*(.*)")` — extracts portaria by matching the numeric code layout at the start. Non-numeric strings (e.g. "Despesas Correntes") get `portaria=""`.

Portaria formats by table/year:
- receitas/despesas 2013–2017: 15-char codes (`3.0.00.00.00.00`)
- receitas/despesas 2018+: 12-char codes (`3.0.00.00.00`)
- despesas_funcao: short codes (`01`, `01.031`)
- balanco_patrimonial: split not used; `portaria` = full `conta` field, merged by `portaria` key only

---

## Adding a New Year

1. Download API data: `code/download_api.py --ano {year}`
2. Run `code/build.py --ano {year}` to test just that year
3. If "unmatched rows" error: add missing keys to crosswalk files, re-run
4. Fill `*_bd` columns for new rows
5. Run full build to confirm

---

## Uploading to Data Basis

Script: `code/upload_to_db.py` (not committed — regenerate as needed via `/upload-databasis`).

Key points:
- `basedosdados-dev` is a requester-pays GCS bucket; `gcs.Client.bucket` must be monkey-patched to inject `user_project` globally before any `bd.*` calls
- When uploading with `--if-exists replace`, the script first deletes the entire GCS staging prefix to avoid stale files from prior schema versions causing BQ partition key conflicts
- Publishing happens via dbt, not `table.publish()` (which requires backend metadata registration)
- dbt environment: `/tmp/dbt_env/bin/dbt` (created with `~/.pyenv/versions/3.11.6/bin/python -m venv /tmp/dbt_env && /tmp/dbt_env/bin/pip install dbt-bigquery`)
- dbt run: `cd pipelines && /tmp/dbt_env/bin/dbt run --select {models} --profiles-dir ~/.dbt --target dev`

---

## Python Environment

Use `~/.pyenv/versions/3.11.6/bin/python` (has `basedosdados` and all dependencies installed).
