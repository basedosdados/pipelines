# br_me_siconfi

## Dataset Overview

SICONFI (Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro) is published by the Brazilian Ministry of Finance (ME). It contains annual budget and balance sheet data for all government entities: municipalities, states (UF), and the federal government (Brasil).

Coverage: 2014–present (API; 1998–2012 from legacy bulk files, handled separately).

Tables produced:
- `municipio_receitas_orcamentarias` — budgetary revenues by municipality
- `municipio_despesas_orcamentarias` — budgetary expenditures by municipality
- `municipio_despesas_funcao` — expenditures by function/subfunction by municipality
- `municipio_balanco_patrimonial` — balance sheet by municipality
- `uf_receitas_orcamentarias` / `uf_despesas_orcamentarias` / `uf_despesas_funcao` — same for states
- `brasil_receitas_orcamentarias` / `brasil_despesas_orcamentarias` / `brasil_despesas_funcao` — federal level

---

## Data Sources

### API (2014–present)
- Endpoint: SICONFI DCA (Demonstrativo de Contas Anuais)
- Download script: `code/download_api.py` / `code/download_raw_data_api.py`
- Raw files: `code/input/api/dca_{ano}_{cod_ibge}.json`
  - `cod_ibge` length determines entity level: 1 digit = Brasil, 2 = UF, 7 = município
- Annexos used:
  - `DCA-Anexo I-C` → receitas_orcamentarias
  - `DCA-Anexo I-D` → despesas_orcamentarias
  - `DCA-Anexo I-E` → despesas_funcao
  - `DCA-Anexo I-AB` → balanco_patrimonial

### Legacy bulk files (1998–2012)
- Downloaded separately; handled by `code/tables/` (not `code/tables_api/`)

---

## Build Pipeline (API tables)

Entry point: `code/tables_api/build.py`

```bash
~/.pyenv/versions/3.11.6/bin/python code/tables_api/build.py --path_dados /path/to/dados_SICONFI
# optional filters:
#   --table municipio_receitas_orcamentarias
#   --ano 2022
```

**Output location**: `{path_dados}/output_API/{table}/ano={ano}/sigla_uf={uf}/{table}.csv`
(Brasil tables: `{path_dados}/output_API/{table}/ano={ano}/{table}.csv`)

### How the build works

1. `build.py` iterates years in the outer loop, calls `load_year_data(ano, api_dir)` once per year
2. `load_year_data` reads all `dca_{ano}_*.json` files in a single pass, partitions into `{level: {annexo: DataFrame}}`
3. Each table builder (`tables_api/{table}.py`) receives the pre-loaded `year_data` dict
4. Builders call `apply_conta_split(df)` to split the raw `conta` field into `portaria` + `conta`
5. Builders merge with the relevant compatibilizacao file to get BD canonical columns
6. Unmatched rows (keys absent from compatibilizacao) are collected and written to `code/compatibilizacao/missing_{comp_file}.xlsx`; build exits with error until all rows are mapped

---

## Compatibilizacao Files

Location: `code/compatibilizacao/`

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

### Updating compatibilizacao files

When the build fails with "unmatched rows detected", the workflow is:

1. Open the `missing_{comp_file}.xlsx` files in `code/compatibilizacao/`
2. Paste the missing key rows into the corresponding `{comp_file}.xlsx`
3. Re-run the build — it should now print "All done."
4. Fill in the `*_bd` columns (`estagio_bd`, `id_conta_bd`, `conta_bd`) for the newly added rows
5. Re-run once more to confirm

A row is only flagged as missing if the key does not exist in the comp file at all. Rows that exist but have empty `*_bd` columns are **not** flagged — fill those at your own pace.

---

## `apply_conta_split` — Key Logic

Raw API `conta` strings combine a portaria code and account name, e.g.:
- `"1.9.4.4.03.0.0 - Multas e Juros de Mora"` (standard)
- `"01 - Legislativa"` (bare integer, despesas_funcao)
- `"1.9.4.4.03.0.0- Name"` (no space before dash)
- `"1.9.4.4.05.0.0    Name"` (spaces only, no dash)

Split logic: `str.extract(r"^(\d+(?:\.\d+)*)\s*-?\s*(.*)")` — extracts portaria by matching the numeric code layout pattern at the start of the string. Non-numeric strings (e.g. "Despesas Correntes") get `portaria=""`.

Portaria formats by table/year:
- receitas/despesas 2013–2017: 15-char codes (`3.0.00.00.00.00`)
- receitas/despesas 2018+: 12-char codes (`3.0.00.00.00`)
- despesas_funcao: short codes (`01`, `01.031`)
- balanco_patrimonial: split not used; `portaria` = full `conta` field, merged by `portaria` key only

---

## Adding a New Year

1. Download API data: run `code/download_api.py` for the new year
2. Run `build.py --ano {year}` to test just that year
3. If "unmatched rows" error: add missing keys to compatibilizacao files, re-run
4. Fill `*_bd` columns for new rows
5. Run full build to confirm

---

## Python Environment

Use `~/.pyenv/versions/3.11.6/bin/python` (has `basedosdados` and all dependencies installed).
