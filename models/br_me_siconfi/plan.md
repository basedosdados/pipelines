# Plan: Refactor build.ipynb + Fix municipio_balanco_patrimonial

## Context

- **Input data**: `/Users/rdahis/Mac/Downloads/dados_SICONFI/` (or equivalent `path_dados`)
- **Output**: `{path_dados}/output/{table}/ano={ano}/sigla_uf={uf}/{table}.csv`
- **Compatibilizacao tables**: `code/compatibilizacao/*.xlsx`
- **Existing notebook**: `code/build.ipynb` — processes 7 tables across pre-2013 (Excel) and post-2013 (finbra CSV inside zip) data

---

## Step 1: Refactor build.ipynb → Python scripts

### Structure

Replace the notebook with:

```
code/
  build.py              # entry point: calls all table builders
  tables/
    shared.py           # shared helpers: load_compatibilizacao(), build_ibge_id(), partition_and_save()
    municipio_receitas_orcamentarias.py
    municipio_despesas_orcamentarias.py
    municipio_despesas_funcao.py
    municipio_balanco_patrimonial.py   ← needs fixes (see Step 2)
    uf_receitas_orcamentarias.py
    uf_despesas_orcamentarias.py
    uf_despesas_funcao.py
```

### `build.py` interface

```python
python build.py --path_dados /path/to/dados_SICONFI [--table TABLE] [--ano ANO]
```

- `--table` (optional): run only one table (e.g. `municipio_balanco_patrimonial`)
- `--ano` (optional): run only one year (for testing)

### Shared helpers (`shared.py`)

Extract into reusable functions:

- `load_compatibilizacao(path_queries)` → returns dict of all 5 comp dataframes
- `build_ibge_id_1998_2012(df)` → constructs `id_municipio_6` from `CD_UF`+`CD_MUN`, merges with municipio comp
- `split_conta_portaria(conta_str, ano)` → splits `"1.0.0.0.0.00.00 - Ativo"` into portaria + conta based on year format
- `partition_and_save(df, table_name, ano, ufs, path_dados, id_col)` → saves partitioned CSVs
- `unzip_finbra(zip_path, folder)` → replaces the `!unzip` shell magic with `zipfile` module

### Key refactoring rules

- Replace all `!unzip` and `!rm` shell magic with Python `zipfile` and `os.remove()`
- Replace `df_dados.apply(lambda row: ..., axis=1)` vectorized multiplications with `df["valor"] *= 1000` etc.
- No notebook-only syntax; all code must run with `python build.py`

---

## Step 2: Fix municipio_balanco_patrimonial

### Bugs identified (cell 14 in build.ipynb)

| # | Bug | Fix |
|---|-----|-----|
| 1 | Wrong file range for 1998–2003: uses `5 <= file <= 6`, but file 5 is despesas_funcao | Use `6 <= file <= 7` for **all** of 1998–2012 |
| 2 | 1998–2012 id_municipio logic: uses `municipio_original` merge (1989–1997 pattern) | Use `build_ibge_id_1998_2012()` (CD_UF + CD_MUN → 7-digit IBGE), same as receitas/despesas |
| 3 | Wrong `id_vars` in melt: references `municipio_auxiliar`, `municipio_original`, `CdUF`, `CdMun` | Use `["id_municipio", "sigla_uf", "CD_UF", "CD_MUN"]` then drop `CD_UF`, `CD_MUN` |
| 4 | Wrong compatibilizacao: merges with `df_comp_despesas_funcao` | Merge with `df_comp_balanco_patrimonial` |
| 5 | Wrong merge keys for 1998–2012: uses `["ano", "conta"]` with wrong comp table | Merge on `["ano", "conta"]` with **balanco** comp table |
| 6 | Wrong merge keys for 2013+: uses `["ano", "estagio", "portaria", "conta"]` | Merge on `["ano", "portaria"]` only (portaria = account ID like `1.0.0.0.0.00.00`) |
| 7 | Wrong Conta parsing for 2013+: splits on `"-"` but format is `"1.0.0.0.0.00.00 - Ativo"` | Split on `" - "` (with spaces): portaria = part before, conta = part after |
| 8 | Wrong output schema: uses `ordem["municipio"]` which includes `estagio`/`estagio_bd` | Balanco schema has **no** `estagio`/`estagio_bd` — define separate `ordem_balanco` |
| 9 | Wrong currency adjustments: applies 1990–1993 corrections | Remove — balanco starts in 1998, no currency conversion needed |
| 10 | For 2013+, `Coluna` (= date like "31/12/2013") is mapped to `estagio` but unused in output | Drop `Coluna` entirely |

### Correct output schema for municipio_balanco_patrimonial

Partition columns (excluded from CSV): `ano`, `sigla_uf`

CSV columns (in order):
```
id_municipio, portaria, conta, id_conta_bd, conta_bd, valor
```

(Matches `schema.yml`: no `estagio`, no `estagio_bd`)

### Correct input logic

**1998–2012 (quadro files)**:
```
arquivos = quadro{ano}_6.xlsx (ativo) + quadro{ano}_7.xlsx (passivo)
- Read each as wide format
- Build id_municipio from CD_UF + CD_MUN
- Melt: id_vars = [id_municipio, sigla_uf, CD_UF, CD_MUN], value_vars = account columns
- Drop CD_UF, CD_MUN
- Merge with df_comp_balanco_patrimonial on ["ano", "conta"]
```

**2013–2023 (finbra CSV inside zip)**:
```
zip: input/finbra_MUN_BalancoPatrimonialDCA(AnexoI-AB)/{ano}.zip → finbra.csv
Columns after drop: Cod.IBGE, UF, Conta, Valor  (drop: Instituição, População, Coluna, Identificador da Conta)
Rename: id_municipio, sigla_uf, conta_raw, valor
Split conta_raw on " - ": portaria = left part, conta = right part
Merge with df_comp_balanco_patrimonial on ["ano", "portaria"]
```

---

## Step 3: Create output directory structure

Keep the existing logic from cell 3 as `setup_output_dirs(path_dados, last_year)`.

---

## Step 4: Validation script

Create `code/validate_output.py` to verify new output matches reference files at:
`/Users/rdahis/Mac/Downloads/dados_SICONFI/output/`

Checks per table/year/uf partition:
1. File exists
2. Column names match exactly (same order)
3. Row count within 1% of reference (or exact if deterministic)
4. No columns have all-null values when reference has data

---

## Execution order

1. `shared.py` — write helpers
2. Port each working table (receitas, despesas_orcamentarias, despesas_funcao, uf tables) from notebook with minimal changes
3. Rewrite `municipio_balanco_patrimonial.py` from scratch with fixes above
4. Write `build.py` entry point
5. Test per table with `--table` flag and a single year (`--ano 2016` since reference data exists there)
6. Run `validate_output.py` against reference outputs
7. Full run for all years

---

## Resolved questions

- **Q1**: Null `id_conta_bd`/`conta_bd` for 2022–2023 is acceptable — leave as is.
- **Q2**: Unknown whether ativo/passivo conta names overlap for 1998–2003. Test empirically: after melt+merge, check for unexpected duplicates per `(id_municipio, conta)`. If duplicates exist, investigate and document.
- **Q3**: `Coluna` is expected to always be end-of-year. Before dropping it: tabulate unique values of `Coluna` across a sample of years and assert all are `31/12/{ano}`. If any are not, add a filter step.
