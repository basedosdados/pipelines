# br_ufmg_censo_demografico_1872

The first general census of Brazil, taken in 1872 under the Empire, as digitised
and corrected by the Núcleo de Pesquisa em História Econômica e Demográfica
(NPHED) of Cedeplar/UFMG and distributed as the **Pop-72** database.

Source: <http://www.nphed.cedeplar.ufmg.br/pop-72-brasil/>
Data file: `Pop-1872-Brasil_versao1_0.zip` → `Pop-1872-Brasil_versao1_0.mdb`
(Microsoft Access Jet4, 75 MB, last modified at source 2017-06-18).

## Two versions of every table

The Access file ships each cross-tabulation twice, and both are published here:

| Suffix | Meaning |
|---|---|
| `_original` | The figures as printed by the Diretoria Geral de Estatística in 1872 |
| `_corrigido` | The same figures with NPHED's arithmetic corrections applied |

The difference is not cosmetic. The corrected tables reproduce the canonical
national totals — **9,930,478 persons, of whom 1,510,806 enslaved** — while the
original tables sum to 9,952,948 and contain 367 parish rows where
`total_livres + total_escravizados` does not equal `total`. That is the error
NPHED's *Relatório crítico do censo de 1872* documents, and it is preserved here
rather than silently fixed. Use `_corrigido` unless you specifically need to
reproduce the 1872 publication.

Two tables exist only in corrected form, because the source provides no original
counterpart: `populacao_total_idade_corrigido_*` (present plus absent population)
and `resumo_geral_corrigido_*`.

## Three geographic levels

The parish is the census's native unit: 1,440 parishes carry data, out of 1,473
listed. The 5-digit parish id encodes the hierarchy exactly —
`id_paroquia // 100 == id_municipio_1872` and
`id_municipio_1872 // 100 == id_provincia` — so the `_municipio` and `_provincia`
tables are sums over the parishes they contain. `clean.py` asserts that encoding
before aggregating, and the three levels agree to the unit.

Six of the 642 municipalities have no parish with data, so the municipality
tables cover 636.

**The geographic codes are Pop-72 codes, not IBGE codes.** The source publishes no
crosswalk to the present-day municipal grid, so none is asserted here. This is why
the column is `id_municipio_1872` rather than `id_municipio`: joining these values
against `br_bd_diretorios_brasil.municipio` would silently produce nonsense.

## Table layout

| Stem | Content | `id_categoria` varies over |
|---|---|---|
| `domicilio` | Inhabited houses, uninhabited houses, fogos | — |
| `populacao_geral` | Population by sex and free/enslaved condition | colour, marital status, religion, nationality, literacy, disability, absent, transient |
| `populacao_presente_idade` | Population present, by sex, colour, condition | age band |
| `populacao_ausente_idade` | Population absent from the parish | age band |
| `populacao_total_idade` | Present plus absent (corrected only) | age band |
| `homem_origem_brasileira` | Brazilian-born men by marital status, colour, condition | province of birth |
| `mulher_origem_brasileira` | Brazilian-born women, same breakdown | province of birth |
| `estrangeiro_nacionalidade` | Resident foreigners by religion, marital status, sex | nationality of origin |
| `profissao` | Population by nationality, marital status, sex | occupation |
| `resumo_geral` | Summary by sex and condition (corrected only) | all axes above, stacked |

Each stem is published as `<stem>_<versao>_<nivel>`. With three geography lookup
tables (`provincia`, `municipio`, `paroquia`) and `dicionario`, that is 58 tables.

`id_categoria` is a code in every data table; `dicionario` resolves it per table.
Its `valor` prefixes the category group where the label alone would be ambiguous
across groups ("Raças: Branco", "Faixa Etária: 6-10 anos").

## Column naming

Counts are named for what they count, in Portuguese, with the census's own colour
categories (`branco`, `pardo`, `preto`, `caboclo`). Two conventions worth knowing:

- Columns say **`escravizado`** where the 1872 source says "escravo". Each
  column's `original_name` in the architecture preserves the source spelling.
- The men's and women's origin tables carry **gendered** column names
  (`solteiras_brancas_livres` in the women's table), even though the source uses
  one masculine column set for both. The sex is a property of the table there, not
  of the column, so a column in the women's table is never spelled masculine.

## Running it

```sh
# 1. download the .mdb and dump every table to CSV (needs: brew install mdbtools)
uv run python models/br_ufmg_censo_demografico_1872/code/extract.py

# 2. clean into partitioned, all-STRING parquet (58 tables)
uv run python models/br_ufmg_censo_demografico_1872/code/clean.py

# 3. regenerate architecture CSVs, dbt models, schema.yml and columns.json
uv run python models/br_ufmg_censo_demografico_1872/code/generate_artifacts.py
uv run pre-commit run --files models/br_ufmg_censo_demografico_1872/*

# 4. upload to BigQuery
uv run python models/br_ufmg_censo_demografico_1872/code/upload.py --env dev
```

Intermediate data goes to `~/Downloads/br_ufmg_censo_demografico_1872_data/`
(override with `CENSO_1872_DATA`), never into the repo.

`spec.py` is the single source of truth. The architecture tables, the dbt casts
and the column descriptions are all generated from it, so they cannot drift from
the transform. Editing a column name means editing `spec.py` and re-running steps
2–4.

## Auxiliary files

`auxiliary_files.zip` bundles the Pop-72 tutorial and the *Relatório crítico*,
both of which apply to every table in the dataset. It is stored **once** at
`auxiliary_files/br_ufmg_censo_demografico_1872/auxiliary_files.zip` rather than
copied under each of the 58 table prefixes, which would mean 58 byte-identical
5.9 MB objects.
