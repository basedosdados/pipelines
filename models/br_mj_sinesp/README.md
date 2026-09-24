# br_mj_sinesp — Sistema Nacional de Estatísticas de Segurança Pública (SINESP VDE)

National public security statistics reported monthly by the State Statistics
Managers of the 26 Brazilian states and the Federal District through SINESP VDE,
and published by the Ministry of Justice and Public Security as one workbook per
year (`bancovde-YYYY.xlsx`), 2015 onwards.

## Tables

| Table | Grain | Rows |
|---|---|---|
| `municipio_mes` | municipality × month × occurrence type × reporting body | ~8.9M |
| `uf_mes` | state × month × occurrence type × reporting body × (weapon / agent / age band) | ~294k |
| `dicionario` | stable key → raw source label, per year | ~100 |

Both fact tables are **long on `tipo_ocorrencia`**, never one column per crime.
The source's indicator list is currently stable, but which reporting bodies
contribute to a series changes between years; a wide table would break on that.

## Three things to know before using this data

**1. Only 11 of the 31 occurrence types are published by municipality.** The
other 20 exist only in `uf_mes`. Notably **estupro, roubo de veículo, furto de
veículo, roubo a instituição financeira and roubo de carga are state-level
only** — there is no municipal series for them at any point in 2015–2026.

**2. `abrangencia` is a reporting body, not a grain.** The same occurrence type
is reported independently by `Estadual`, `Polícia Federal` and `Polícia
Rodoviária Federal` for the same municipality and month. Summing across
`abrangencia` without meaning to double-counts.

**3. Missing and zero are different, and both are represented.** Three states
are distinguishable:

| `situacao_registro` | measures | meaning |
|---|---|---|
| `reportado` | `0` | the source reported a zero |
| `reportado` | `NULL` | the source published a row and left the value blank |
| `nao_reportado` | `NULL` | the source published no row for this municipality-month |

Nothing is imputed. A `nao_reportado` row is emitted for every municipality-month
the source omits from a series it otherwise reported that year.

## The source emits several rows per cell, and they must be summed

The workbooks are not unique on their own key. Three situations produce repeats,
and in every one the repeated rows are **components, not copies**:

- **The Distrito Federal is reported once per administrative region** — 33 of
  them — with the region name replaced by `BRASÍLIA`. Their values differ: in
  January 2023 the 33 DF `feminicidio` rows read `0,0,0,0,0,3,…,1,…,1,…` and sum
  to 5. Taking the first row would report zero.
- Some state-level cells appear twice in one file (2,426 groups across the series).
- July 2016 `mandado_de_prisao_cumprido`/Polícia Federal is republished for every
  municipality (26,615 groups), with no values in either copy.

Classifying every repeated group across all twelve years gives 1,229 with
differing non-zero values, 2,754 all-zero, 26,639 all-null, and **none** that are
identical copies carrying a non-zero value. Summing is therefore safe: no case
exists in which it could double a real number. The cleaner aggregates on the
output key, with NULL preserved as distinct from zero — a sum of blanks stays
blank rather than becoming a reported zero.

Consequence for users: **a Distrito Federal row is the sum of its 33
administrative regions.** The regions themselves are not recoverable, because the
source discards their names.

## Coverage gaps

Measured against (operating municipalities per the BD directory) × (months in
the file), **936 of 779,800 municipality-months are missing — 0.12%**:

- **Espírito Santo publishes nothing at municipal level in 2015.** Complete from
  2016. This is the only whole state-year blackout, and the only real gap.
- Ragged series end-dates, uniform across all 27 states, not municipality gaps:
  `tentativa_de_feminicidio` (TO absent 2015; SP stops October 2015; TO stops
  September 2018), `mandado_de_prisao_cumprido`/Polícia Federal (stops October
  2016), `morte_no_transito`/PRF (2026 reaches July while the state series
  reaches August).
- `mandado_de_prisao_cumprido`/Polícia Federal carries rows but **no values at
  all** in 2015 (100% NULL), and the Estadual series is 52% NULL that year.

Boa Esperança do Norte/MT (`5101837`) is in the BD directory but was never an
operating municipality in SINESP; it is excluded from the benchmark rather than
reported as a gap in every year.

## Publication lag

The source states that validated data are available *"até o décimo quinto dia do
mês subsequente"* — roughly a one-month lag — and that figures *"refletem o nível
de alimentação e consolidação de cada UF na data de sua extração, podendo ocorrer
atualizações posteriores à publicação"*. **Every month is revisable and no month
is withheld.** There is no published provisional window, so none is encoded here.

## Municipality matching

The source publishes municipality *names*, not IBGE codes. Names are normalised
(accent-free, uppercase, punctuation-stripped) and joined to
`br_bd_diretorios_brasil.municipio` on `(sigla_uf, nome)`, which is unique.
Thirteen SINESP spellings differ from the directory and are resolved by an
explicit map in `code/utils.py` (`NAME_OVERRIDES`) — for example *Itapajé* for
*Itapagé*, *São Tomé das Letras* for *São Thomé das Letras*, and *Januário
Cicco* for the renamed *Boa Saúde*. With those, **match rate is 100% in every
year**; the cleaner raises rather than dropping an unmatched name.

## Test scoping

`municipio_mes` is 8.8M rows, so its model-level and `relationships` tests are
scoped with `config: where: __most_recent_year__` per the repo's convention for
large tables. Unfiltered, each test is a full scan, and the set of them is
enough on its own to trip the project's **daily BigQuery byte quota** — which is
shared across every pipeline, so the cost lands on other people's runs too.

Key uniqueness across **all** years is checked before upload instead, by
`code/validate.py`, which reads the parquet directly and costs nothing. That is
the check that caught the Distrito Federal aggregation bug above.

`uf_mes` (292k rows) and `dicionario` (89) are cheap and stay unscoped.

## Running it

```bash
cd models/br_mj_sinesp/code
python run_onboarding.py          # download + clean all years
python validate.py                # local checks before upload
python upload.py                  # -> basedosdados-dev
```

Scratch data goes to `$SINESP_DATA_DIR` (default `~/Downloads/br_mj_sinesp_data`),
never into the repo or Dropbox, and is deleted once the onboarding is verified.

## Source quirks

- `dados.mj.gov.br` does not exist. The live source is the gov.br page linked above.
- gov.br returns **403 to HEAD** and to a bare user agent; a browser UA plus a
  `Referer` is required (`constants.HTTP_HEADERS`).
- Downloads **truncate silently on HTTP 200** — 2016 and 2019 both arrived short
  on the first pass. `utils.is_complete_xlsx` validates the zip central directory
  before a file is accepted.
