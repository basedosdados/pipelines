# br_mj_sisdepen — architecture notes

Source inspection: 2026-09-28. Coverage report: see the onboarding session output.

## Coverage shipped

**2016/2 – 2025/2, 19 semiannual cycles.** Partitioned by `ano`.

`2005–2015 is not shipped.` The INFOPEN historical host `dados.mj.gov.br` no longer
resolves (NXDOMAIN — the host is gone, not a 404). `dados.gov.br`'s CKAN API now
requires a key (HTTP 401) and SENAPPEN's own open data page carries no historical
microdata. The series is currently unobtainable; treat it as a separate follow-up.

The SISDEPEN downloads page carries all 19 cycles plus a legacy 2016 INFOPEN base
(`2016_basefinal_depen_publicacao_revisado`, 1,117 columns, different layout). That
legacy file is **not** ingested — its layout does not map onto generations A–D.

## Schema generations — there is no 2019 break

The instrument does not change in 2019. Cycles 2–9 (2017/1 through 2020/2) share a
byte-identical 1,333-column header, spanning the supposed discontinuity.

| Generation | Cycles | Semesters | Columns |
|---|---|---|---|
| A | 1 | 2016/2 | 1,327 |
| B | 2–9 | 2017/1 – 2020/2 | 1,333 |
| C | 10–13 | 2021/1 – 2022/2 | 1,334 |
| D | 14–19 | 2023/1 – 2025/2 | 1,737 |

The 1,334 → 1,737 jump at 2023/1 is almost entirely block 5.8 (nationality),
expanding from 233 to 636 columns — a country-list expansion, not a redesign. Every
questionnaire section from 1.1 to 7.5 is present with identical sub-item counts in
both 2019/1 and 2025/2.

`geracao_esquema` carries A–D. A `fonte` column distinguishing INFOPEN from SISDEPEN
would be constant across every file on the page and is therefore not shipped.

## Source quirks that the cleaning code must handle

1. **`.` is the decimal separator.** Values are written as floats (`0.0`, `759.0`,
   and IBGE codes as `1200401.0`). Stripping `.` as a thousands separator inflates
   every quantity tenfold.
2. **Block 1.3 publishes both regime totals and sex margins.** The seven regime
   `| Total` columns and the `Masculino | Total` / `Feminino | Total` columns are
   two views of the same quantity. Summing all nine double counts. The two
   constructions reconcile in 100% of rows in all 19 cycles — use that as a test.
3. **One column is renamed in cycles 16 and 18** (the two `retificado` files):
   `Tipo do Estabelecimento` → `Tipo de Recolhimento`, same position.
4. **Published totals are discarded.** Blocks 4.1 and 5.x publish `Total` rows and
   columns alongside their components. Only components are ingested.
5. **`Situação de Preenchimento` is `Validado` for all 28,825 rows** and
   `Situação do Estabelecimento` is `Ativo` for all of them. The source publishes
   only validated returns, so a non-reporting unit is absent and unflagged.

## Table shapes, and why `populacao_prisional` is not a cross-tabulation

Blocks 5.1 (age), 5.2 (race), 5.4 (marital status) and 5.6 (education) are
**separate marginals**, each crossed only with sex. The source publishes no joint
age × race × education distribution. A single table carrying all three as columns
would assert a joint distribution that does not exist.

Hence two population tables:

- **`populacao_prisional`** — block 4.1, a genuine joint:
  `situacao_processual` × `regime` × `esfera_justica` × `sexo`.
- **`populacao_caracteristica`** — blocks 5.1/5.2/5.4/5.6 in long form:
  `caracteristica` × `categoria` × `sexo`, each row carrying `condicao_registro`.

`condicao_registro` is load-bearing, not decorative. It records the establishment's
own answer to *"tem condições de obter estas informações em seus registros?"* with
three levels (all / part / none). When the answer is `parte`, the counts are real but
do **not** sum to the establishment's own total. Item non-response ranges from 0.0%
(PA) to 37.9% (MT) and falls from 22.3% to 1.8% between 2016/2 and 2025/2, so a
breakdown pooled across years mixes very different completeness regimes.

## Unit identity

The source publishes **no establishment identifier** in any cycle. `id_unidade` is
reconstructed by one-to-one assignment between consecutive cycles within
municipality, scoring `0.80 × name-token Jaccard + 0.20 × capacity proximity`
(threshold 0.55), followed by a pass linking trajectory ends to later starts to
recover genuine gaps.

| Metric | Value |
|---|---|
| Distinct units | 2,384 |
| Same-cycle collisions | 0 of 28,825 (structurally impossible by construction) |
| Max cycles per unit | 19 (ceiling respected) |
| Successor slots linked | 26,344 / 27,344 (96.3%) |
| Ambiguous links (runner-up within 0.05) | 184 (0.70%) |

Two approaches were tried and rejected. Exact normalised names under-merge: MG
renamed its APAC units between cycles 8 and 10 (`APAC - ARCOS` → `Apac Arcos I`),
inflating MG's expected count to 420 against a true row count of 216–244 and
manufacturing a fake 190-unit non-response episode. Jaccard with transitive
union-find over-merges: 6.2% of unit-cycles held two rows and ES showed 27 rows per
unit against a ceiling of 19, because transitive closure chains A~B~C when A≁C.

Matching provenance ships as its own table, `unidade_crosswalk`, rather than being
buried in the cleaning code.

A prison-unit **directory** does not exist in `br_bd_diretorios_*`. `id_unidade`
therefore carries no `directory_column`. If these units are referenced from another
dataset later, a directory should be created first.

## Open items for the metadata step

- GCP dataset id is **`br_mj_sisdepen`**: organization stays `mj` (Ministério da
  Justiça), dataset slug becomes `sisdepen`. Organizations `senappen` and `depen` do
  not exist in the backend and are not created.
- The shell to reuse is `levantamento_nacional_de_informacoes_penitenciarias_infopen`
  (prod id `5ee80380-83fd-4e13-b145-437cb227a087`, org `mj`, `tables: {}`). Reusing it
  means renaming its slug to `sisdepen` and widening its description to the
  2016–2025 SISDEPEN series; the organization is unchanged.
- Measurement unit `semester` is not yet used anywhere in this repo; confirm it
  resolves in the backend before column upload, or fall back to leaving it blank.
- The dev backend returned HTTP 503 during this session, so metadata registration
  targets the **staging** backend instead (`env="staging"`).

## Cleaning output

`code/clean.py` builds all seven tables in about 15 seconds from the 19 cycle
files. It asserts, and fails rather than writing, on:

- **capacity reconciliation** — block 1.3's seven regime totals against its two
  sex margins, per cycle. Holds at 100% in all 19 cycles; a mismatch means the
  numeric parse is wrong (see source quirk 1).
- **no same-cycle collisions** in the crosswalk, which the one-to-one assignment
  makes structurally impossible.
- **population components reconcile** with the establishment totals (15,274,185
  person-records) and **capacity by regime** with the panel (10,434,069).
- **grain uniqueness** on every table's declared key.

| Table | Rows | Partitions |
|---|---|---|
| `unidade_prisional` | 201,775 | ano=2016..2025 |
| `populacao_prisional` | 1,037,700 | ano=2016..2025 |
| `populacao_caracteristica` | 1,790,112 | ano=2016..2025 |
| `uf_semestre` | 513 | ano=2016..2025 |
| `unidade_crosswalk` | 28,825 | ano=2016..2025 |
| `cobertura` | 513 | ano=2016..2025 |
| `dicionario` | 20 | unpartitioned |

Output is Snappy parquet with **every column STRING**, per the staging
convention, cast through arrow rather than `astype(str)` so that NULL stays NULL
instead of becoming the literal `"nan"`. Verified: 0 such literals across all
3.06M rows.

`data_inauguracao` is preserved as reported. Three establishments declare
implausible dates (1201, 1500) and ten predate 1900; these are source data-entry
errors, not parse failures, and are not silently corrected.
