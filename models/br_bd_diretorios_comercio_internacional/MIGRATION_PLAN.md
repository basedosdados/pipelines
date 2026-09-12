# Migration Plan — `sistema_harmonizado` → `diretorios_comercio_internacional`

Move the international HS classification directory out of `br_bd_diretorios_mundo`
("world directories") into a new, trade-specific directory dataset, and add
per-revision HS tables so trade datasets can run **exact** dbt `relationships` tests.
Requested by user 2026-08-11.

Status: **DRAFT — plan only.** The destructive steps (dropping the old prod table) are
gated behind the verification checkpoint.

---

## 1. Proposed end-state

New dataset **`diretorios_comercio_internacional`** (GCP `br_bd_diretorios_comercio_internacional`,
org `bd`), containing:

- `sistema_harmonizado` — the **existing** combined SH2/SH4/SH6 reference, moved as-is
  (this is what keeps `br_me_comex_stat` working).
- `hs1992`, `hs1996`, `hs2002`, `hs2007`, `hs2012`, `hs2017`, `hs2022` — **new**
  per-revision HS6 tables (from BACI `product_codes_HS*` files).
- `sitc` — **new** SITC rev.2 table (from OEC / Harvard Atlas).

Stays in `br_bd_diretorios_mundo`: `pais`, `continente`, `nomenclatura_comum_mercosul`
(NCM). **NCM is a Brazilian (Mercosul) nomenclature, not an international one** — I did
*not* move it. If you want it in `diretorios_brasil` instead, that is a separate decision
(§6, Q2).

## 2. Blast radius (verified via repo grep, 2026-08-11)

Every dbt `relationships`/`custom_relationships` test targeting
`ref('br_bd_diretorios_mundo__sistema_harmonizado')`:

| Consumer file | Model / column | Field | Type |
|---|---|---|---|
| `models/br_me_comex_stat/schema.yml:27` | table 1, `id_sh4` | `id_sh4` | relationships |
| `models/br_me_comex_stat/schema.yml:79` | table 2, `id_sh4` | `id_sh4` | relationships |
| `models/br_bd_diretorios_mundo/schema.yml:106` | `nomenclatura_comum_mercosul`, `id_sh6` | `id_sh6` | custom_relationships (ignores `811261`, `382739`) |

Three tests, two consumer datasets (one of them internal). That is the entire dbt surface.

**Also to check (backend, not dbt):** columns in other datasets may carry a *metadata*
`directory_column` link to `br_bd_diretorios_mundo.sistema_harmonizado:id_sh{2,4,6}`
(the site-level FK, independent of dbt). Reusing the table's existing **column UUIDs**
when moving preserves these automatically; still, run a backend query for reverse
directory links to `sistema_harmonizado` and repoint any found. (Discovery step in §4.)

## 3. dbt changes (one PR, atomic — refs must never dangle)

1. Create `models/br_bd_diretorios_comercio_internacional/`; move
   `br_bd_diretorios_mundo__sistema_harmonizado.sql` there, rename to
   `br_bd_diretorios_comercio_internacional__sistema_harmonizado.sql`, update its
   `config(schema="br_bd_diretorios_comercio_internacional", alias="sistema_harmonizado")`.
   The SELECT body (staging source) is unchanged.
2. Add the new dataset's `schema.yml` with the moved `sistema_harmonizado` block + the
   8 new per-revision/SITC tables.
3. Add per-revision model files `br_bd_diretorios_comercio_internacional__hs1992.sql` … etc.
4. Repoint the 3 tests to `ref('br_bd_diretorios_comercio_internacional__sistema_harmonizado')`
   (br_me_comex_stat ×2 on `id_sh4`; NCM ×1 on `id_sh6`). Cross-dataset `ref()` is fine.
5. Remove the `sistema_harmonizado` model + schema block from `br_bd_diretorios_mundo`.
6. `dbt_project.yml`: add the `br_bd_diretorios_comercio_internacional` node
   (`+materialized: table`, `+schema: br_bd_diretorios_comercio_internacional`).
7. `uv run dbt run/test --select br_bd_diretorios_comercio_internacional br_me_comex_stat
   br_bd_diretorios_mundo` in dev — all three green before commit.

## 4. Backend + prod BigQuery migration (world_wb_wdi precedent)

Follow the same pattern used for `mundo_bm_wdi → world_wb_wdi` (reuse UUIDs, repoint
cloud_tables, drop old — gated):

1. **Reverse-link discovery:** query backend for columns whose `directoryPrimaryKey`
   table is `sistema_harmonizado`; record them.
2. **Move the table record**, don't recreate: reuse the existing table UUID
   (`2399179d-…`) and its column UUIDs; change its `dataset` to the new dataset and
   repoint its cloud_table to `basedosdados.br_bd_diretorios_comercio_internacional.sistema_harmonizado`.
   Register the new dataset first (`status = under_review`).
3. Register the 8 new directory tables' metadata (columns, OL `product_category`,
   coverage `world`, cloud_tables).
4. Repoint any reverse directory links found in step 1 to the new dataset path.
5. **Merge → table-approve** materializes `br_bd_diretorios_comercio_internacional.*` in prod.
6. **Verify** prod: new tables resolve, row counts match, br_me_comex tests pass against
   the new location.
7. **[gate]** Only then drop the orphaned `basedosdados.br_bd_diretorios_mundo.sistema_harmonizado`
   and publish the new dataset.

## 5. Sequencing vs. the BACI onboarding

The migration and the per-revision table build are the **prerequisite** for
`world_cepii_baci`'s exact FKs. Recommended order:

1. **PR A — directory dataset + migration:** create `diretorios_comercio_internacional`, move
   `sistema_harmonizado`, repoint the 3 dbt tests, add pass-1 directory tables
   (`hs1992`, `hs2017`, `sitc`). Land + verify + drop old table.
2. **PR B — `world_cepii_baci` pass 1:** trade + complexity tables FK to PR A's tables.
3. **PR C — pass 2 backfill:** `hs1996/2002/2007/2012/2022` directory + trade tables.

Keeping the migration in its own PR (A) isolates the breaking directory change from the
new-data onboarding, so a problem in one does not block the other.

## 6. Decisions to confirm

1. **Dataset name:** `diretorios_comercio_internacional` (GCP `br_bd_diretorios_comercio_internacional`)?
   Alternative English-style: `world_trade_directories`.
2. **NCM:** leave `nomenclatura_comum_mercosul` in `diretorios_mundo` (recommended, it is
   Brazilian), or relocate it (to here, or to `diretorios_brasil`)?
3. **Do the migration now or after BACI pass 1?** Plan orders it first (PR A) so trade
   FKs are exact from the start; say if you'd rather ship BACI against the existing
   `sistema_harmonizado` first and migrate later.
