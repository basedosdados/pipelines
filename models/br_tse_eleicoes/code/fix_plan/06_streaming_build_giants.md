# 06 — Streaming build for the giant seção tables

> **STATUS 2026-07-31: DONE — build + partition wired end-to-end**
> (commits 677c3cd0 build, 6e9bb4ca wiring). Both OOM sites eliminated:
> `build.py` routes the two giant steps through
> `streaming_secao.build_all_*` (per-`(ano,uf)` parquet →
> `config.STREAM_SECAO_ROOT`), and
> `normalization_partition._partition_secao_table` enriches + partitions
> one `(ano,uf)` at a time (falls back to the monolithic path on a big
> host). Validated: stream==in-RAM build (byte-identical to March) incl.
> the hard SP-2018 BR-routing case; stream→partition CSVs byte-identical
> to monolithic→partition. Peak RSS 5.3 GB. The "Remaining wiring" section
> below is now implemented. No dbt/metadata/other-builder changes.


Work order 05 must rebuild every table from one uniform vintage. Three
families cannot be built in 16 GB RAM and need a streaming path:
`resultados_candidato_secao`, `resultados_partido_secao` (both from
`build_resultados_secao`), and `perfil_eleitorado_secao`. Everything else
already rebuilds fine at ≤9.4M rows (Gate B).

## Why they OOM — two sites per family

The final output is Hive-partitioned by `(ano, sigla_uf)`, but the code
currently materializes the whole year in RAM twice:

1. **Build** (`sub/results_section.py`, `sub/voter_profile_section.py`):
   the per-UF loop appends every UF's frame to a list, then
   `pd.concat(...)` + `drop_duplicates(keep="first")` over the whole year
   (60–75M rows).
2. **Partition + enrich** (`normalization_partition.py`): reads the whole
   year's intermediate parquet (`_read_parquet(name, ano)`), does the
   `titulo_eleitoral` merge against `norm_cand`, then `save_partitioned`.

A single UF can also be too big on its own: `votacao_secao_2022_SP.csv` is
**5.45 GB** uncompressed, `perfil_eleitor_secao_2024_SP.csv` **4.10 GB** —
each becomes 15–25 GB as an all-string pandas frame. So per-UF alone is
not enough; the biggest UFs need within-UF chunking too.

## Design — make `(ano, uf)` the unit of work end to end

Three structural facts make this exact and safe:

- **Partido aggregation is already per-UF.** The nominais `groupby` and
  legenda selection happen inside the UF loop; the post-loop `concat` only
  stacks per-UF results. No cross-UF state → trivially streamable.
- **Candidato dedup is `keep="first"` in UFS-order.** `dup_keys` has no
  `sigla_uf`, but it has `id_municipio_tse`, which is globally unique per
  municipality → a duplicate pair almost always shares a UF. To reproduce
  the *exact* keep-first semantics without assuming that, carry a **global
  set of 64-bit key hashes** (~0.5–1 GB for 60M rows) and drop a row iff
  its hash was already seen, processing UFs and rows in the current order.
- **The titulo merge keys include `sigla_uf`** (`merge_mod0`,
  `merge_mod2_est`; only the president subset `merge_mod2_pres` is national
  and it matches all UFs anyway). So the enrich step runs per `(ano, uf)`
  against the small `norm_cand` lookup subsets held in RAM.

### Flow

```
for ano:
  seen = set()                        # candidato dup-key hashes, this year
  for uf in UFS[ano]:                 # same order as today
    for chunk in read_raw_csv_chunks(uf_file, CHUNK):
        cand, part_partial = _process_secao_chunk(chunk, ano, uf)   # existing row logic
        cand = cand[~hash(cand[dup_keys]).isin(seen)]; seen |= new
        append cand → intermediate ano=YYYY/sigla_uf=UF/candidato.parquet
        accumulate part_partial into a per-UF running groupby-sum
    write partido running-sum → intermediate ano=YYYY/sigla_uf=UF/partido.parquet
# enrich (fused): per (ano, uf) read intermediate partition, merge titulo
# from norm_cand subsets, save_partitioned → final ano=YYYY/sigla_uf=UF/
```

Peak memory = one chunk (tunable, e.g. 2M rows ≈ 1–2 GB) + the hash set
(≤1 GB) + the small norm_cand lookups. Fits 16 GB with headroom.

## Scope — what actually changes

| File | Change | Est. lines | Risk |
|---|---|---|---|
| `utils/helpers.py` | new `read_raw_csv_chunks(path, chunksize)` — header-aware chunk generator; existing `read_raw_csv` untouched | +30 | low (additive) |
| `sub/results_section.py` | extract the per-UF row body (~L60–207) into `_process_secao_chunk()` (cut-paste); rewrite build to stream per-UF/chunk with hash-set dedup + partido running-sum; write per-`(ano,uf)` | ~120 | **medium-high** — partido chunk-accumulation is the one genuinely new bit of logic |
| `sub/voter_profile_section.py` | same per-UF/chunk pattern; simpler (groupby-sum only, no dedup, no titulo merge) | ~50 | medium |
| `normalization_partition.py` | `_partition_resultados_secao` / `_partition_perfil_secao`: iterate `(ano,uf)` partitions instead of whole-year read; titulo merge per partition | ~50 | medium |

Total ≈ **4 files, ~250 lines**, of which ~80 (partido accumulation) is
new logic and the rest is extract-method + loop restructuring. **No other
builder, no dbt model, no metadata changes** — the 11 tables that already
rebuilt in Gate B are untouched, and the output schema/partition layout is
identical (only *how* it is produced changes).

## The one subtlety: partido under chunking

Today partido = (nominais `groupby(group_cols).sum()`) outer-merged with
(legenda rows, 2-digit votavel, not aggregated). Under chunking:

- **nominais**: accumulate a running `groupby(group_cols)["votos"].sum()`
  across chunks (concat partial sums, re-sum at UF end). Exact.
- **legenda**: 2-digit-votavel rows are a small fraction; collect them
  per-UF (they fit) and outer-merge once at UF end — identical to today.

If a later check shows legenda can also be large for a UF, switch it to
the same running-sum (legenda is a per-group sum too), but the per-UF
collect is expected to suffice.

## Validation

Gate A gives byte-parity references (the March parquets). After the
refactor, validate the streamed output against them **order-independently**
with `gate_a.py` for a big year per family (e.g. 2018/2022), UF by UF —
identical multiset ⇒ semantics preserved. The hash-set reproduces
`keep="first"` exactly; the residual parity risk is confined to the
partido accumulation and int/float fill edges, which the 2018/2022 compare
will catch.

## Alternative considered

Rewriting the three builders in polars lazy/streaming would cut memory
with less manual chunking, but it is a full logic re-expression in another
library → high parity risk against the validated Stata outputs, for only
three tables. Chunked-pandas keeps the exact existing transforms and is
the lower-risk path. (If the production Prefect worker has ≥64 GB, the
builders could run unchanged there — but the streaming path is the durable
fix and makes the pipeline runnable on constrained hosts.)

## Remaining wiring (partition + enrich half)

The streamed intermediates land at
`<out_root>/<table>/ano=YYYY/sigla_uf=UF/data.parquet` with the pre-enrich
schema (`CAND_COLS` / `PART_COLS` / `PERFIL_COL_ORDER`). To finish the
production path, `normalization_partition.py` must consume them per
`(ano, uf)` instead of reading the whole-year monolithic parquet:

- `_partition_resultados_secao`: for each `(ano, uf)` partition, read it,
  run the existing `titulo_eleitoral` merge against the (small, in-RAM)
  `norm_cand` subsets, `save_partitioned`. The merge keys already carry
  `sigla_uf`, so this is a per-partition drop-in — no logic change, only
  the loop granularity (per `(ano,uf)` rather than per `ano`).
- `_partition_perfil_secao`: per-`(ano,uf)` read + `save_partitioned`
  (no titulo merge).
- Orchestration: route the three giant families through
  `stream_*` in the build step (`build.py` / the production driver)
  instead of the in-RAM `build_all()`; leave all other tables as-is.

This half is low-risk (per-partition read + existing merge) and validated
the same way — compare the final partitions to the March reference with
`gate_a.py`, order-independent, UF by UF.
