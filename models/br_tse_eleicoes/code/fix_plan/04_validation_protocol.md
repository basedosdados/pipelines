# 04 — Validation protocol

Two gates. Gate A proves the Python code reproduces Stata; Gate B proves the
fixed code parses fresh TSE downloads correctly. Both must pass before any
upload (05).

**Never run two validations concurrently — RAM explosion. One table × year
at a time, sequentially.**

## Gate A — cell-by-cell parity vs Stata (old vintage)

Reference: `~/Downloads/dados_TSE/output/*.dta` (hydrated per 01), built by
the Stata code from the raw files in `~/Downloads/dados_TSE/input/`. Running
the fixed Python pipeline **on those same inputs** must reproduce them.

1. Extend `validate.py` from sampled comparison to full-cell comparison:
   - row count, column set/order, then a full anti-join or hash-per-row
     comparison after canonical sort (define the sort key per table —
     use each table's dbt uniqueness-test key).
   - normalize representations before comparing (NA vs "", float tolerance
     1e-9, date formats) and log every normalization applied.
   - stream in chunks; peak RSS < 16 GB.
2. Run per table × year, writing one line per cell to
   `fix_plan/parity_matrix.md`: `MATCH` / `MISMATCH(n cells, cols…)` /
   `SKIPPED(reason)`.
3. **Expected mismatches**: cells where the *Stata-era output itself* is
   corrupted (Stata parsed a republished file with stale positions — e.g.
   `bens_candidato` 2014, `candidatos` 1996 demographics NULL). For each
   mismatch: classify as (a) Python bug → fix, (b) known Stata-era
   corruption → document in the parity matrix with the DIAGNOSIS.md
   reference, (c) unexplained → stop and escalate to user.
   No cell stays unexplained.

## Gate B — fresh downloads (new vintage)

1. Download fresh raw per family (disk budget per 01; keep the download
   manifest: URL, date, sha256 of each zip).
2. `uv run --with pdfplumber python -m diagnostics run --tier 2` then
   `--tier 3` → must exit 0 (this re-validates layouts against today's TSE,
   catching any republication since Pisa's run — drift is ongoing:
   TSE inserted a column in 2024 files *after* the refactor was written).
3. Build each table from fresh raw with the fixed code, then invariant
   checks per table × year:
   - value domains: `sigla_uf` valid, `titulo_eleitoral` 10–13 digits,
     `votos` ranges sane vs neighboring years, no raw TSE codes in decoded
     columns, TSE sentinels (`#NULO#`, `#NE#`, −1, −3, −4) cleaned.
   - volume: row count within expected band of the old-vintage build for the
     same cell; investigate any |Δ| > 1% (TSE genuinely revises data —
     document confirmed revisions, don't paper over them).
   - cross-table: aggregation identities (e.g. Σ `rcmz.votos` by
     mun = `rcm.votos`; comparecimento = votes accounting in detalhes).
4. Output: `fix_plan/freshness_report.md` — per cell PASS/FAIL + documented
   genuine TSE revisions.

## Acceptance

- [ ] Parity matrix complete: every table × year MATCH or documented (b)
- [ ] Tier 2+3 green on fresh layouts
- [ ] Freshness report complete, all FAILs resolved or escalated
- [ ] dbt tests pass on dev after 05's upload (checked there)
