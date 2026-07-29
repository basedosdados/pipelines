# 02 — Header-aware parsing (fixes B1 + B2)

Goal: eliminate the entire positional-mapping failure class. After this work
order, no builder mis-parses any raw vintage, past or future.

## Design (agreed direction — DIAGNOSIS.md recommendation 2)

TSE's current republication of every year ≥ 1994 carries a header row
(verified back to 1994). The old-generation downloads (the vintage the Stata
code was written for, still in `~/Downloads/dados_TSE/input/`) are mostly
headerless. Therefore:

1. **Detect the header per file** in `utils/helpers.py::read_raw_csv`:
   peek at row 1; a TSE header row is `;`-separated ALL-CAPS names
   (`DT_GERACAO`, `SG_UF`, …) or the quoted equivalent. If present → read
   with `header=0` and **select/rename by name**; if absent → fall back to
   the existing positional year-block (unchanged, for old vintages and the
   headerless historical `*_uf` files).
2. **Name → BD-column mapping** comes from one place: a per-family mapping
   table derived from `diagnostics/artifacts/layouts/*.json` (the official
   name→position layouts Pisa's harness already fetched) plus the BD column
   affinities in `diagnostics/affinity.py`. Encode it as a module
   (`utils/header_maps.py` or per-builder dicts) — no scattered literals.
   TSE's idiosyncratic 2002–2016 finance header names are already
   whitelisted in `affinity.py`; reuse that.
3. Positional blocks stay only where no header can exist. Each surviving
   positional block gets a comment naming the vintage it parses.

Keep the diagnostics harness meaningful: tier 1/3 validate positional sites
via AST. After the refactor, remaining positional sites must still pass, and
the header path needs its own check — add a small tier-3 extension or unit
test asserting that for each family × year with an official layout, the
header map covers every BD column the builder emits.

## Work items (one commit each; test after each)

Order = blast radius. `candidatos` first: it feeds `norm_candidatos`, which
enriches five downstream tables.

| # | Builder | FAIL/WARN years | Notes |
|---|---|---|---|
| 2.1 | `sub/candidates.py` | FAIL 1994, 1996, 2014, 2018, 2020, 2022 | linchpin; also OUT_OF_RANGE v51/v54 (1994) and v51–v58 (2020/2022) |
| 2.2 | `sub/parties.py` | FAIL 1994–2014, 2018, 2020 | 4 distinct broken blocks |
| 2.3 | `sub/vacancies.py` | FAIL 1994–2012 | `vagas ← SG_UF` ⇒ NULL after int cast |
| 2.4 | `sub/results_mun_zone.py` (candidato) | FAIL 1994, 1996, 2016, 2018–2022 | 1994 `votos ← SG_FEDERACAO` ⇒ zeros (prod smoking gun) |
| 2.5 | `sub/results_mun_zone.py` (partido) | FAIL 1996, 2016 | |
| 2.6 | `sub/voting_details_mun_zone.py` | FAIL 1996–2016 | uncleaned `-3` sentinel in 1996 too (see 03/B3 sibling issue) |
| 2.7 | `sub/voter_profile_mun_zone.py` | FAIL 2008–2016, 2020–2024 | 2024: TSE inserted `TP_OBRIGATORIEDADE_VOTO` post-refactor |
| 2.8 | `sub/campaign_finance.py` (despesas 2014) | FAIL 2014 | plus WARN: receitas 2014 `_sup` variant has its own layout — verify against the actual file |
| 2.9 | `sub/voter_profile_section.py` | WARN 2008–2022 (`cd_mun_sit_biometrica` MISSING_KEY) | confirm against real headers |
| 2.10 | `sub/voting_details_state.py` | WARN 1945–1990 (leiame-only) | headerless; verify v13 `votos_validos` vs `QTD_VOTOS_NOMINAIS` against Stata output, document |
| 2.11 | `sub/results_section.py` | WARN 1998, 2008 legacy variants | zips absent from current portal; keep positional, document |

## Per-item procedure

1. Read the per-table diagnosis file in `../diagnosis/<table>.md`.
2. Implement header path; keep positional fallback byte-identical.
3. Unit check: parse the first 1,000 rows of each available year in both
   modes where both exist; assert identical output on old-vintage files.
4. Run harness for that family; update `../diagnosis/<table>.md` status.
5. Commit `fix(br_tse_eleicoes): header-aware parsing for <table>`.

## Acceptance (work order done)

- [ ] `uv run --with pdfplumber python -m diagnostics run --tier 3` exits 0
      (no FAIL cells) on fresh-download layouts
- [ ] All surviving positional blocks documented with their vintage
- [ ] Old-vintage parses byte-identical to pre-refactor behavior (spot-check
      per family, 3 years each)
- [ ] `DIAGNOSIS.md` status matrix regenerated (`python -m diagnostics report`)
