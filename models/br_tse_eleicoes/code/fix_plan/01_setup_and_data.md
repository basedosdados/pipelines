# 01 — Setup and reference data

Goal: a working branch with the refactor code, a Python environment that runs
the pipeline and the diagnostics harness, and hydrated reference data for the
parity gate.

## 1. Branch

Work on `fix/br_tse_eleicoes_diagnosis`, created off
`origin/feat/refactor_eleicoes` (PR #1476). All code lives under
`models/br_tse_eleicoes/code/python/`.

## 2. Environment

From `models/br_tse_eleicoes/code/python/`:

```bash
uv run python -c "import pandas, pyarrow; print('ok')"
uv run --with pdfplumber python -m diagnostics report
```

The harness reads/writes `diagnostics/artifacts/`. `TSE_DATA_DIR` points at
the data root (default in `config.py`; the historical runs used
`/tmp/dados_TSE` — for this plan use `~/Downloads/dados_TSE` once hydrated).

## 3. Reference data — Dropbox hydration (USER ACTION REQUIRED)

`~/Downloads/dados_TSE/` holds the canonical tree (`input/`, `output/`,
`output_python/`), but **all 9,785 files are 0-byte Dropbox online-only
placeholders** (`com.dropbox.placeholder` xattr). Nothing can be validated
until hydrated. Local disk has ~75 GB free — the full tree probably does not
fit, so hydrate selectively, per table family, and drop when done.

Priority order for hydration (smallest useful set first):

1. `output/*.dta` — the Stata reference outputs (ground truth for Gate A).
2. `input/consulta_cand/` — candidatos is the linchpin table.
3. Per-family raw inputs, one family at a time, in the order of work order 02.

To hydrate: in Finder, right-click the folder → "Make available offline"
(or `dropbox` CLI if configured). A session finding a 0-byte file must stop
and report — never treat an empty placeholder as an empty dataset.

## 4. Sanity checks before starting 02

- [ ] `git branch --show-current` = `fix/br_tse_eleicoes_diagnosis`
- [ ] diagnostics harness runs: `uv run --with pdfplumber python -m diagnostics report`
- [ ] `~/Downloads/dados_TSE/output/candidatos_1994.dta` is > 0 bytes
- [ ] Record in the status board which families are hydrated

## Disk budget

Track with `df -h` before each family. If a family does not fit: hydrate →
build → validate → evict (`Remove offline copy` / delete local cache) before
the next family. Large families: `prestacao_contas`, `votacao_secao`,
`detalhe_votacao_secao`, `perfil_eleitor_secao`.
