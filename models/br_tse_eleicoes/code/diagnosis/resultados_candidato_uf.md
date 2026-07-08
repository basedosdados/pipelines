# `resultados_candidato_uf` — diagnóstico

**Builder:** `sub/results_state.py::build_candidato`
**Família raw:** `votacao_candidato_uf` · **Granularidade:** candidato × UF (histórico)
**A jusante:** `resultados_candidato` (rollup histórico)
**Status do harness:** OK (limpo)

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

Nenhuma incompatibilidade. Os índices posicionais batem com o layout oficial em todos os anos verificados.

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_candidato` L115 — OK — anos 1955, 1960, 1965
- ✓ `build_candidato` L144 — OK — anos 1986, 1989–1990
- ✓ `build_candidato` L178 — OK — anos 1945–1947, 1950, 1954, 1958, 1962, 1966, 1970, 1974, 1978, 1982

- **Prod:** limpo. Layouts de 1945–1990 via leiame validam OK hoje.

## 3. Mudanças necessárias

Nenhuma mudança de mapeamento necessária. Manter como referência de controle positivo (re-rodar o harness após cada refresh do TSE).

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** limpo — controle positivo.
