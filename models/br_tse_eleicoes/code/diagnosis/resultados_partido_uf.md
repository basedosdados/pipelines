# `resultados_partido_uf` — diagnóstico

**Builder:** `sub/results_state.py::build_partido`
**Família raw:** `votacao_partido_uf` · **Granularidade:** partido × UF (histórico)
**A jusante:** intermediário histórico
**Status do harness:** OK (limpo)

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

Nenhuma incompatibilidade. Os índices posicionais batem com o layout oficial em todos os anos verificados.

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_partido` L303 — OK — anos 1945–1947, 1950, 1954–1955, 1958–1962, 1965–1966, 1970, 1974, 1978, 1982, 1986, 1990

- **Prod:** limpo. Intermediário histórico 1945–1990 (exceto 1989).

## 3. Mudanças necessárias

Nenhuma mudança de mapeamento necessária. Manter como referência de controle positivo (re-rodar o harness após cada refresh do TSE).

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** limpo — controle positivo.
