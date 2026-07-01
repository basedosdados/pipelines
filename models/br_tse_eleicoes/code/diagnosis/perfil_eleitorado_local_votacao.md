# `perfil_eleitorado_local_votacao` — diagnóstico

**Builder:** `sub/voter_profile_polling_place.py::build_perfil_local_votacao`
**Família raw:** `eleitorado_local_votacao` · **Granularidade:** local de votação
**A jusante:** folha
**Status do harness:** OK (limpo)

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

Nenhuma incompatibilidade. Os índices posicionais batem com o layout oficial em todos os anos verificados.

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_perfil_local_votacao` L22 — OK — anos 2010–2024

- **Prod:** limpo em todos os anos (2010+).

## 3. Mudanças necessárias

Nenhuma mudança de mapeamento necessária. Manter como referência de controle positivo (re-rodar o harness após cada refresh do TSE).

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** limpo — controle positivo.
