# `bens_candidato` — diagnóstico

**Builder:** `sub/campaign_finance.py::build_bens`
**Família raw:** `bem_candidato` · **Granularidade:** bem × candidato
**A jusante:** folha (consome `titulo` de `norm_candidatos`)
**Status do harness:** OK (limpo)

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

Nenhuma incompatibilidade. Os índices posicionais batem com o layout oficial em todos os anos verificados.

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_bens` L100 — OK — anos 2006–2024

- **Prod — smoking gun (Q9, Q13):** `bens_candidato` 2014 carrega um único dígito (3–8) em `titulo_eleitoral_candidato` nas 83.053 linhas; os demais anos têm ~100% de títulos válidos de 10–13 dígitos. **A causa não é o mapeamento desta tabela** (que é correto) — é o merge com uma safra corrompida de `candidatos` 2014 (`titulo ← CD_GRAU_INSTRUCAO`, códigos 1–8) via `norm_candidatos`.
- **Controle positivo:** `descricao_item` tem 0% de siglas de UF em todos os anos — o fix do PR #1564 está em prod.
- **Propagação (Q16):** só o build de `bens_candidato` 2014 consumiu a safra corrompida; receitas/despesas/rcmz/rcs 2014 rodaram contra outra safra de `candidatos` e estão limpos.

## 3. Mudanças necessárias

Nenhuma mudança de mapeamento necessária. Manter como referência de controle positivo (re-rodar o harness após cada refresh do TSE).

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** mapeamento limpo (controle positivo do PR #1564), **mas o dado 2014 em prod está corrompido** (títulos de 1 dígito herdados da safra `candidatos` 2014); requer rebuild após corrigir `candidatos`..
