# `resultados_candidato_secao` — diagnóstico

**Builder:** `sub/results_section.py::build_resultados_secao`
**Família raw:** `votacao_secao` · **Granularidade:** candidato × seção
**A jusante:** folha (consome `titulo` de `norm_candidatos`)
**Status do harness:** WARN

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**⚠ WARN — UNVERIFIED_VARIANT** · `sub/results_section.py:71` `build_resultados_secao` · anos 1998, 2008 · layout: n/a

> legacy variant for votacao_secao_1998_BR / votacao_secao_2008_TO only; those specific zips are absent from the current TSE portal

**· NO_LAYOUT — NO_LAYOUT** · `sub/results_section.py:100` `build_resultados_secao` · anos 1994–1996 · layout: n/a


## 2. Validação realizada

**Estática (tier 1/3):**
- ⚠ `build_resultados_secao` L71 — UNVERIFIED_VARIANT — anos 1998, 2008
- · `build_resultados_secao` L100 — NO_LAYOUT — anos 1994–1996
- ✓ `build_resultados_secao` L100 — OK — anos 1998–2024

- **Prod:** anos com layout header limpos. WARN em 1998/2008 é variante legada (`votacao_secao_1998_BR` / `votacao_secao_2008_TO`) cujos zips somem do portal atual; 1994/1996 sem layout oficial.
- **Propagação (Q17):** `titulo_eleitoral_candidato` 1996 é 100% NULL (4.075.079 linhas) herdado de `candidatos` 1996 pelo merge `norm_candidatos`.

## 3. Mudanças necessárias

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** mapeamento OK/variante; **`titulo` 1996 em prod 100% NULL** (herdado de `candidatos` 1996); reparo pendente upstream-first..
