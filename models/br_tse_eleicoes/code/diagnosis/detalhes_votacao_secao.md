# `detalhes_votacao_secao` — diagnóstico

**Builder:** `sub/voting_details_section.py::build_detalhes_secao`
**Família raw:** `detalhe_votacao_secao` · **Granularidade:** seção
**A jusante:** folha
**Status do harness:** OK (limpo)

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**· NO_LAYOUT — NO_LAYOUT** · `sub/voting_details_section.py:45` `build_detalhes_secao` · anos 1994–1996 · layout: n/a


## 2. Validação realizada

**Estática (tier 1/3):**
- · `build_detalhes_secao` L45 — NO_LAYOUT — anos 1994–1996
- ✓ `build_detalhes_secao` L45 — OK — anos 1998–2024

- **Prod:** limpo (1998+ com layout header verificado). 1994/1996 sem layout oficial adquirido (não há FAIL — apenas ausência de referência).

## 3. Mudanças necessárias

Nenhuma mudança de mapeamento necessária. Manter como referência de controle positivo (re-rodar o harness após cada refresh do TSE).

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** limpo — controle positivo.
