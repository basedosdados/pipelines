# `detalhes_votacao_uf` — diagnóstico

**Builder:** `sub/voting_details_state.py::build_voting_details_state`
**Família raw:** `detalhe_votacao_uf` · **Granularidade:** UF (histórico 1945–1990)
**A jusante:** intermediário histórico (não publicado como modelo DBT)
**Status do harness:** WARN

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**⚠ WARN — POSITIONAL_MAPPING_MISMATCH** · `sub/voting_details_state.py:81` `build_voting_details_state` · anos 1945–1947, 1950, 1954–1955, 1958–1962, 1965–1966, 1970, 1974, 1978, 1982, 1986, 1989–1990 · layout: leiame

Posições deslocadas (exemplo: 1945):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v13` | `votos_validos` | `QTD_VOTOS_NOMINAIS` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ⚠ `build_voting_details_state` L81 — POSITIONAL_MAPPING_MISMATCH — anos 1945–1947, 1950, 1954–1955, 1958–1962, 1965–1966, 1970, 1974, 1978, 1982, 1986, 1989–1990

- **Prod:** não publicado como modelo DBT (intermediário histórico). Os layouts vêm de leiame PDF (baixa confiança) → WARN, não FAIL.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Arquivos `*_uf` históricos são **headerless** — devem permanecer posicionais. Layout vem de leiame PDF (WARN, baixa confiança); revisar manualmente contra o leiame se houver rebuild.

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** alerta de baixa confiança / variante não verificável; revisar manualmente.
