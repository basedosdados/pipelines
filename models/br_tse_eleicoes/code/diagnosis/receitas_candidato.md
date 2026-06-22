# `receitas_candidato` — diagnóstico

**Builder:** `sub/campaign_finance.py::_build_receitas_2002`, `sub/campaign_finance.py::_build_receitas_2004`, `sub/campaign_finance.py::_build_receitas_2006`, `sub/campaign_finance.py::_build_receitas_2008`, `sub/campaign_finance.py::_build_receitas_2010`, `sub/campaign_finance.py::_build_receitas_2012`, `sub/campaign_finance.py::_build_receitas_2014`, `sub/campaign_finance.py::_build_receitas_2016`, `sub/campaign_finance.py::_build_receitas_2018_plus`
**Família raw:** `prestacao_receitas` · **Granularidade:** receita × candidato
**A jusante:** `receitas_comite`, `receitas_orgao_partidario`
**Status do harness:** WARN

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**⚠ WARN — UNVERIFIED_VARIANT** · `sub/campaign_finance.py:680` `_build_receitas_2014` · anos 2014 · layout: n/a

> 2014 supplementary file (receitas_..._sup) has its own layout, different from the per-UF receitas files

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `_build_receitas_2002` L157 — OK — anos 2002
- ✓ `_build_receitas_2004` L218 — OK — anos 2004
- ✓ `_build_receitas_2006` L276 — OK — anos 2006
- ✓ `_build_receitas_2008` L342 — OK — anos 2008
- ✓ `_build_receitas_2010` L426 — OK — anos 2010
- ✓ `_build_receitas_2012` L504 — OK — anos 2012
- ✓ `_build_receitas_2014` L603 — OK — anos 2014
- ⚠ `_build_receitas_2014` L680 — UNVERIFIED_VARIANT — anos 2014
- ✓ `_build_receitas_2016` L801 — OK — anos 2016
- ✓ `_build_receitas_2018_plus` L1031 — OK — anos 2018–2024

- **Prod:** 2018+ limpos; 2002–2016 validados após conferir os nomes idiossincráticos do header do TSE (whitelist em `affinity.py`). Nenhuma anomalia em prod.
- O único alerta é o arquivo suplementar de 2014 (variante não verificável — layout próprio, distinto dos arquivos por UF).

## 3. Mudanças necessárias

Mapeamento principal de 2014 valida OK. Apenas o arquivo suplementar (`receitas_..._sup`) tem layout próprio não verificável pelo harness — confirmar manualmente contra o leiame do suplementar.

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** alerta de baixa confiança / variante não verificável; revisar manualmente.
