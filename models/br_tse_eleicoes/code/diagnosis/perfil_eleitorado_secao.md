# `perfil_eleitorado_secao` — diagnóstico

**Builder:** `sub/voter_profile_section.py::build_perfil_secao`
**Família raw:** `perfil_eleitor_secao` · **Granularidade:** seção
**A jusante:** folha
**Status do harness:** WARN

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**⚠ WARN — MISSING_KEY** · `sub/voter_profile_section.py:70` `build_perfil_secao` · anos 2008–2022 · layout: header

Chave selecionada pelo código mas ausente do layout oficial:

| chave (nome de header) | mapeia para |
|---|---|
| `cd_mun_sit_biometrica` | `situacao_biometria` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ⚠ `build_perfil_secao` L70 — MISSING_KEY — anos 2008–2022
- ✓ `build_perfil_secao` L87 — OK — anos 2024

- **Prod:** sem anomalia. O alerta é estrutural: a chave `cd_mun_sit_biometrica` (→ `situacao_biometria`) está ausente do layout oficial 2008–2022 (MISSING_KEY).

## 3. Mudanças necessárias

Não é shift posicional: o builder já lê por nome de header, mas seleciona uma chave (`cd_mun_sit_biometrica`) ausente do layout oficial 2008–2022. Conferir o nome correto da coluna de situação biométrica no leiame e ajustar a chave.

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- **Estado atual:** alerta de baixa confiança / variante não verificável; revisar manualmente.
