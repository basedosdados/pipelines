# `resultados_candidato_municipio_zona` — diagnóstico

**Builder:** `sub/results_mun_zone.py::_build_candidato`
**Família raw:** `votacao_candidato_munzona` · **Granularidade:** candidato × município × zona
**A jusante:** `resultados_candidato_municipio`, `resultados_candidato` (agregações phase-3)
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/results_mun_zone.py:58` `_build_candidato` · anos 1994 · layout: header

Posições deslocadas (exemplo: 1994):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v29` | `numero_partido` | `DS_SITUACAO_JULGAMENTO` |
| `v30` | `sigla_partido` | `CD_SITUACAO_CASSACAO` |
| `v40` | `votos` | `SG_FEDERACAO` |
| `v44` | `resultado` | `DS_COMPOSICAO_COLIGACAO` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/results_mun_zone.py:78` `_build_candidato` · anos 1996, 2016 · layout: header

Posições deslocadas (exemplo: 1996):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v29` | `numero_partido` | `DS_SITUACAO_JULGAMENTO` |
| `v30` | `sigla_partido` | `CD_SITUACAO_CASSACAO` |
| `v36` | `resultado` | `SG_PARTIDO` |
| `v38` | `votos` | `NR_FEDERACAO` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/results_mun_zone.py:98` `_build_candidato` · anos 2018–2022 · layout: header

Posições deslocadas (exemplo: 2018):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v29` | `numero_partido` | `DS_SITUACAO_JULGAMENTO` |
| `v30` | `sigla_partido` | `CD_SITUACAO_CASSACAO` |
| `v42` | `votos` | `SQ_COLIGACAO` |
| `v44` | `resultado` | `DS_COMPOSICAO_COLIGACAO` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✗ `_build_candidato` L58 — POSITIONAL_MAPPING_MISMATCH — anos 1994
- ✗ `_build_candidato` L78 — POSITIONAL_MAPPING_MISMATCH — anos 1996, 2016
- ✓ `_build_candidato` L78 — OK — anos 2002–2014
- ✓ `_build_candidato` L98 — OK — anos 1998–2000
- ✗ `_build_candidato` L98 — POSITIONAL_MAPPING_MISMATCH — anos 2018–2022
- ✓ `_build_candidato` L118 — OK — anos 2024

- **Prod — smoking gun (Q6, Q12):** 1994 tem `votos = 0` em todas as 842.223 linhas (`resultado`/`sigla_partido` sãos) — o shift previsto `v40: votos ← SG_FEDERACAO` (branco pré-federação ⇒ parse vazio ⇒ 0).
- **Prod — latente:** 1996/2016–2022 limpos em prod; corrompem no rebuild (`votos ← SQ_COLIGACAO` ⇒ magnitudes absurdas ou NULL).
- **Propagação (Q15, Q17):** 1994 votos=0 propaga para `resultados_candidato_municipio` e `resultados_candidato`; 1996 herda `titulo` NULL de `candidatos` 1996.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Três blocos FAIL (L58 1994; L78 1996/2016; L98 2018–2022). Em todos, `votos`/`resultado` caem em colunas de federação/coligação inseridas pelo TSE.

Layout oficial de referência (`votacao_candidato_munzona` 1994, fonte: header de arquivo, `diagnostics/artifacts/layouts/votacao_candidato_munzona_1994.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:NR_TURNO  7:CD_ELEICAO  8:DS_ELEICAO  9:DT_ELEICAO  10:TP_ABRANGENCIA  11:SG_UF  12:SG_UE  13:NM_UE  14:CD_MUNICIPIO  15:NM_MUNICIPIO  16:NR_ZONA  17:CD_CARGO  18:DS_CARGO  19:SQ_CANDIDATO  20:NR_CANDIDATO  21:NM_CANDIDATO  22:NM_URNA_CANDIDATO  23:NM_SOCIAL_CANDIDATO  24:CD_SITUACAO_CANDIDATURA  25:DS_SITUACAO_CANDIDATURA  26:CD_DETALHE_SITUACAO_CAND  27:DS_DETALHE_SITUACAO_CAND  28:CD_SITUACAO_JULGAMENTO  29:DS_SITUACAO_JULGAMENTO  30:CD_SITUACAO_CASSACAO  31:DS_SITUACAO_CASSACAO  32:CD_SITUACAO_DCONST_DIPLOMA  33:DS_SITUACAO_DCONST_DIPLOMA  34:TP_AGREMIACAO  35:NR_PARTIDO  36:SG_PARTIDO  37:NM_PARTIDO  38:NR_FEDERACAO  39:NM_FEDERACAO  40:SG_FEDERACAO  41:DS_COMPOSICAO_FEDERACAO  42:SQ_COLIGACAO  43:NM_COLIGACAO  44:DS_COMPOSICAO_COLIGACAO  45:ST_VOTO_EM_TRANSITO  46:QT_VOTOS_NOMINAIS  47:NM_TIPO_DESTINACAO_VOTOS  48:QT_VOTOS_NOMINAIS_VALIDOS  49:CD_SIT_TOT_TURNO  50:DS_SIT_TOT_TURNO
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
