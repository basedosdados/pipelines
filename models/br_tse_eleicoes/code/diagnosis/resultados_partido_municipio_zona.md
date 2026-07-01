# `resultados_partido_municipio_zona` — diagnóstico

**Builder:** `sub/results_mun_zone.py::_build_partido`
**Família raw:** `votacao_partido_munzona` · **Granularidade:** partido × município × zona
**A jusante:** `resultados_partido_municipio` (agregação phase-3)
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/results_mun_zone.py:245` `_build_partido` · anos 1996, 2016 · layout: header

Posições deslocadas (exemplo: 1996):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v27` | `votos_nominais` | `SQ_COLIGACAO` |
| `v28` | `votos_legenda` | `NM_COLIGACAO` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `_build_partido` L229 — OK — anos 1994, 1998
- ✗ `_build_partido` L245 — POSITIONAL_MAPPING_MISMATCH — anos 1996, 2016
- ✓ `_build_partido` L245 — OK — anos 2000–2014
- ✓ `_build_partido` L261 — OK — anos 2018–2020
- ✓ `_build_partido` L277 — OK — anos 2022–2024

- **Prod — latente (Q7):** 1996/2016 limpos em prod. No rebuild, `votos_nominais ← SQ_COLIGACAO` (absurdo) e `votos_legenda ← NM_COLIGACAO` (NULL).

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Bloco L245 desalinhado em 1996/2016 (`votos_nominais ← SQ_COLIGACAO`, `votos_legenda ← NM_COLIGACAO`).

Layout oficial de referência (`votacao_partido_munzona` 1996, fonte: header de arquivo, `diagnostics/artifacts/layouts/votacao_partido_munzona_1996.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:NR_TURNO  7:CD_ELEICAO  8:DS_ELEICAO  9:DT_ELEICAO  10:TP_ABRANGENCIA  11:SG_UF  12:SG_UE  13:NM_UE  14:CD_MUNICIPIO  15:NM_MUNICIPIO  16:NR_ZONA  17:CD_CARGO  18:DS_CARGO  19:TP_AGREMIACAO  20:NR_PARTIDO  21:SG_PARTIDO  22:NM_PARTIDO  23:NR_FEDERACAO  24:NM_FEDERACAO  25:SG_FEDERACAO  26:DS_COMPOSICAO_FEDERACAO  27:SQ_COLIGACAO  28:NM_COLIGACAO  29:DS_COMPOSICAO_COLIGACAO  30:ST_VOTO_EM_TRANSITO  31:QT_VOTOS_LEGENDA_VALIDOS  32:QT_VOTOS_NOM_CONVR_LEG_VALIDOS  33:QT_TOTAL_VOTOS_LEG_VALIDOS  34:QT_VOTOS_NOMINAIS_VALIDOS  35:QT_VOTOS_LEGENDA_ANUL_SUBJUD  36:QT_VOTOS_NOMINAIS_ANUL_SUBJUD  37:QT_VOTOS_LEGENDA_ANULADOS  38:QT_VOTOS_NOMINAIS_ANULADOS
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
