# `detalhes_votacao_municipio_zona` — diagnóstico

**Builder:** `sub/voting_details_mun_zone.py::build_detalhes_mun_zona`
**Família raw:** `detalhe_votacao_munzona` · **Granularidade:** município × zona
**A jusante:** `detalhes_votacao_municipio` (agregação phase-3)
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/voting_details_mun_zone.py:108` `build_detalhes_mun_zona` · anos 1996, 2000–2016 · layout: header

Posições deslocadas (exemplo: 1996):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v22` | `aptos_totalizadas` | `QT_SECOES_NAO_INSTALADAS` |
| `v23` | `secoes_totalizadas` | `QT_TOTAL_SECOES` |
| `v25` | `abstencoes` | `QT_ELEITORES_SECOES_NAO_INSTALADAS` |
| `v27` | `votos_nominais` | `ST_VOTO_EM_TRANSITO` |
| `v28` | `votos_brancos` | `QT_VOTOS` |
| `v29` | `votos_nulos` | `QT_VOTOS_CONCORRENTES` |
| `v30` | `votos_legenda` | `QT_TOTAL_VOTOS_VALIDOS` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_detalhes_mun_zona` L47 — OK — anos 1994, 1998
- ✗ `build_detalhes_mun_zona` L108 — POSITIONAL_MAPPING_MISMATCH — anos 1996, 2000–2016
- ✓ `build_detalhes_mun_zona` L159 — OK — anos 2018–2024

- **Prod — confirmado (Q4, Q11):** 1996 tem apenas 548 linhas em 19 UFs (anos municipais comparáveis: ~12k) e `aptos_totalizadas = -3` (sentinela `#NULO#` do TSE) em 100% das linhas.
- **Prod — latente:** 2000–2016 limpos em prod; corrompem no rebuild (`votos_nominais ← ST_VOTO_EM_TRANSITO`, `votos_brancos ← QT_VOTOS`).
- **Propagação (Q18):** o buraco de cobertura de 1996 chega a `detalhes_votacao_municipio` (242 municípios / 19 UFs vs ~11.200 / 26 UFs em 2000/2004).

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Bloco de 1996/2000–2016 (L108) desalinhado: o TSE inseriu `QT_SECOES_NAO_INSTALADAS`, `QT_TOTAL_SECOES`, etc. Os blocos 1994/1998 (L47) e ≥2018 (L159) validam OK.

Layout oficial de referência (`detalhe_votacao_munzona` 1996, fonte: header de arquivo, `diagnostics/artifacts/layouts/detalhe_votacao_munzona_1996.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:NR_TURNO  7:CD_ELEICAO  8:DS_ELEICAO  9:DT_ELEICAO  10:TP_ABRANGENCIA  11:SG_UF  12:SG_UE  13:NM_UE  14:CD_MUNICIPIO  15:NM_MUNICIPIO  16:NR_ZONA  17:CD_CARGO  18:DS_CARGO  19:QT_APTOS  20:QT_SECOES_PRINCIPAIS  21:QT_SECOES_AGREGADAS  22:QT_SECOES_NAO_INSTALADAS  23:QT_TOTAL_SECOES  24:QT_COMPARECIMENTO  25:QT_ELEITORES_SECOES_NAO_INSTALADAS  26:QT_ABSTENCOES  27:ST_VOTO_EM_TRANSITO  28:QT_VOTOS  29:QT_VOTOS_CONCORRENTES  30:QT_TOTAL_VOTOS_VALIDOS  31:QT_VOTOS_NOMINAIS_VALIDOS  32:QT_TOTAL_VOTOS_LEG_VALIDOS  33:QT_VOTOS_LEG_VALIDOS  34:QT_VOTOS_NOM_CONVR_LEG_VALIDOS  35:QT_TOTAL_VOTOS_ANULADOS  36:QT_VOTOS_NOMINAIS_ANULADOS  37:QT_VOTOS_LEGENDA_ANULADOS  38:QT_TOTAL_VOTOS_ANUL_SUBJUD  39:QT_VOTOS_NOMINAIS_ANUL_SUBJUD  40:QT_VOTOS_LEGENDA_ANUL_SUBJUD  41:QT_VOTOS_BRANCOS  42:QT_TOTAL_VOTOS_NULOS  43:QT_VOTOS_NULOS  44:QT_VOTOS_NULOS_TECNICOS  45:QT_VOTOS_ANULADOS_APU_SEP  46:HH_ULTIMA_TOTALIZACAO  47:DT_ULTIMA_TOTALIZACAO
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
