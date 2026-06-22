# `perfil_eleitorado_municipio_zona` — diagnóstico

**Builder:** `sub/voter_profile_mun_zone.py::build_perfil_mun_zona`
**Família raw:** `perfil_eleitorado` · **Granularidade:** município × zona
**A jusante:** folha
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/voter_profile_mun_zone.py:21` `build_perfil_mun_zona` · anos 2008–2016, 2020–2022 · layout: header

Posições deslocadas (exemplo: 2008):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v7` | `situacao_biometria` | `NR_ZONA` |
| `v9` | `zona` | `DS_GENERO` |
| `v10` | `genero` | `CD_ESTADO_CIVIL` |
| `v12` | `estado_civil` | `CD_FAIXA_ETARIA` |
| `v14` | `grupo_idade` | `CD_GRAU_ESCOLARIDADE` |
| `v16` | `instrucao` | `CD_RACA_COR` |
| `v18` | `eleitores` | `CD_IDENTIDADE_GENERO` |
| `v19` | `eleitores_biometria` | `DS_IDENTIDADE_GENERO` |
| `v20` | `eleitores_deficiencia` | `CD_QUILOMBOLA` |
| `v21` | `eleitores_inclusao_nome_social` | `DS_QUILOMBOLA` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/voter_profile_mun_zone.py:54` `build_perfil_mun_zona` · anos 2024 · layout: header

Posições deslocadas (exemplo: 2024):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v24` | `eleitores` | `TP_OBRIGATORIEDADE_VOTO` |
| `v25` | `eleitores_biometria` | `QT_ELEITORES_PERFIL` |
| `v26` | `eleitores_deficiencia` | `QT_ELEITORES_BIOMETRIA` |
| `v27` | `eleitores_inclusao_nome_social` | `QT_ELEITORES_DEFICIENCIA` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_perfil_mun_zona` L21 — OK — anos 1994–2006, 2018
- ✗ `build_perfil_mun_zona` L21 — POSITIONAL_MAPPING_MISMATCH — anos 2008–2016, 2020–2022
- ✗ `build_perfil_mun_zona` L54 — POSITIONAL_MAPPING_MISMATCH — anos 2024

- **Prod — confirmado (Q5, Q14):** 2008/2016 misturam domínios de valor numa mesma partição: ~3.6M/4.1M linhas com códigos crus do TSE (`genero` ∈ 0/2/4, ~35 eleitores/linha) mais um resíduo com rótulos de texto (`masculino`/`feminino`, ~200 eleitores/linha) — duas gerações de build coexistindo no mesmo `ano`.
- **Prod — latente:** 2010/2012/2014 e 2020–2024 limpos em prod; corrompem no rebuild.
- **2024:** o TSE inseriu `TP_OBRIGATORIEDADE_VOTO` *depois* de o refactor ser escrito — o bloco de 2024 está desalinhado por uma coluna.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Bloco ≤2022 (L21) desalinhado a partir de 2008 (inserção de `DS_GENERO`, `CD_IDENTIDADE_GENERO`, `CD_QUILOMBOLA`, …). Bloco 2024 (L54) desalinhado por `TP_OBRIGATORIEDADE_VOTO`.

Layout oficial de referência (`perfil_eleitorado` 2008, fonte: header de arquivo, `diagnostics/artifacts/layouts/perfil_eleitorado_2008.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:SG_UF  5:CD_MUNICIPIO  6:NM_MUNICIPIO  7:NR_ZONA  8:CD_GENERO  9:DS_GENERO  10:CD_ESTADO_CIVIL  11:DS_ESTADO_CIVIL  12:CD_FAIXA_ETARIA  13:DS_FAIXA_ETARIA  14:CD_GRAU_ESCOLARIDADE  15:DS_GRAU_ESCOLARIDADE  16:CD_RACA_COR  17:DS_RACA_COR  18:CD_IDENTIDADE_GENERO  19:DS_IDENTIDADE_GENERO  20:CD_QUILOMBOLA  21:DS_QUILOMBOLA  22:CD_INTERPRETE_LIBRAS  23:DS_INTERPRETE_LIBRAS  24:TP_OBRIGATORIEDADE_VOTO  25:QT_ELEITORES_PERFIL  26:QT_ELEITORES_BIOMETRIA  27:QT_ELEITORES_DEFICIENCIA  28:QT_ELEITORES_INC_NM_SOCIAL
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
