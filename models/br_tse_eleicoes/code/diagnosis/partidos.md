# `partidos` — diagnóstico

**Builder:** `sub/parties.py::build_partidos`
**Família raw:** `consulta_coligacao` · **Granularidade:** partido × município/UF
**A jusante:** folha
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/parties.py:53` `build_partidos` · anos 1994, 1998, 2002, 2006, 2010 · layout: header

Posições deslocadas (exemplo: 1994):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v4` | `turno` | `CD_TIPO_ELEICAO` |
| `v7` | `sigla_uf` | `CD_ELEICAO` |
| `v10` | `cargo` | `SG_UF` |
| `v11` | `tipo_agremiacao` | `SG_UE` |
| `v12` | `numero` | `NM_UE` |
| `v13` | `sigla` | `CD_CARGO` |
| `v14` | `nome` | `DS_CARGO` |
| `v16` | `nome_coligacao` | `NR_PARTIDO` |
| `v17` | `composicao_coligacao` | `SG_PARTIDO` |
| `v18` | `sequencial_coligacao` | `NM_PARTIDO` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/parties.py:84` `build_partidos` · anos 1996, 2000, 2004, 2008, 2012 · layout: header

Posições deslocadas (exemplo: 1996):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v4` | `turno` | `CD_TIPO_ELEICAO` |
| `v6` | `sigla_uf` | `NR_TURNO` |
| `v7` | `id_municipio_tse` | `CD_ELEICAO` |
| `v10` | `cargo` | `SG_UF` |
| `v11` | `tipo_agremiacao` | `SG_UE` |
| `v12` | `numero` | `NM_UE` |
| `v13` | `sigla` | `CD_CARGO` |
| `v14` | `nome` | `DS_CARGO` |
| `v16` | `nome_coligacao` | `NR_PARTIDO` |
| `v17` | `composicao_coligacao` | `SG_PARTIDO` |
| `v18` | `sequencial_coligacao` | `NM_PARTIDO` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/parties.py:122` `build_partidos` · anos 2014, 2018 · layout: header

Posições deslocadas (exemplo: 2014):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v19` | `sequencial_coligacao` | `NR_FEDERACAO` |
| `v20` | `nome_coligacao` | `NM_FEDERACAO` |
| `v21` | `composicao_coligacao` | `SG_FEDERACAO` |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/parties.py:157` `build_partidos` · anos 2020 · layout: header

Posições deslocadas (exemplo: 2020):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v19` | `sequencial_coligacao` | `NR_FEDERACAO` |
| `v20` | `nome_coligacao` | `NM_FEDERACAO` |
| `v21` | `composicao_coligacao` | `SG_FEDERACAO` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `build_partidos` L53 — OK — anos 1990
- ✗ `build_partidos` L53 — POSITIONAL_MAPPING_MISMATCH — anos 1994, 1998, 2002, 2006, 2010
- ✗ `build_partidos` L84 — POSITIONAL_MAPPING_MISMATCH — anos 1996, 2000, 2004, 2008, 2012
- ✗ `build_partidos` L122 — POSITIONAL_MAPPING_MISMATCH — anos 2014, 2018
- ✓ `build_partidos` L157 — OK — anos 2016
- ✗ `build_partidos` L157 — POSITIONAL_MAPPING_MISMATCH — anos 2020
- ✓ `build_partidos` L197 — OK — anos 2022
- ✓ `build_partidos` L242 — OK — anos 2024

- **Prod — latente (Q2):** todos os anos limpos em prod; o último build precede a republicação. Qualquer rebuild a partir de download novo desloca `cargo ← SG_UF` (siglas de UF) e `nome_coligacao ← NR_PARTIDO` (numérico).

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Seis blocos (federal/municipal × geração). O bloco de federação de 2014–2020 (`NR/NM/SG_FEDERACAO`) não existia quando os índices foram fixados.

Layout oficial de referência (`consulta_coligacao` 1994, fonte: header de arquivo, `diagnostics/artifacts/layouts/consulta_coligacao_1994.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:NR_TURNO  7:CD_ELEICAO  8:DS_ELEICAO  9:DT_ELEICAO  10:SG_UF  11:SG_UE  12:NM_UE  13:CD_CARGO  14:DS_CARGO  15:TP_AGREMIACAO  16:NR_PARTIDO  17:SG_PARTIDO  18:NM_PARTIDO  19:SQ_COLIGACAO  20:NM_COLIGACAO  21:DS_COMPOSICAO_COLIGACAO  22:CD_SITUACAO_LEGENDA  23:DS_SITUACAO
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
