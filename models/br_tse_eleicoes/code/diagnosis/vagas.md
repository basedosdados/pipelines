# `vagas` — diagnóstico

**Builder:** `sub/vacancies.py::build_vagas`
**Família raw:** `consulta_vagas` · **Granularidade:** cargo × município/UF
**A jusante:** folha
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/vacancies.py:46` `build_vagas` · anos 1994–2012 · layout: header

Posições deslocadas (exemplo: 1994):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v5` | `sigla_uf` | `NM_TIPO_ELEICAO` |
| `v6` | `id_municipio_tse` | `CD_ELEICAO` |
| `v9` | `cargo` | `DT_POSSE` |
| `v10` | `vagas` | `SG_UF` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✗ `build_vagas` L46 — POSITIONAL_MAPPING_MISMATCH — anos 1994–2012
- ✓ `build_vagas` L58 — OK — anos 2014–2024

- **Prod — latente (Q3):** 1994–2012 limpos em prod. No rebuild, `vagas ← SG_UF` vira NULL após cast para int e `cargo ← DT_POSSE` recebe strings de data.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Dois blocos (≤2012 / ≥2014). Os índices ≤2012 assumem o layout antigo sem as colunas `CD_ELEICAO`/`DT_POSSE` inseridas na republicação atual.

Layout oficial de referência (`consulta_vagas` 1994, fonte: header de arquivo, `diagnostics/artifacts/layouts/consulta_vagas_1994.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:CD_ELEICAO  7:DS_ELEICAO  8:DT_ELEICAO  9:DT_POSSE  10:SG_UF  11:SG_UE  12:NM_UE  13:CD_CARGO  14:DS_CARGO  15:QT_VAGA
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
