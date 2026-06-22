# `candidatos` — diagnóstico

**Builder:** `sub/candidates.py::_parse_schema`, `sub/candidates.py::build_candidatos`
**Família raw:** `consulta_cand`, `consulta_cand_complementar` · **Granularidade:** candidato (nacional)
**A jusante:** `norm_candidatos` (phase-2 linchpin) → `titulo_eleitoral_candidato` em `bens_candidato`, `receitas_candidato`, `despesas_candidato`, `resultados_candidato_secao`, `resultados_candidato_municipio_zona`, `resultados_candidato`
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — OUT_OF_RANGE** · `sub/candidates.py:30` `_parse_schema` · anos 1994–1996, 2014, 2018 · layout: header

Índices além do fim do layout oficial (coluna inexistente):

| vN | código mapeia para | tamanho do layout |
|---|---|---|
| `v51` | `ocupacao` | 50 |
| `v54` | `resultado` | 50 |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/candidates.py:30` `_parse_schema` · anos 1994–1996, 2014, 2018 · layout: header

Posições deslocadas (exemplo: 1994):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v26` | `situacao` | `NR_PARTIDO` |
| `v29` | `sigla_partido` | `NR_FEDERACAO` |
| `v35` | `nacionalidade` | `DS_COMPOSICAO_COLIGACAO` |
| `v38` | `municipio_nascimento` | `NR_TITULO_ELEITORAL_CANDIDATO` |
| `v39` | `data_nascimento` | `CD_GENERO` |
| `v41` | `titulo_eleitoral` | `CD_GRAU_INSTRUCAO` |
| `v43` | `genero` | `CD_ESTADO_CIVIL` |
| `v45` | `instrucao` | `CD_COR_RACA` |
| `v47` | `estado_civil` | `CD_OCUPACAO` |
| `v49` | `raca` | `CD_SIT_TOT_TURNO` |

**✗ FAIL — OUT_OF_RANGE** · `sub/candidates.py:61` `_parse_schema` · anos 2020–2022 · layout: header

Índices além do fim do layout oficial (coluna inexistente):

| vN | código mapeia para | tamanho do layout |
|---|---|---|
| `v51` | `estado_civil` | 50 |
| `v53` | `raca` | 50 |
| `v55` | `ocupacao` | 50 |
| `v58` | `resultado` | 50 |

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/candidates.py:61` `_parse_schema` · anos 2020–2022 · layout: header

Posições deslocadas (exemplo: 2020):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v26` | `situacao` | `NR_PARTIDO` |
| `v29` | `sigla_partido` | `NR_FEDERACAO` |
| `v39` | `nacionalidade` | `CD_GENERO` |
| `v40` | `sigla_uf_nascimento` | `DS_GENERO` |
| `v42` | `municipio_nascimento` | `DS_GRAU_INSTRUCAO` |
| `v43` | `data_nascimento` | `CD_ESTADO_CIVIL` |
| `v45` | `titulo_eleitoral` | `CD_COR_RACA` |
| `v47` | `genero` | `CD_OCUPACAO` |
| `v49` | `instrucao` | `CD_SIT_TOT_TURNO` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✗ `_parse_schema` L30 — OUT_OF_RANGE — anos 1994–1996, 2014, 2018
- ✗ `_parse_schema` L30 — POSITIONAL_MAPPING_MISMATCH — anos 1994–1996, 2014, 2018
- ✓ `_parse_schema` L30 — OK — anos 1998–2012
- ✓ `_parse_schema` L61 — OK — anos 2016
- ✗ `_parse_schema` L61 — OUT_OF_RANGE — anos 2020–2022
- ✗ `_parse_schema` L61 — POSITIONAL_MAPPING_MISMATCH — anos 2020–2022
- ✓ `_parse_schema` L92 — OK — anos 2024
- ✓ `build_candidatos` L148 — OK — anos 2024

- **Local (fresh download):** `candidatos_1994.parquet` reconstruído de `/tmp/dados_TSE/input/consulta_cand/` carrega códigos de gênero em `titulo_eleitoral`, estado civil em `genero` e números de partido em `situacao` — exatamente a classe de falha do PR #1564, agora na tabela-linchpin.
- **Prod — confirmado (Q1, Q10):** `candidatos` 1996 tem o bloco demográfico inteiro (`titulo_eleitoral`, `genero`, `estado_civil`, `instrucao`, `nacionalidade`, `municipio_nascimento`) NULL em todas as 10.356 linhas — consistente com o drift de layout de 1996 (não poderia ter vindo do arquivo republicado atual).
- **Prod — latente:** 1994/2014/2018/2020/2022 estão limpos em prod (build anterior à republicação do TSE); corrompem no próximo rebuild a partir de download novo.
- **Propagação (Q17):** a corrupção de 1996 chega a `titulo_eleitoral_candidato` em `rcmz`/`rcs`/`rc` 1996 (100% NULL) pela borda do merge `norm_candidatos`.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Três blocos year-conditional (≤2014/2018; 2016/2020–2022; 2024). O TSE inseriu o bloco de federação (`NR_FEDERACAO`, `SG_FEDERACAO`, …) e reordenou o bloco demográfico entre gerações — daí os shifts em 1994/1996/2014/2018/2020/2022. **Prioridade máxima de reparo (topológico):** corrigir aqui *antes* de `norm_candidatos` e de qualquer tabela phase-2/3 a jusante.

Layout oficial de referência (`consulta_cand` 1994, fonte: header de arquivo, `diagnostics/artifacts/layouts/consulta_cand_1994.json`):

```
1:DT_GERACAO  2:HH_GERACAO  3:ANO_ELEICAO  4:CD_TIPO_ELEICAO  5:NM_TIPO_ELEICAO  6:NR_TURNO  7:CD_ELEICAO  8:DS_ELEICAO  9:DT_ELEICAO  10:TP_ABRANGENCIA  11:SG_UF  12:SG_UE  13:NM_UE  14:CD_CARGO  15:DS_CARGO  16:SQ_CANDIDATO  17:NR_CANDIDATO  18:NM_CANDIDATO  19:NM_URNA_CANDIDATO  20:NM_SOCIAL_CANDIDATO  21:NR_CPF_CANDIDATO  22:DS_EMAIL  23:CD_SITUACAO_CANDIDATURA  24:DS_SITUACAO_CANDIDATURA  25:TP_AGREMIACAO  26:NR_PARTIDO  27:SG_PARTIDO  28:NM_PARTIDO  29:NR_FEDERACAO  30:NM_FEDERACAO  31:SG_FEDERACAO  32:DS_COMPOSICAO_FEDERACAO  33:SQ_COLIGACAO  34:NM_COLIGACAO  35:DS_COMPOSICAO_COLIGACAO  36:SG_UF_NASCIMENTO  37:DT_NASCIMENTO  38:NR_TITULO_ELEITORAL_CANDIDATO  39:CD_GENERO  40:DS_GENERO  41:CD_GRAU_INSTRUCAO  42:DS_GRAU_INSTRUCAO  43:CD_ESTADO_CIVIL  44:DS_ESTADO_CIVIL  45:CD_COR_RACA  46:DS_COR_RACA  47:CD_OCUPACAO  48:DS_OCUPACAO  49:CD_SIT_TOT_TURNO  50:DS_SIT_TOT_TURNO
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
