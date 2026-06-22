# `despesas_candidato` — diagnóstico

**Builder:** `sub/campaign_finance.py::_build_despesas_2002`, `sub/campaign_finance.py::_build_despesas_2004`, `sub/campaign_finance.py::_build_despesas_2006`, `sub/campaign_finance.py::_build_despesas_2008`, `sub/campaign_finance.py::_build_despesas_2010`, `sub/campaign_finance.py::_build_despesas_2012`, `sub/campaign_finance.py::_build_despesas_2014`, `sub/campaign_finance.py::_build_despesas_2016`, `sub/campaign_finance.py::_build_despesas_2018_plus`
**Família raw:** `prestacao_despesas` · **Granularidade:** despesa × candidato
**A jusante:** folha
**Status do harness:** FAIL

> Diagnóstico extraído de `diagnostics/artifacts/findings.json` e dos layouts oficiais em `diagnostics/artifacts/layouts/`. Narrativa completa e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

## 1. Diagnóstico — problemas encontrados

**✗ FAIL — POSITIONAL_MAPPING_MISMATCH** · `sub/campaign_finance.py:1635` `_build_despesas_2014` · anos 2014 · layout: header

Posições deslocadas (exemplo: 2014):

| vN | código mapeia para | variável TSE real nessa posição |
|---|---|---|
| `v7` | `sigla_partido` | `Sigla da UE` |
| `v8` | `numero_candidato` | `Nome da UE` |
| `v9` | `cargo` | `Sigla  Partido` |
| `v12` | `tipo_documento` | `Nome candidato` |
| `v13` | `numero_documento` | `CPF do candidato` |
| `v14` | `cpf_cnpj_fornecedor` | `CPF do vice/suplente` |
| `v15` | `nome_fornecedor` | `Tipo de documento` |
| `v16` | `nome_fornecedor_rf` | `Número do documento` |
| `v17` | `cnae_2_fornecedor` | `CPF/CNPJ do fornecedor` |
| `v18` | `descricao_cnae_2_fornecedor` | `Nome do fornecedor` |
| `v20` | `valor_despesa` | `Cod setor econômico do fornecedor` |
| `v21` | `tipo_despesa` | `Setor econômico do fornecedor` |

## 2. Validação realizada

**Estática (tier 1/3):**
- ✓ `_build_despesas_2002` L1179 — OK — anos 2002
- ✓ `_build_despesas_2004` L1236 — OK — anos 2004
- ✓ `_build_despesas_2006` L1298 — OK — anos 2006
- ✓ `_build_despesas_2008` L1359 — OK — anos 2008
- ✓ `_build_despesas_2010` L1457 — OK — anos 2010
- ✓ `_build_despesas_2012` L1525 — OK — anos 2012
- ✗ `_build_despesas_2014` L1635 — POSITIONAL_MAPPING_MISMATCH — anos 2014
- ✓ `_build_despesas_2016` L1718 — OK — anos 2016
- ✓ `_build_despesas_2018_plus` L1859 — OK — anos 2018–2024

- **Prod — latente (Q8):** 2014 limpo em prod (build anterior). No rebuild, `cargo ← "Sigla  Partido"` e `tipo_documento ← "Nome candidato"` deslocam.

## 3. Mudanças necessárias

**Correção recomendada:** substituir os índices posicionais hardcoded (`vN → coluna`) por leitura por nome de header. Todo arquivo republicado carrega header (verificado até 1994); use o mapa nome→coluna do layout oficial abaixo. Blocos posicionais só permanecem necessários para os arquivos `*_uf` históricos (headerless).

Bloco único de 2014 (`_build_despesas_2014`) desalinhado a partir de `v7`: o header republicado tem `Sigla da UE`/`Nome da UE` antes de `Sigla Partido`.

Layout oficial de referência (`prestacao_despesas` 2014, fonte: header de arquivo, `diagnostics/artifacts/layouts/prestacao_despesas_2014.json`):

```
1:Cód. Eleição  2:Desc. Eleição  3:Data e hora  4:CNPJ Prestador Conta  5:Sequencial Candidato  6:UF  7:Sigla da UE  8:Nome da UE  9:Sigla  Partido  10:Número candidato  11:Cargo  12:Nome candidato  13:CPF do candidato  14:CPF do vice/suplente  15:Tipo de documento  16:Número do documento  17:CPF/CNPJ do fornecedor  18:Nome do fornecedor  19:Nome do fornecedor (Receita Federal)  20:Cod setor econômico do fornecedor  21:Setor econômico do fornecedor  22:Data da despesa  23:Valor despesa  24:Tipo despesa  25:Descriçao da despesa
```

## 4. Status da validação

- [ ] correção de código aplicada (read-by-header)
- [ ] harness re-rodado limpo (`python -m diagnostics run --tier 3`, exit 0)
- [ ] prod reconstruído nas células confirmadas (ordem topológica)
- **Estado atual:** prod corrompido (células confirmadas) + latente; reparo pendente.
