# `detalhes_votacao_municipio` — diagnóstico (agregação phase-3)

**Builder:** `aggregation.py` (sem mapeamento posicional de raw)
**Granularidade:** município · **A montante:** `detalhes_votacao_municipio_zona` (agregação)
**Status do harness:** corrupção apenas por propagação (sem FAIL próprio)

> Tabela phase-3: não lê arquivos raw, agrega tabelas phase-1. O harness não verifica mapeamentos aqui; a corrupção é herdada por borda do grafo de dependência. Ver [`../DIAGNOSIS.md`](../DIAGNOSIS.md) (Dependency-chain propagation).

## 1. Diagnóstico — problemas encontrados

Sem mapeamento posicional próprio (agregação phase-3). Herda cobertura do upstream.

## 2. Validação realizada

**Propagação (Q18):** `dvmz` 1996 (colapso de cobertura: 548 linhas / 19 UFs) propaga para `dvm` 1996 — 242 municípios / 19 UFs vs ~11.200 / 26 UFs em 2000/2004. A sentinela −3 não aflora (`dvm` agrega `aptos`), mas o buraco de cobertura sim.

## 3. Mudanças necessárias

Reconstruir após corrigir `detalhes_votacao_municipio_zona` 1996.

## 4. Status da validação

- [ ] upstream corrigido e reconstruído
- [ ] esta agregação reconstruída (ordem topológica)
- **Estado atual:** 1996 com cobertura colapsada via agregação; reparo pendente upstream-first.
