# `resultados_candidato_municipio` — diagnóstico (agregação phase-3)

**Builder:** `aggregation.py` (sem mapeamento posicional de raw)
**Granularidade:** candidato × município · **A montante:** `resultados_candidato_municipio_zona` (agregação)
**Status do harness:** corrupção apenas por propagação (sem FAIL próprio)

> Tabela phase-3: não lê arquivos raw, agrega tabelas phase-1. O harness não verifica mapeamentos aqui; a corrupção é herdada por borda do grafo de dependência. Ver [`../DIAGNOSIS.md`](../DIAGNOSIS.md) (Dependency-chain propagation).

## 1. Diagnóstico — problemas encontrados

Sem mapeamento posicional próprio (agregação phase-3). Herda valores do upstream.

## 2. Validação realizada

**Propagação (Q15):** `rcmz` 1994 com `votos = 0` propaga para todas as 705.492 linhas de 1994 (controle 1998: 0 zeros, máx 3,29M).

## 3. Mudanças necessárias

Reconstruir *depois* de corrigir e reconstruir `resultados_candidato_municipio_zona` 1994. Nenhuma mudança de código nesta tabela.

## 4. Status da validação

- [ ] upstream corrigido e reconstruído
- [ ] esta agregação reconstruída (ordem topológica)
- **Estado atual:** 1994 corrompido (votos=0) via agregação; reparo pendente upstream-first.
