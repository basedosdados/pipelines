# `resultados_candidato` — diagnóstico (agregação phase-3)

**Builder:** `aggregation.py` (sem mapeamento posicional de raw)
**Granularidade:** candidato (rollup nacional) · **A montante:** `resultados_candidato_municipio_zona`, `resultados_candidato_uf`, `norm_candidatos` (titulo)
**Status do harness:** corrupção apenas por propagação (sem FAIL próprio)

> Tabela phase-3: não lê arquivos raw, agrega tabelas phase-1. O harness não verifica mapeamentos aqui; a corrupção é herdada por borda do grafo de dependência. Ver [`../DIAGNOSIS.md`](../DIAGNOSIS.md) (Dependency-chain propagation).

## 1. Diagnóstico — problemas encontrados

Sem mapeamento posicional próprio (rollup all-years). Herda valores do upstream.

## 2. Validação realizada

**Propagação (Q16/Q17):** 1994 com `votos = 0` em todas as 6.719 linhas (de `rcmz` 1994); `titulo_eleitoral_candidato` 1996 100% NULL em 9.983 linhas (de `candidatos` 1996).

## 3. Mudanças necessárias

Reconstruir após `candidatos` 1996 → `norm_candidatos` e após `rcmz`/`rcuf` 1994.

## 4. Status da validação

- [ ] upstream corrigido e reconstruído
- [ ] esta agregação reconstruída (ordem topológica)
- **Estado atual:** 1994 (votos=0) e 1996 (titulo NULL) corrompidos via agregação/merge; reparo pendente upstream-first.
