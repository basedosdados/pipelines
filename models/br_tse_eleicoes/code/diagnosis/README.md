# `br_tse_eleicoes` — diagnóstico por tabela

Quebra por tabela do diagnóstico de schema (refactor Stata → Python). Cada arquivo reúne, para uma tabela: (1) problemas encontrados, (2) validação realizada (estática/local/prod), (3) mudanças necessárias e (4) status da validação.

Fonte de verdade: `../python/diagnostics/artifacts/findings.json` e os layouts oficiais em `../python/diagnostics/artifacts/layouts/`. Narrativa completa, evidência de prod (Q1–Q18) e metodologia em [`../DIAGNOSIS.md`](../DIAGNOSIS.md).

Legenda: ✗ FAIL (mismatch confirmado por header) · ⚠ WARN (layout de baixa confiança ou variante não verificável) · ✓ OK (limpo).

## Tabelas phase-1 (mapeamento de raw)

| Tabela | Status | Anos com problema | Doc |
|---|---|---|---|
| `bens_candidato` | ✓ OK | — | [bens_candidato.md](bens_candidato.md) |
| `candidatos` | ✗ FAIL | FAIL: 1994–1996, 2014, 2018–2022 | [candidatos.md](candidatos.md) |
| `despesas_candidato` | ✗ FAIL | FAIL: 2014 | [despesas_candidato.md](despesas_candidato.md) |
| `detalhes_votacao_municipio_zona` | ✗ FAIL | FAIL: 1996, 2000–2016 | [detalhes_votacao_municipio_zona.md](detalhes_votacao_municipio_zona.md) |
| `detalhes_votacao_secao` | ✓ OK | — | [detalhes_votacao_secao.md](detalhes_votacao_secao.md) |
| `detalhes_votacao_uf` | ⚠ WARN | WARN: 1945–1947, 1950, 1954–1955, 1958–1962, 1965–1966, 1970, 1974, 1978, 1982, 1986, 1989–1990 | [detalhes_votacao_uf.md](detalhes_votacao_uf.md) |
| `partidos` | ✗ FAIL | FAIL: 1994–2014, 2018–2020 | [partidos.md](partidos.md) |
| `perfil_eleitorado_local_votacao` | ✓ OK | — | [perfil_eleitorado_local_votacao.md](perfil_eleitorado_local_votacao.md) |
| `perfil_eleitorado_municipio_zona` | ✗ FAIL | FAIL: 2008–2016, 2020–2024 | [perfil_eleitorado_municipio_zona.md](perfil_eleitorado_municipio_zona.md) |
| `perfil_eleitorado_secao` | ⚠ WARN | WARN: 2008–2022 | [perfil_eleitorado_secao.md](perfil_eleitorado_secao.md) |
| `receitas_candidato` | ⚠ WARN | WARN: 2014 | [receitas_candidato.md](receitas_candidato.md) |
| `resultados_candidato_municipio_zona` | ✗ FAIL | FAIL: 1994–1996, 2016–2022 | [resultados_candidato_municipio_zona.md](resultados_candidato_municipio_zona.md) |
| `resultados_candidato_secao` | ⚠ WARN | WARN: 1998, 2008 | [resultados_candidato_secao.md](resultados_candidato_secao.md) |
| `resultados_candidato_uf` | ✓ OK | — | [resultados_candidato_uf.md](resultados_candidato_uf.md) |
| `resultados_partido_municipio_zona` | ✗ FAIL | FAIL: 1996, 2016 | [resultados_partido_municipio_zona.md](resultados_partido_municipio_zona.md) |
| `resultados_partido_uf` | ✓ OK | — | [resultados_partido_uf.md](resultados_partido_uf.md) |
| `vagas` | ✗ FAIL | FAIL: 1994–2012 | [vagas.md](vagas.md) |

## Tabelas phase-3 (agregação — corrupção por propagação)

| Tabela | A montante | Células afetadas | Doc |
|---|---|---|---|
| `resultados_candidato_municipio` | `rcmz` | 1994 (votos=0) | [resultados_candidato_municipio.md](resultados_candidato_municipio.md) |
| `resultados_candidato` | `rcmz`/`rcuf`/`candidatos` | 1994 (votos=0), 1996 (titulo NULL) | [resultados_candidato.md](resultados_candidato.md) |
| `detalhes_votacao_municipio` | `dvmz` | 1996 (cobertura) | [detalhes_votacao_municipio.md](detalhes_votacao_municipio.md) |

## Ordem de reparo (topológica)

Reconstruir upstream-first; manter a safra de `candidatos` atômica por batch (safras misturadas produziram o smoking gun de `bens` 2014):

1. `candidatos` 1996 → `norm_candidatos` → `rcs`/`rcmz` 1996 → `resultados_candidato` 1996
2. `resultados_candidato_municipio_zona` 1994 → `resultados_candidato_municipio` / `resultados_candidato` 1994
3. `detalhes_votacao_municipio_zona` 1996 → `detalhes_votacao_municipio` 1996
4. folhas: `bens_candidato` 2014, `perfil_eleitorado_municipio_zona` 2008/2016
