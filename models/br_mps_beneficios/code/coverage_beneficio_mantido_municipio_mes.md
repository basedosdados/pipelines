# Cobertura de municípios e espécies — `beneficio_mantido_municipio_mes`

Diretório de referência: 5571 municípios (`br_bd_diretorios_brasil.municipio`).

O campo `Mun Resid` da fonte não traz código de município — seus dígitos
são o código da Gerência-Executiva do INSS — de modo que a resolução é
feita por nome. A tabela abaixo quantifica a perda por ano.

| ano | meses | municípios | % do diretório | ausentes | benefícios | sem município | % sem município | espécies | categorias |
|---|---|---|---|---|---|---|---|---|---|
| 2021 | 6 | 5564 | 99.87% | 7 | 216,489,971 | 1,008,339 | 0.47% | 60 | 13 |
| 2022 | 12 | 5568 | 99.95% | 3 | 440,649,354 | 2,009,778 | 0.46% | 61 | 14 |
| 2023 | 11 | 5569 | 99.96% | 2 | 420,146,398 | 2,371,851 | 0.56% | 61 | 14 |
| 2024 | 8 | 5569 | 99.96% | 2 | 318,830,645 | 2,545,798 | 0.80% | 33 | 14 |
| 2025 | 10 | 5569 | 99.96% | 2 | 409,117,451 | 4,135,926 | 1.01% | 33 | 14 |
| 2026 | 1 | 5569 | 99.96% | 2 | 40,746,961 | 448,823 | 1.10% | 33 | 14 |

Municípios do diretório nunca observados em toda a série: **2** — MT Boa Esperança do Norte (5101837), PI Pau D'Arco do Piauí (2207793)

Nenhum deles é uma falha de correspondência: a fonte nunca publica esses nomes em nenhuma competência.

## Nomes não resolvidos

Nenhum. Todos os nomes de município publicados pela fonte foram resolvidos para um código IBGE.
