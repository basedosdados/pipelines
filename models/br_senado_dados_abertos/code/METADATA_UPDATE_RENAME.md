# Metadata column update — normalization (staging + prod)

The dbt models were normalized. Backend column metadata must match on BOTH the
`staging` and `prod` backends. Do the SAME operations on each (run env="staging"
first, then env="prod").

## Renamed columns (old name → new name)
| table | delete (old) | the new name is created by bulk_upsert |
|---|---|---|
| partido | `sigla_partido`, `nome_partido` | `sigla`, `nome` |
| bloco | `nome_bloco` | `nome` |
| comissao | `sigla_comissao`, `nome_comissao` | `sigla`, `nome` |
| votacao | `descricao_votacao`, `resultado_votacao`, `sequencial_votacao` | `descricao`, `resultado`, `sequencial` |

## Dropped columns (delete, no replacement)
| table | delete |
|---|---|
| votacao_parlamentar | `nome`, `sexo` |

## Procedure (per backend: staging, then prod)
For each affected table (partido, bloco, comissao, votacao, votacao_parlamentar):
1. `get_dataset("br_senado_dados_abertos", env=<env>)` → read the table's columns (id + name).
2. `delete_column(column_id=<id of each OLD/dropped name above>, env=<env>)`.
3. For the 4 tables with NEW names (partido, bloco, comissao, votacao) — NOT votacao_parlamentar —
   `bulk_upsert_columns(table_id=<id>, columns_json=<contents of code/columns_json/<table>.json>, env=<env>)`.
   This creates the new-named columns (sigla/nome/descricao/resultado/sequencial) with their
   PT/EN/ES descriptions and is idempotent for the unchanged columns.
4. `reorder_columns(table_id=<id>, column_names=<the exact column-name order from
   code/columns_json/<table>.json>, env=<env>)` so the new columns sit in their architecture position.

## Do NOT change
- Observation levels and their grain-column links (grain columns id_partido, id_comissao, id_bloco,
  ano, id_votacao, id_senador are all UNCHANGED by this rename).
- `is_partition` on `ano` (unchanged). `is_primary_key` stays False.
- Dataset status stays `under_review` on prod; `is_closed=false` coverages unchanged.

## Report
After both backends: for each affected table, list its final column names (via get_dataset) and confirm
no orphaned old names remain and the new names are present. Note any errors.
