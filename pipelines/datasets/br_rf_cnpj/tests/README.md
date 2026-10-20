# Testes de integridade — br_rf_cnpj

Cobrem os controles de integridade do flow (constantes em `constants.py`,
seção "Constantes de controle de integridade dos dados"). Não usam rede nem GCP.

## Como rodar

```bash
uv run pytest pipelines/datasets/br_rf_cnpj/tests -v
```

## O que cada teste verifica

| Controle | Teste | Resultado esperado |
|---|---|---|
| `validate_expected_file_count` | `test_file_count_expected_passes` (3 tabelas) | 10 arquivos passa |
| | `test_file_count_mismatch_raises` (0, 9, 11) | `ValueError` citando a tabela |
| | `test_file_count_tolerance` | tolerância 1 aceita 9, rejeita 8 |
| `validate_partition_rows` | `test_rows_equal_passes` | sem erro |
| | `test_rows_lost_raises_with_zero_tolerance` | 1 linha perdida aborta |
| | `test_rows_lost_within_tolerance_only_warns` | só log `warning` |
| | `test_rows_lost_above_tolerance_raises` | aborta |
| | `test_rows_zero_input_does_not_divide_by_zero` | CSV vazio passa em silêncio (comportamento atual, documentado) |
| `process_csv_estabelecimentos` | `test_estabelecimentos_all_valid_ufs` | `rows_in == rows_out` == linhas gravadas |
| | `test_estabelecimentos_invalid_uf_is_detected` | UF `ZZ` gera `rows_out < rows_in` e a validação aborta (`2 de 4`) |
| | `test_estabelecimentos_retry_does_not_duplicate` | reprocessar o mesmo arquivo não duplica linhas |
| `main` (task) | `test_main_aborts_before_download_on_wrong_file_count` | 9 arquivos listados → `ValueError` antes de qualquer download |

## Registro de execuções

| Data | Branch | Comando | Resultado |
|---|---|---|---|
| 2026-10-20 | `testes/br_rf_cnpj` | `uv run pytest pipelines/datasets/br_rf_cnpj/tests -v` | 16 passed, 0 failed |

## Não coberto

- Execução ponta a ponta com a infra local (`scripts/dev_local.sh up`) e com
  dados reais da RF: não foi rodada.
- `validate_partition_rows` só é chamada para `estabelecimentos` em `tasks.py`;
  `empresas` e `socios` não têm checagem de linhas.
- Os testes usam um CSV sintético pequeno; não exercitam volume real.
