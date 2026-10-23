# Documentação do Conjunto de Dados: br_me_caged (Novo CAGED)

Microdados do Novo Cadastro Geral de Empregados e Desempregados (Novo CAGED),
publicados pelo Ministério do Trabalho e Emprego: cada linha é uma admissão ou um
desligamento de emprego formal.

- Fonte: FTP anônimo `ftp.mtps.gov.br`, pasta `pdet/microdados/NOVO CAGED/`
- Código: `pipelines/datasets/br_me_caged/` e `models/br_me_caged/`

## A fonte

Há uma pasta por ano e, dentro dela, uma pasta `AAAAMM` por mês de competência, desde
janeiro de 2020. Cada pasta mensal traz três arquivos `.7z`, cada um com um `.txt`
separado por `;`, em UTF-8 e com decimal em vírgula:

| arquivo | conteúdo | tabela |
|---|---|---|
| `CAGEDMOVAAAAMM.7z` | movimentações declaradas no prazo | `microdados_movimentacao` |
| `CAGEDFORAAAAMM.7z` | movimentações declaradas fora do prazo | `microdados_movimentacao_fora_prazo` |
| `CAGEDEXCAAAAMM.7z` | movimentações excluídas | `microdados_movimentacao_excluida` |

A publicação é mensal: o mês M sai entre os dias 27 e 30 do mês seguinte, no começo
da tarde. A fonte pode atualizar meses já publicados; o `Aviso 220131.txt`, nas
pastas `202110` e `202111`, registra uma dessas revisões.

## As tabelas

As três tabelas de microdados têm as mesmas colunas de trabalhador e de vínculo, e
`saldo_movimentacao` vale `1` na admissão e `-1` no desligamento. A diferença está no
que cada uma registra e no que `ano` e `mes` querem dizer:

- **`microdados_movimentacao`**: o que foi declarado dentro do prazo. `ano` e `mes`
  são o mês da movimentação.
- **`microdados_movimentacao_fora_prazo`**: movimentações de meses anteriores,
  declaradas com atraso. `ano` e `mes` são o mês da declaração; o mês da
  movimentação está em `ano_competencia_movimentacao` e `mes_competencia_movimentacao`.
- **`microdados_movimentacao_excluida`**: cancelamentos de movimentações já
  declaradas. `ano` e `mes` são o mês da exclusão. O `saldo_movimentacao` mantém o
  sinal da movimentação cancelada, e o efeito da exclusão é o inverso: excluir uma
  admissão reduz o saldo.

Para o saldo de um mês com todos os ajustes, some as duas primeiras tabelas e subtraia
a terceira, agrupando pelo mês da movimentação.

O `build_partitions` troca o código da UF pela sigla e descarta `regiao`,
`unidadesalariocodigo` e `valorsalariofixo`. O modelo dbt converte o município de 6 para 7 dígitos, cruzando com
`br_bd_diretorios_brasil.municipio`.

`microdados_movimentacao` e `_fora_prazo` são **incrementais**: cada `dbt run` só
acrescenta os meses posteriores ao último que já está na tabela, então rodar de novo
não duplica dados. `_excluida` é reconstruída inteira a cada execução.

### `dicionario`

Traduz os códigos das colunas (por exemplo, `tipo_movimentacao = 32` é "Desligamento
Por Demissão Com Justa Causa"). Ele lê um arquivo único na staging,
`gs://basedosdados/staging/br_me_caged/dicionario/`, carregado fora do flow. **Nenhum
flow o atualiza.** Quando a fonte criar ou mudar um código, é preciso substituir esse
arquivo e rodar `dbt run --select br_me_caged__dicionario`.

## Como o flow funciona

O conjunto segue o modelo em etapas. Cada tabela tem dois flows em `flows.py`, montados
por `CheckThenExtractLoadPipeline`, e a terceira etapa é o `build_and_promote`
compartilhado. Cada etapa roda num pod próprio e dispara a seguinte:

1. **`br_me_caged_<tabela>_check_update`**, com o cron: `get_latest_update` lista as
   pastas do FTP e devolve o último mês publicado. O código compartilhado compara esse
   mês com o fim da cobertura da tabela em produção, no formato `%Y-%m`. Se a fonte não
   estiver à frente, o flow termina. Se estiver, grava o `Update` da fonte e dispara o
   `_download` com o mês publicado em `reference_date`. O `Update` da fonte é gravado
   antes do download e não trava a próxima tentativa, porque a comparação olha a
   cobertura da tabela, não esse registro: se uma etapa seguinte falhar, a cobertura não
   anda, e a execução seguinte tenta de novo.
2. **`br_me_caged_<tabela>_download`**: `run_download` decide os meses com
   `resolve_year_months`, do fim da cobertura até o `reference_date`, ou os de
   `year_months`, quando vierem no `download_params`. Sem mês, o flow termina sem upload
   nem dbt. Com meses, `extract_load_data` baixa só o `.7z` da tabela em cada um
   (`download_caged_ftp`) e grava um CSV por `ano`, `mes` e `sigla_uf`
   (`build_partitions`). Se nenhum arquivo do mês for baixado, a execução para com
   erro. O código compartilhado sobe os CSVs para a staging de `basedosdados-dev` e
   dispara o `build_and_promote`.
3. **`build_and_promote`**: roda o `dbt run` e o `dbt test` em dev, copia para
   `basedosdados` só as partições desta execução, roda o dbt em produção e atualiza a
   cobertura no site. Um deployment `dev-` nunca chega a esta cópia: o
   `build_and_promote` que ele dispara para depois do dbt em dev.

A cobertura tem duas partes: os 6 meses mais recentes ficam restritos ao BD Pro e o
restante é aberto (`PartBdpro` com `YearMonth`, em `constants.COVERAGE`). O
`build_and_promote` recalcula esse corte a cada carga e recria as regras de acesso no
BigQuery, então a janela anda sozinha.

**Execução verde não quer dizer que ingeriu.** Quando não há novidade, o
`check_update` termina com sucesso do mesmo jeito, sem disparar o `_download`. Para
saber se entrou dado, procure o `_download` e o `build_and_promote` da tabela no
Prefect, a mensagem `dbt run OK` no log, ou veja se a cobertura andou.

## Agendamento

Só o `check_update` tem horário, no horário de Brasília:

| tabela | cron |
|---|---|
| `microdados_movimentacao` | `5 8,17 1-4,26-31 * *` |
| `microdados_movimentacao_fora_prazo` | `10 8,17 1-4,26-31 * *` |
| `microdados_movimentacao_excluida` | `45 8,17 1-4,26-31 * *` |

Às 8h e às 17h dos dias 26 a 31 e 1 a 4, em minutos diferentes para as três não
disputarem o BigQuery. A janela acompanha a publicação, que sai entre os dias 27 e 30;
os dias 1 a 4 cobrem um atraso da fonte. A execução das 17h pega a publicação do mesmo
dia, e a das 8h é uma segunda tentativa. Fora dessa janela não haveria o que baixar.

## Parâmetros

O `check_update` não tem parâmetros. O `_download` recebe um `download_params`:

| chave | o que é |
|---|---|
| `reference_date` | último mês publicado, como `"2026-08-01"`; o `check_update` preenche, e um disparo manual precisa informar |
| `year_months` | opcional: lista de meses `AAAAMM` a baixar e carregar, no lugar dos que faltam na cobertura; um mês fora do intervalo de 202001 ao `reference_date` faz o flow parar com erro |

Para testar no pool de dev sem levar nada para produção, com um mês já publicado,
dispare o `dev-br_me_caged_<tabela>_download` com:

```json
{"download_params": {"reference_date": "2026-08-01", "year_months": ["202608"]}}
```

Ele roda o `_download` e o `build_and_promote` até o dbt em dev. O `check_update` não tem
teste equivalente: o poll e o `Update` da fonte vão para o backend de produção mesmo
num deployment `dev-`.

## Rodar manualmente e reprocessar

**Meses atrasados:** basta disparar o `check_update` da tabela. Todos os meses que
faltam entram na mesma execução.

**Um mês já carregado** volta para a staging pelo `_download` com o mês em
`year_months`, mas a `microdados_movimentacao` e a `_fora_prazo` só mudam depois de
reconstruídas: as tabelas incrementais ignoram meses que já têm. Para reprocessar:

1. dispare o `dev-br_me_caged_<tabela>_download` com o mês:

   ```json
   {"download_params": {"reference_date": "2026-08-01", "year_months": ["202512"]}}
   ```

   O mês novo substitui o antigo na staging de dev, no mesmo caminho, e os outros ficam
   como estão. A `_excluida` já sai reconstruída deste passo;
2. na `movimentacao` e na `_fora_prazo`, reconstrua a tabela em dev com
   `dbt run --select br_me_caged__<tabela> --full-refresh`, que lê a staging inteira de
   novo, e confira o resultado;
3. em produção, use os flows utilitários do Prefect, que rodam com as credenciais do
   worker. Rode fora da janela do cron e na ordem abaixo:
   1. `BD Utils: Transfere arquivos do bucket basedosdados-dev para basedosdados`,
      com `dataset_id="br_me_caged"`, `table_id="<tabela>"`,
      `folders=["ano=2025/mes=12"]` e `dbt_command="test"`. Ele copia só as pastas do
      mês da staging de dev para a de produção; os outros meses ficam como estão. O
      mês aparece na pasta sem zero à esquerda (`mes=1` a `mes=12`);
   2. `BD template: Executa DBT model`, com `dataset_id="br_me_caged"`,
      `table_id="<tabela>"`, `target="prod"`, `dbt_command="run"` e
      `flags="--full-refresh"`. Ele reconstrói a tabela a partir da staging de
      produção. A reconstrução remove as regras de acesso do BD Pro;
   3. logo em seguida, `update_temporal_coverage`, com `dataset_id="br_me_caged"`,
      `table_id="<tabela>"` e a cobertura abaixo. Ele recalcula a cobertura e recria
      as regras de acesso. Até este passo terminar, os meses do BD Pro ficam abertos.

      ```json
      {"tier": "part_bdpro", "date_column": {"kind": "year_month", "year": "ano", "month": "mes"}, "date_format": "%Y-%m", "free_lag": {"unit": "months", "value": 6}}
      ```
