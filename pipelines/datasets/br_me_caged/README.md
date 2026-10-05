# Documentação do Conjunto de Dados: br_me_caged (Novo CAGED)

Microdados do Novo Cadastro Geral de Empregados e Desempregados (Novo CAGED),
publicados pelo Ministério do Trabalho e Emprego: cada linha é uma admissão ou um
desligamento de emprego formal.

- Fonte: FTP anônimo `ftp.mtps.gov.br`, pasta `pdet/microdados/NOVO CAGED/`
- Código: `pipelines/datasets/br_me_caged/flows.py`, `pipelines/crawler/me_caged/` e
  `models/br_me_caged/`

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

O crawler troca o código da UF pela sigla e descarta `regiao`, `unidadesalariocodigo` e
`valorsalariofixo`. O modelo dbt converte o município de 6 para 7 dígitos, cruzando com
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

Há um flow por tabela, todos com a mesma lógica (`_run_me_caged`, em `flows.py`). Cada
execução:

1. **Descobre o último mês publicado**: `get_source_last_date` lista as pastas do FTP.
2. **Verifica se há novidade**: `poll_source_for_update_task` compara esse mês com o
   fim da cobertura da tabela em produção, no formato `%Y-%m`. Se a fonte não estiver
   à frente, o flow termina sem baixar nada.
3. **Registra a publicação da fonte**: `commit_source_update_task` grava o `Update` da
   fonte **antes do download**. Isso não trava a próxima tentativa, porque a
   verificação do passo 2 olha a cobertura da tabela, não esse registro. Se o flow
   falhar depois, a cobertura não anda e a execução seguinte tenta de novo.
4. **Baixa os meses que faltam**: `generate_yearmonth_range` lista os meses entre o fim
   da cobertura e o último publicado, e `crawl_novo_caged_ftp` baixa só o `.7z` da
   tabela em cada um. Os arquivos que falham vão para `failed_downloads`, registrada no
   log; se nenhum arquivo do mês for baixado, a execução para com erro.
5. **Gera as partições**: `build_partitions` grava um CSV por `ano`, `mes` e
   `sigla_uf`.
6. **Carrega em dev e depois em produção**: os CSVs vão para o bucket
   `basedosdados-dev` e o dbt roda e testa em dev; só então vão para o bucket
   `basedosdados` e o dbt roda e testa em produção. Uma falha em dev impede a ida para
   produção.
7. **Atualiza a cobertura**: `register_table_materialization_task` lê o último mês da
   tabela de produção e atualiza a cobertura no site.

A cobertura tem duas partes: os 6 meses mais recentes ficam restritos ao BD Pro e o
restante é aberto. O passo 7 recalcula esse corte a cada mês e recria as regras de
acesso no BigQuery, então a janela anda sozinha.

**Execução verde não quer dizer que ingeriu.** Quando não há novidade, o flow
termina com sucesso do mesmo jeito. Para saber se entrou dado, procure no log as
mensagens `Há atualizações na fonte original` e `dbt run OK`, ou veja se a cobertura
andou.

## Agendamento

`0 8,17 1-4,26-31 * *`, no horário de Brasília: às 8h e às 17h dos dias 26 a 31 e 1 a
4. A janela acompanha a publicação, que sai entre os dias 27 e 30; os dias 1 a 4
cobrem um atraso da fonte. A execução das 17h pega a publicação do mesmo dia, e a das
8h é uma segunda tentativa. Fora dessa janela não haveria o que baixar.

## Parâmetros

| parâmetro | padrão | quando mudar |
|---|---|---|
| `materialize_after_dump` | `True` | `False` para parar depois do dbt em dev, sem levar nada para produção |
| `update_metadata` | `True` | `False` para não registrar cobertura nem a publicação da fonte |
| `target` | `prod` | define onde roda o segundo `dbt run`; o upload e o registro de cobertura continuam indo para produção |
| `force_run` | `False` | `True` para pular a verificação de novidade (passo 2) |

Para testar no pool de dev sem gravar nada em produção:

```json
{"materialize_after_dump": false, "update_metadata": false, "force_run": true}
```

O teste só baixa dados quando a fonte tem um mês que a tabela de produção ainda não
tem, porque os meses a baixar vêm da cobertura. Com a tabela em dia, não há o que
baixar.

## Rodar manualmente e reprocessar

**Meses atrasados:** basta rodar o flow com os parâmetros padrão. Todos os meses que
faltam entram na mesma execução.

**Um mês já carregado** não é reprocessado pelo flow, nem com `force_run`: os meses a
baixar começam depois da cobertura, e as tabelas incrementais ignoram meses que já
têm. Para reprocessar:

1. gere os CSVs do mês e suba para a staging de dev com as próprias funções do
   crawler, rodando a partir de uma pasta fora do repositório (o crawler grava em
   `tmp/`):

   ```python
   from pipelines.crawler.me_caged.tasks import (
       build_partitions,
       build_table_paths,
       crawl_novo_caged_ftp,
   )
   from pipelines.utils.tasks import upload_to_gcs

   table_id = "microdados_movimentacao"
   _, output_dir = build_table_paths.fn(table_id)
   crawl_novo_caged_ftp.fn("202512", table_id)
   path = build_partitions.fn(table_id=table_id, table_output_dir=output_dir)
   upload_to_gcs.fn(
       data_path=path,
       dataset_id="br_me_caged",
       table_id=table_id,
       bucket_name="basedosdados-dev",
       dump_mode="append",
   )
   ```

   O mês novo substitui o antigo no mesmo caminho e os outros ficam como estão;
2. reconstrua a tabela com `dbt run --select br_me_caged__<tabela> --full-refresh`,
   que lê a staging inteira de novo. A `_excluida` não precisa da opção;
3. em produção, a reconstrução precisa de credenciais de produção, e depois dela o
   passo 7 tem que rodar de novo para recriar as regras de acesso do BD Pro.
