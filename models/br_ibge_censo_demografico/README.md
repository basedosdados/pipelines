# br_ibge_censo_demografico — notas de manutenção

Censo Demográfico do IBGE. 37 modelos:

| Família | Anos | Grão |
|---|---|---|
`microdados_pessoa` | 1970, 1980, 1991, 2000, 2010, 2022 | pessoa |
`microdados_domicilio` | 1970, 1980, 1991, 2000, 2010, 2022 | domicílio |
`microdados_familia` | 2022 | família |
`microdados_mortalidade` | 2022 | óbito reportado no domicílio |
`setor_censitario_*` (22 tabelas) | 2010 | setor censitário |
`dicionario` | todas | par chave → valor |

Censo é decenal e não há pipeline do Prefect: as cargas são feitas à mão pelo código em
`code/`.

## Nomes de coluna seguem os códigos do IBGE

As colunas que não têm nome consagrado ficam com o código do questionário — `v0502` na série
histórica, `p0150`/`d0130` em 2022. Só as que têm nome estável em todos os censos são
traduzidas: `id_regiao`, `sigla_uf`, `controle`, `numero_ordem`, `peso_amostral`,
`situacao_setor`, `situacao_domicilio`.

Isso diverge do manual de estilo, que pede nome "intuitivo, claro e extenso" e reserva o
código para `original_name`. A escolha mantém a comparabilidade entre censos: renomear um ano
só quebraria a série, e renomear todos é trabalho separado (ver Pendências). O nome original
de cada coluna está registrado nos metadados.

## Microdados de 2022 — arquivo de acesso público

`microdados_*_2022` vêm do arquivo de **acesso público** da amostra, não do arquivo restrito:
geografia máxima é a UF, apenas registros com risco de revelação abaixo de 20%, idade em
grupos quinquenais, variáveis quase-identificadoras omitidas, subamostra de 50% dos
domicílios. `peso_amostral` é adimensional (única coluna numérica sem `measurement_unit`) e é
obrigatório em qualquer estimativa — a soma dos pesos da `pessoa` reproduz os 203.080.756
habitantes divulgados pelo IBGE.

`ano` e `sigla_uf` são colunas de partição e existem apenas no caminho hive, não dentro do
parquet; a UF vem no arquivo bruto como código de dois dígitos e é convertida para a sigla no
`clean.py`.

## Dicionário

Um dicionário por conjunto, cobrindo as 14 tabelas de microdados de todos os anos. A staging
é uma external **CSV** sobre dois arquivos no mesmo prefixo:

```text
gs://<bucket>/staging/br_ibge_censo_demografico/dicionario/
├── dicionario.csv        1970-2010, 2.954 linhas
└── dicionario_2022.csv     2022,      892 linhas
```

Consequências de os dois conviverem num prefixo:

- **Qualquer carga precisa ter os dois arquivos em disco.** `_upload_to_gcs` limpa o prefixo e
  reenvia só o `data_path` local, então subir apenas um dos CSVs apaga o outro do bucket.
  `sync_gcs.py::fetch_historical_dicionario` baixa o histórico antes de subir e falha se ele
  não estiver no bucket, em vez de truncar a tabela.
- O modelo lê apenas a staging. Não referenciar a tabela publicada em produção: além de fazer
  a série histórica existir só como dado publicado, o resultado passa a depender do ambiente.

`regexp_replace(chave, r"\.0$", "")` no modelo remove o sufixo `.0` que o CSV histórico traz
em 2.732 das 2.954 chaves (resquício de leitura como float). A âncora `$` é necessária: sem
ela, uma chave decimal como `10.05` perderia o `.0` do meio.

`cobertura_temporal` é `(1)` em todas as linhas — "mesma cobertura da tabela" na notação do
manual. Valor literal do ano seria redundante, porque `id_tabela` já carrega o ano
(`microdados_pessoa_2010` × `microdados_pessoa_2022`).

## Limiares do `not_null_proportion_multiple_columns`

`at_least` é o piso de **não-nulos**: a macro reprova quando `nulos / total > 1 - at_least`.
O `0.05` usado na maior parte do repositório só reprova coluna praticamente vazia.

| Modelo | `at_least` | `ignore_values` |
|---|---|---|
`microdados_pessoa_2022` | 0.95 | 66 colunas |
`microdados_domicilio_2022` | 0.95 | `d0290` |
`microdados_familia_2022` | 0.95 | `f0220`, `f0270` |
`microdados_mortalidade_2022` | 0.95 | — |
`dicionario` | 0.90 | — |

As colunas dispensadas são variáveis condicionais do questionário — perguntadas só a
migrantes, indígenas e quilombolas, ocupados, quem estuda, quem tem renda — e ficam vazias na
maioria das linhas por desenho da coleta, não por falha de carga. Nenhuma coluna das quatro
tabelas de 2022 está totalmente vazia.

O `dicionario` fica em `0.90` porque `chave` está em 94,23%: 222 linhas com chave nula, todas
nas tabelas de 1970-2010. O manual proíbe chave nula, então é defeito a corrigir; `0.95`
reprovaria hoje. O ponto de virada é `0.9423`.

## Código de carga (`code/`)

| Arquivo | Função |
|---|---|
`download.py` | baixa os 27 zips de UF e os dois documentos do FTP do IBGE |
`clean.py` | extrai os CSVs e escreve parquet particionado; `--tables dicionario` gera só o dicionário, sem precisar dos zips |
`sync_gcs.py` | sobe a staging de dev (com conferência de linhas local × BigQuery) e monta um `auxiliary_files.zip` por tabela |
`build_architecture.py` / `build_dbt.py` | geram as arquiteturas em `code/architecture/` e os modelos a partir do layout do IBGE |

Os dados intermediários ficam fora da árvore do repositório, em
`~/Downloads/br_ibge_censo_demografico_data/` (`CENSO_DATA_ROOT` sobrescreve). A carga escreve
apenas `basedosdados-dev`; `sync_gcs.py::assert_dev_target` recusa qualquer outro destino,
porque `dump_mode="overwrite"` apaga a tabela antes de recriá-la.

## O table-approve só sincroniza tabela cujo `.sql` mudou na PR

O merge copia `gs://basedosdados-dev/staging/<ds>/<tabela>/` para `gs://basedosdados`
automaticamente, com backup em `basedosdados-backup`. Mas o laço do `--sync-bucket` percorre
apenas os arquivos modificados na PR; `--dataset-id` dispara só o `dbt run`, sem sincronizar
bucket.

Consequência para este conjunto: restaurar um modelo ao estado de `main` tira aquela tabela do
sync, e o dado novo dela não chega a produção — silenciosamente, porque a materialização
conclui sem erro. Antes do merge, conferir que toda tabela com dado novo aparece em:

```bash
git diff origin/main --name-only | grep '\.sql$'
```

## Pendências

- **32 tabelas históricas sem teste algum**, nem `unique_combination_of_columns`. Fechar o
  conjunto exige medir a esparsidade de cada uma, como foi feito nas de 2022.
- **222 chaves nulas no dicionário histórico**, em 10 tabelas de 1970-2010. Corrigi-las
  permite subir o limiar do `dicionario` para `0.95`.
- **Sem `custom_dictionary_coverage`** em nenhuma tabela, embora a `pessoa` de 2022 tenha 92
  colunas marcadas `covered_by_dictionary`. Nada verifica se os códigos guardados têm tradução
  no dicionário.
- **Dicionário histórico sem origem reproduzível.** Existe como um CSV de 2023 no bucket; o
  IBGE ainda publica as fontes. 2010 tem `Documentacao.zip` com `Layout_microdados_Amostra.xls`
  parseável, no mesmo formato de 2022. 2000 tem `1_Documentacao_20170908.zip`, mas os rótulos
  estão num `.doc` de 1,5 MB com planilhas avulsas por variável — os scripts SAS incluídos são
  a via mais limpa. 1970, 1980 e 1991 só trazem documentação dentro dos zips de microdados
  (251, 520 e 643 MB).
- **`cobertura_temporal` vazia** nas 264 colunas das quatro arquiteturas de 2022; o manual pede
  `(1)`.
- **Renomear os códigos do IBGE** para nomes descritivos, em toda a série de uma vez.
