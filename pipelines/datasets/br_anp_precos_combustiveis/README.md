# Preços de Combustíveis (ANP)

Pipeline que mantém atualizada a tabela `microdados` do conjunto
`br_anp_precos_combustiveis`: preços de venda coletados toda semana pela ANP em uma
amostra de postos. Cada linha é o preço de um produto, em um posto, em uma data de
coleta, desde mai/2004.

## Fonte

A [Série Histórica de Preços de Combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis)
publica três arquivos em endereços fixos (`ultimas-4-semanas-glp.csv`,
`...-gasolina-etanol.csv` e `...-diesel-gnv.csv`), separados por `;`, com decimal em
vírgula. Cada um traz **só as últimas 4 semanas**: a tabela acumula o histórico; os
arquivos, não.

A mesma página publica arquivos semestrais (`dsas/ca/ca-AAAA-SS` e
`dsas/glp/glp-AAAA-SS`), que a pipeline não lê. Eles servem para recuperar semanas
perdidas (ver [Operação](#operação)).

Particularidades:

- a pesquisa é amostral: um município fora da amostra não aparece na tabela;
- `Valor de Compra` vem vazio nos arquivos atuais;
- o arquivo não traz o código IBGE do município, só o nome e a UF;
- entre maio e junho de 2026, a ANP serviu XLSX nos endereços `.csv`.

## Como a pipeline funciona

```text
check_update  ──▶  extract_and_load  ──▶  build_and_promote
(todo dia, 10h)    (4 semanas)            (dbt e publicação)
```

- **`check_update`**: lê a maior data do arquivo de GLP e compara com a cobertura da
  tabela. Se houver data nova, dispara o próximo estágio. Depois, `verificar_lacuna`
  **falha a execução** se a tabela parou mais de 7 dias antes da primeira data do
  arquivo, ou seja, se uma semana saiu do arquivo sem ser carregada. Desde 2019,
  feriados nunca deixaram mais de 7 dias sem coleta.
- **`extract_and_load`**: baixa e trata os três arquivos (`utils.py`) e grava
  `data_coleta=aaaa-mm-dd/data.csv` na staging.
- **`build_and_promote`**: estágio comum a todos os conjuntos
  (`pipelines/utils/metadata/flows.py`). Roda o dbt e publica em produção.

O modelo dbt é incremental com `insert_overwrite`: cada run regrava, a partir da
staging, os anos da janela de 4 semanas. Uma semana que a ANP republicar com correção
entra sozinha; datas fora da janela só entram com `--full-refresh`.

### Tratamento

- **`read_anp_file`** identifica o formato pelos primeiros bytes (`PK\x03\x04` é
  XLSX). No XLSX de 2026, cada linha do CSV foi cortada nas vírgulas, uma parte por
  célula; a função junta as células de novo e lê o texto como CSV. O CNPJ é lido como
  texto, para não perder o zero à esquerda.
- **`id_municipio`** vem do cruzamento do nome do município e da UF com
  `br_bd_diretorios_brasil.municipio`. Municípios que não casam ficam nulos e são
  listados no log.
- **`unidade_medida`** é padronizada por `UNIDADES` (`constants.py`). Uma grafia fora
  do mapeamento **interrompe o flow**; inclua a nova grafia ali.
- `endereco_revenda` junta rua, número e complemento; os preços trocam a vírgula por
  ponto; o CEP perde o hífen.

## Como ler a tabela

- **A ANP repete algumas linhas no próprio arquivo** (2005, jun/2024 e fev/2026). Elas
  são mantidas como publicadas.
- **O mesmo posto pode ter dois preços no mesmo dia e produto** (ex.: Taubaté,
  jun/2024). Não é duplicata; as duas linhas ficam.
- Os produtos aparecem com `initcap` (`Glp`, `Gnv`, `Diesel S10`); `Diesel S50` só
  existe em 2012.

## Operação

- **Teste em dev:** com o label `deploy-flow` no PR, disparar o `dev-…check_update`.
  Nada vai para produção, mas o `check_update` grava em produção o registro de consulta
  à fonte. O `build_and_promote` usa os modelos dbt da `main`: mudanças nos modelos de
  um PR devem ser validadas com `dbt run` e `dbt test` locais.
- **Recuperar semanas perdidas** (quando `verificar_lacuna` falhar):
  1. gerar as datas pelos semestrais, primeiro sem gravar, conferir o resumo e rodar de
     novo com `--gravar`:

     ```bash
     uv run python models/br_anp_precos_combustiveis/code/recuperar_semanas.py \
         --inicio 2023-08-05 --fim 2023-08-20
     ```

     O script usa o mesmo tratamento do flow e só cria pastas novas. Para refazer datas
     que já existem, copie antes a staging para um prefixo de backup e passe
     `--substituir <prefixo>`;
  2. rodar `dbt run --full-refresh` e `dbt test` em dev;
  3. levar só as pastas novas para produção com `transfer_files_to_prod_flow`
     (`folders`) e rodar o deployment "BD template: Executa DBT model" com
     `target="prod"` e `flags="--full-refresh"`.
- **`--full-refresh` em produção** remove as regras de acesso do BD Pro, recriadas na
  próxima carga do `build_and_promote`.
- **Semestrais × arquivos de 4 semanas:** postos, produtos e preços são os mesmos, mas o
  cadastro do posto (nome, endereço, CEP) pode vir atualizado no semestral.
