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
  confere as semanas (ver abaixo).
- **`extract_and_load`**: baixa e trata os três arquivos (`utils.py`) e grava
  `data_coleta=aaaa-mm-dd/data.csv` na staging.
- **`build_and_promote`**: estágio comum a todos os conjuntos
  (`pipelines/utils/metadata/flows.py`). Roda o dbt e publica em produção.

O modelo dbt é incremental com `insert_overwrite`: cada run regrava, a partir da
staging, as datas desde 1º de janeiro do ano de `max(data_coleta) - 35 dias` (a maior
data da tabela). Com a maior data em 01/02/2026, por exemplo, regrava desde
01/01/2025. Qualquer data a partir desse limite que estiver na staging entra sozinha,
inclusive uma semana que a ANP republicar com correção; datas anteriores só entram
com `--full-refresh`.

### Verificação de lacunas

`verificar_lacuna` **falha o `check_update`** se, nos últimos 182 dias
(`JANELA_LACUNA`), duas datas de coleta seguidas da tabela de produção ficam a mais de
7 dias uma da outra (`DIAS_SEM_COLETA`). A primeira data do arquivo entra na conta,
para pegar a semana que saiu do arquivo sem ser carregada.

- Roda **depois** de disparar o `extract_and_load`, de propósito: as semanas que ainda
  estão no arquivo são carregadas antes de saírem da janela de 4 semanas.
- Como olha a tabela, e não a cobertura, continua falhando todo dia até as semanas
  serem recuperadas (ver [Operação](#operação)) ou saírem da janela.
- Lacunas que também faltam nos semestrais são da fonte: registre o par de datas em
  `LACUNAS_DA_FONTE` (`constants.py`) para a checagem ignorá-las. A última foi de 13 a
  25/09/2022.
- A consulta lê cerca de 30 MiB por dia.

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

- Os produtos aparecem com `initcap` (`Glp`, `Gnv`, `Diesel S10`); `Diesel S50` só
  existe em 2012.

## Problemas conhecidos

### A ANP repete chaves no próprio arquivo

Algumas combinações de `(cnpj_revenda, data_coleta, produto)` aparecem mais de uma vez
nos arquivos da ANP, com o mesmo preço ou com preços diferentes. As linhas ficam como
publicadas: o arquivo não traz identificador da coleta, então uma repetição pode ser um
registro dobrado ou duas coletas no mesmo dia. Nenhum outro ano tem repetição.

| ano | datas | postos | chaves repetidas | linhas a mais | % das linhas do ano |
|---|---|---|---|---|---|
| 2005 | 7, entre 02/05 e 04/06 | 6 (SP, MG, RJ, PB) | 36 | 144 | 0,0075% |
| 2024 | 10/06 e 12/06 | Posto Metano (RJ) e Souza Filho (SC), com o mesmo preço; Auto Posto Mangueira II (Taubaté/SP), com preços diferentes | 14 | 14 | 0,0013% |
| 2026 | 16/02 e 18/02 | H. D. Portella (RS), com o mesmo preço | 6 | 6 | 0,0007% |

A causa foi perguntada à ANP via LAI (protocolo 48003.023478/2026-51, prazo
21/12/2026), acompanhada na #2371.

## Testes

Ficam em `models/br_anp_precos_combustiveis/schema.yml`, rodam só no ano mais recente
(`where: __most_recent_year__`) e têm severidade `error`: uma falha no
`build_and_promote` em dev impede a publicação em produção.

| teste | o que verifica |
|---|---|
| `custom_unique_combinations_of_columns` | `(cnpj_revenda, data_coleta, produto)` repetida em no máximo 0,002% das linhas (`proportion_allowed_failures: 0.00002`), acima do medido em 2024 (0,0013%) e 2026 (0,0007%). Enquanto a #2324 não for corrigida, o teste compara cada chave repetida, e não o total, com esse limite |
| `not_null_proportion_multiple_columns` | pelo menos 99% de preenchimento em cada coluna, exceto `preco_compra` (vazio desde 2021) |
| `dbt_utils.expression_is_true` (unidade) | `R$/13kg` para GLP, `R$/m3` para GNV e `R$/litro` para os demais |
| `dbt_utils.expression_is_true` (preço) | `preco_venda` entre R$ 50 e R$ 250 para GLP e entre R$ 1 e R$ 20 para os demais. De 2021 a 2026, nenhuma linha ficou fora: o GLP variou de R$ 55,99 a R$ 170 e os demais, de R$ 2,05 a R$ 9,99 |
| `relationships` | `sigla_uf` e `id_municipio` existem nos diretórios |
| `accepted_values` | `produto` só com os valores esperados |

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
     (`folders`). Se as datas são a partir de 1º de janeiro do ano de
     `max(data_coleta) - 35 dias`, o próximo run já as inclui; se são anteriores, rodar o
     deployment "BD template: Executa DBT model" com `target="prod"` e
     `flags="--full-refresh"` e, logo depois, o passo abaixo.
- **`--full-refresh` em produção** remove as regras de acesso do BD Pro, e esse
  deployment não registra a materialização (#2368). Para recriá-las, rodar em seguida o
  `transfer_files_to_prod_flow` com `folders` só da data mais recente e
  `dbt_command="test"`: ele copia um arquivo, roda os testes e registra a
  materialização. O `--full-refresh` lê cerca de 4,8 GiB; o run de cada semana, cerca
  de 0,7 GiB.
- **Semestrais × arquivos de 4 semanas:** postos, produtos e preços são os mesmos, mas o
  cadastro do posto (nome, endereço, CEP) pode vir atualizado no semestral.
