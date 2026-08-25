# Documentação do Conjunto de Dados: CNPJ (Cadastro de Pessoa Jurídica)

Pipeline de dados de CNPJ (empresas, estabelecimentos, sócios e simples) da
Receita Federal. Este README documenta as mudanças estruturais feitas na
migração desta pipeline (antes `br_me_cnpj`), o motivo de cada uma e seus
impactos no reprocessamento histórico.

## 1. Correção de encoding na leitura dos CSVs

Os arquivos CSV de origem da Receita Federal são publicados em `latin1`
(`ISO-8859-1`), não em `utf-8`. A pipeline anterior lia esses arquivos com o
encoding incorreto, o que inseria caracteres inválidos em colunas de
texto livre — por exemplo, `razao_social`, `nome_fantasia` e demais campos com
acentuação.

A correção lê o CSV de origem como `latin1` e regrava o CSV intermediário como
`utf-8`, preservando a acentuação corretamente (ver `process_csv_*` em
`pipelines/crawler/rf_cnpj/utils.py`).

## 2. Necessidade de reprocessamento desde 2023

Como o bug de encoding afeta todo dado ingerido pela pipeline antiga, os dados
de referência precisaram ser reprocessados do zero para corrigir os caracteres já gravados incorretamente em BigQuery. 

**Data Range:** 2023-06-10 a 2026-05-10

**Colunas afetadas:**
- br_me_cnpj.estabelecimentos : bairro, complemento, email, logradouro, nome_fantasia, numero, tipo_logradouro

- br_me_cnpj.empresas: razao_social

- br_me_cnpj.socios: nome

## 3. Migração de `br_me_cnpj` para `br_rf_cnpj`

O dataset estava indexado sob a organização ME (Ministério da Economia), que
não é a fonte dos dados. A fonte real é a Receita Federal (RF), então o dataset
foi renomeado/migrado para `br_rf_cnpj` para refletir corretamente a
organização de origem. Toda referência à organização e aos metadados do
dataset (incluindo joins em `br_bd_diretorios_brasil__empresa`) foi atualizada
de `br_me_cnpj` para `br_rf_cnpj`.

## 4. Particionamento por data de referência (`folder_date`, não `last_modified_date`)

O particionamento passou a ser feito pela **data de referência do arquivo na
fonte** (`folder_date` — o mês/competência a que os dados dizem respeito), e
não mais pela `last_modified_date` (data em que o arquivo foi modificado/gerado
pela Receita Federal). Isso alinha o particionamento ao período que os dados de
fato representam.


## 5. Pipeline recorrente de dicionário

A tabela `br_rf_cnpj__dicionario` traduz os códigos usados nas demais tabelas
(qualificação do responsável, motivo da situação cadastral, porte, etc.) para seus valores legíveis
(`chave` - `valor`). Ela é materializada como `table`, não incremental,
apenas o snapshot mais atual dos códigos.

O conteúdo vem de duas origens, unidas no modelo:

- **Arquivos de dicionário publicados pela própria Receita Federal** junto
  com a publicação mensal: arquivo de `Cnaes`, `Naturezas`, `Qualificacoes`,
  `Municipios`, `Paises`, `Motivos`. Esses arquivos são baixados
  e processados por `process_csv_dicionario`
  (`pipelines/crawler/rf_cnpj/utils.py`), que lê cada CSV `chave;valor` e adiciona `id_tabela`/`nome_coluna` para identificar a qual tabela e coluna cada código pertence.
- **Entradas manuais** (`dicionario_not_found`, no modelo dbt), cobrindo
  chaves que aparecem nos dados reais mas não constam nos arquivos oficiais
  da Receita Federal (ex.: códigos `36`, `994`/`393`, `8`/`9`/`32` sem
  correspondência na fonte). Sem essas entradas, os valores ficam sem
  tradução no dicionário.

Como `simples` e `dicionario` não têm cobertura temporal por competência
(`NonHistorical`), o polling dessas tabelas compara contra `Table.Update`
(quando rodamos pela última vez), e não contra `Coverage`, ao decidir se há
dado novo a processar.

## 6. Tabelas legado (`*_legado`) e materialização full-refresh

Foram criadas tabelas `_legado` (`empresas_legado`, `estabelecimentos_legado`,
`socios_legado`) em staging, contendo os dados históricos migrados do
`br_me_cnpj`. Os modelos principais (`br_rf_cnpj__empresas`,
`br_rf_cnpj__estabelecimentos`, `br_rf_cnpj__socios`) são incrementais e, na
materialização **full-refresh** (primeira execução / quando não há
`is_incremental()`), fazem `union all` com os dados das tabelas `_legado` para
reconstruir o histórico completo (dados até 2023-04-30). Em execuções
incrementais normais, apenas os dados novos vindos da staging atual
(`empresas`, `estabelecimentos`, `socios`) são adicionados.


## 7. Alerta: houve erro na divulgação pela Receita Federal. A representação do documento de sócio pessoa jurídica passou de 14 para 8 caracteres a partir do mês de agosto de 2026.

Na tabela de sócios, para registros de sócio **pessoa jurídica** (`tipo = "1"`),
a coluna `documento` armazenava historicamente o **CNPJ completo (14
caracteres)** do sócio PJ. Em **agosto de 2026** a própria Receita Federal passou a
publicar esse campo com **apenas 8 caracteres** (o `cnpj_basico`, sem
filial/dígito verificador), reduzindo a granularidade da identificação do
sócio PJ na fonte.

Essa é uma mudança **na fonte**, não introduzida por esta pipeline.

## Resumo
* Correção de encoding: os CSVs de origem da Receita Federal eram lidos incorretamente, gerando caracteres inválidos em campos de texto (ex.: razão social, nome fantasia). 
* Reprocessamento histórico: dados de referência a partir de 2023 foram reprocessados para corrigir os registros afetados pelo erro de encoding.
* Correção da organização de origem: dataset migrado de br_me_cnpj (Ministério da Economia) para br_rf_cnpj, refletindo corretamente a Receita Federal como fonte dos dados.
* Particionamento por data de referência: a partição das tabelas passou a ser baseada na data de referência do arquivo na fonte (competência dos dados), em vez da data de modificação do arquivo.
* Preservação do histórico: dados anteriores a maio/2023, hoje indisponíveis na fonte original, foram preservados em tabelas legado e continuam incluídos na base a cada atualização completa da tabela.
* Alerta de fonte: o documento do sócio pessoa jurídica (tabela `socios`, `tipo = "1"`) passou a ser publicado pela Receita Federal com 8 caracteres em vez dos 14 caracteres do CNPJ completo, a partir da competência 2026-08.
