# Base de Dados — RAIS

## 1. Visão geral

A Relação Anual de Informações Sociais (RAIS) é uma base administrativa produzida pelo Ministério do Trabalho e Emprego (MTE), contendo informações sobre estabelecimentos e vínculos formais de trabalho no Brasil.

---

## 2. Fonte e acesso

- **Fonte oficial:** Ministério do Trabalho e Emprego (MTE)
- **Meio de acesso:** FTP público `ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`
---

## 3. Procedimento de atualização dos dados

Para atualizar os dados da RAIS, siga os passos abaixo:

1. Acesse o diretório FTP da RAIS utilizando um explorador de arquivos local.
2. Navegue até o ano de referência desejado. Exemplo: `ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2024/`
3. Faça o download do(s) arquivo(s) correspondente(s) à base desejada.
4. Salve os arquivos em um diretório local e realize a descompactação.

> **Atenção**
> Os arquivos de Vínculos possuem tamanho superior a 2 GB. Recomenda-se realizar o download, tratamento e exclusão de um arquivo por vez antes de prosseguir para o próximo.

---

## 4. Estrutura dos arquivos

### 4.1 Estabelecimentos

Base disponibilizada em um único arquivo compactado:

- `RAIS_ESTAB_PUB.7z`

### 4.2 Vínculos

Base segmentada em arquivos regionais:

- `RAIS_VIC_PUB_CENTRO_OESTE.7z`
- `RAIS_VIC_PUB_MG_ES_RJ.7z`
- `RAIS_VIC_PUB_NI.7z`
- `RAIS_VIC_PUB_NORTE.7z`
- `RAIS_VIC_PUB_SP.7z`
- `RAIS_VIC_PUB_SUL.7z`

---

## 5. Particularidades da fonte (sistema novo, 2023 em diante)

> **Nota oficial do MTE**

A partir do ano-base 2024, o MTE implementou uma nova solução tecnológica, resultando em alterações estruturais nos arquivos disponibilizados.

Principais mudanças:

### 5.1 Formato dos arquivos

Os dois formatos são texto delimitado e abrem nos mesmos softwares, mas **não são
intercambiáveis**: mudam a extensão, o delimitador e os nomes das colunas.

| | Sistema antigo | Sistema novo |
| :--- | :--- | :--- |
| Extensão | `.txt` | `.comt` |
| Delimitador | **ponto e vírgula** | **vírgula** |
| Campos | sem aspas | entre aspas |
| Cabeçalhos | `CNAE 2.0 Subclasse` | `CNAE 2.0 Subclasse - Codigo` |
| Colunas (vínculos) | 60 | 62 |

O delimitador foi verificado ano a ano de 2012 a 2022: todos usam ponto e vírgula,
sem nenhuma vírgula no cabeçalho.

**A fronteira é 2023, não 2024.** Embora a mudança tenha sido anunciada para o
ano-base 2024, a fonte **republicou 2023** sob o sistema novo em 18/05/2026 e moveu
o arquivo antigo para `2023/Legado/`. O que está hoje no FTP para 2023 é o formato
novo; 2022 e anteriores seguem no antigo.

O sistema antigo também não é um formato só. A partição dos arquivos e a contagem
de colunas mudam ao longo da série:

| Anos | Arquivos | Colunas |
| :--- | :--- | ---: |
| 2012–2014 | um por UF (`AP2012.7z`, `BA2012.7z`, …) | 45 |
| 2015 | um por UF | 57 |
| 2016 | um por UF | 58 |
| 2017 | um por UF | 60 |
| 2018–2022 | nacional (`RAIS_ESTAB_PUB.7z`) | 24 |

> **O crawler só lê o formato novo.** O `pd.read_csv` em `tasks.py` fixa
> `sep=","`, então um arquivo do sistema antigo é lido como uma coluna só; e o
> caminho de download é montado a partir de `ESTAB_FILE`, que não existe nos anos
> divididos por UF. Processar qualquer ano até 2022 exige detectar o delimitador e
> montar a lista de arquivos, não apenas trocar a extensão. Os anos antigos que
> estão na tabela foram carregados em 2024, por outro código, antes deste crawler
> existir (ele nasceu em #1510, maio de 2026).

### 5.2 Dicionário de dados

- Alterações na nomenclatura e na formatação de determinadas variáveis.
- Recomenda-se validar o schema a cada nova versão anual da base.

---

## 6. Mudanças e particularidades do schema

### 6.1 Coluna `cnae_1`

O teste de relacionamento com a coluna `cnae_1` foi removido das tabelas de Estabelecimentos e Vínculos.

**Motivo:**

O diretório `basedosdados.br_bd_diretorios_brasil.cnae_1:classe` está estruturado conforme a classificação oficial da [CONCLA](https://concla.ibge.gov.br/busca-online-cnae.html?view=estrutura), cuja hierarquia e códigos seguem o padrão divulgado pelo IBGE.

Entretanto, nos microdados oficiais da RAIS (tabelas de Vínculos e Estabelecimentos) observamos que, a partir dos dados definitivos de 2023, a coluna cnae_1 passou a apresentar códigos com 4 caracteres. Nos anos anteriores, bem como no diretório da Base dos Dados (BD), o padrão adotado era de 5 caracteres.

Essa alteração quebra a compatibilidade estrutural entre as bases, impedindo a validação do relacionamento segundo os critérios historicamente utilizados. Dado que a inconsistência se origina na fonte oficial, entendemos tratar-se de um problema upstream que inviabiliza a validação do relacionamento conforme os critérios anteriormente adotados.

> Hipóteses:
> 1. Testamos a padronização dos códigos por meio da adição de zero à esquerda nos registros com 4 caracteres, com o objetivo de restabelecer o padrão de 5 dígitos adotado no diretório `basedosdados.br_bd_diretorios_brasil.cnae_1:classe`, conforme a estrutura definida pela CONCLA. A estratégia resultou em compatibilização apenas parcial: Para o ano de 2023, apenas 4 códigos passaram a coincidir com o diretório, enquanto outros permaneceram sem correspondência válida. Segue os códigos abaixo:

Para verificar a porcentagem de cruzamento, rode o seguinte comando no BigQuery:

```sql
WITH child AS (
    SELECT DISTINCT cnae_1
    FROM `basedosdados-dev.br_me_rais.microdados_estabelecimentos`
    WHERE
        cnae_1 IS NOT NULL
        AND ano = 2023
),

parent AS (
    SELECT DISTINCT cnae_1 AS parent_value
    FROM `basedosdados-dev.br_bd_diretorios_brasil.cnae_1`
)

SELECT
    COUNTIF(p1.parent_value IS NOT NULL) AS matches_original,
    COUNTIF(p2.parent_value IS NOT NULL) AS matches_lpad,
    COUNT(*) AS total_registros,
    ROUND(
        COUNTIF(p2.parent_value IS NOT NULL) / COUNT(*) * 100,
        3
    ) AS percentage_lpad
FROM child c
LEFT JOIN parent p1
    ON c.cnae_1 = p1.parent_value
LEFT JOIN parent p2
    ON LPAD(c.cnae_1, 5, '0') = p2.parent_value;

```
Resultado:
| Total de CNAEs distintos (RAIS 2023) | CNAEs que cruzaram após LPAD | Percentual de cruzamento |
| :--- | :---: | ---: |
| 535 | 4 | 0.748 |

---

Para verificar quais códigos foram cruzados, rode o seguinte comando no BigQuery:

```sql
WITH child AS (
    SELECT DISTINCT cnae_1
    FROM `basedosdados-dev.br_me_rais.microdados_estabelecimentos`
    WHERE
        cnae_1
        cnae_1 IS NOT NULL
        AND ano = 2023
),

parent AS (
    SELECT DISTINCT cnae_1 AS parent_value
    FROM `basedosdados-dev.br_bd_diretorios_brasil.cnae_1`
)

SELECT
    distinct c.cnae_1 AS cnae_original,
    LPAD(c.cnae_1, 5, '0') AS cnae_padronizado
FROM child c
LEFT JOIN parent p1
    ON c.cnae_1 = p1.parent_value
JOIN parent p2
    ON LPAD(c.cnae_1, 5, '0') = p2.parent_value
WHERE p1.parent_value IS NULL
ORDER BY 1;
```

| cnae_original (RAIS 2023) | CNAEs que cruzaram após LPAD
| :--- | :---:
| 1120 | 01120 |
| 1325 | 01325 |
| 1422 | 01422 |
| 5118 | 05118 |

----

### 6.2 Coluna `cnae_2_subclasse`

Em relação à coluna cnae_2_subclasse, a partir de 2023 observou-se inconsistência no tamanho do código: parte dos registros passou a apresentar 6 dígitos, enquanto outros mantiveram 7 dígitos. Para padronização, aplicamos left padding com zero à esquerda nos códigos de 6 dígitos, garantindo que todos passem a ter 7 dígitos. Com essa normalização, os valores tornam-se compatíveis com o diretório `br_bd_diretorios_brasil.cnae:subclasse`.

**Recomendação:**
Para análises e relacionamentos, utilizar as colunas:

- `cnae_2_subclasse`

#### O cabeçalho `Codigo` sem acento (leia antes de mexer nesta coluna)

Nos arquivos de 2024 e 2025 a fonte escreve o cabeçalho desta coluna como
**`CNAE 2.0 Subclasse - Codigo`, sem acento** — e é a única coluna do arquivo
assim; todas as outras usam `Código`. Se o dicionário de rename só contemplar a
versão acentuada, a coluna não é reconhecida, o crawler a emite vazia e a tabela
final sai com `cnae_2_subclasse` **e** `cnae_2` 100% nulas (o modelo deriva a
classe da subclasse, então as duas caem juntas).

Isso já aconteceu duas vezes:

| Quando | O quê |
| :--- | :--- |
| 05/2026 | Diagnosticado em vínculos 2025 e corrigido em `VINCULOS_RENAME` (#1557) |
| 06/2026 | A correção se perdeu ao migrar o arquivo de `pipelines/datasets/` para `pipelines/crawler/` |
| 08/2026 | Reportado de novo, agora em estabelecimentos 2023 e 2025 — 25 milhões de linhas publicadas com a coluna nula |

Duas defesas foram acrescentadas depois disso:

- `_fill_absent_columns` em `tasks.py` **levanta erro** quando uma coluna some
  no rename, em vez de preencher vazio em silêncio. Só as colunas listadas em
  `ESTAB_ABSENT_IN_SOURCE` / `VINCULOS_ABSENT_IN_SOURCE` são toleradas.
- O teste `not_null_proportion_multiple_columns` no `schema.yml`, **escopado ao
  ano mais recente** (`where: __most_recent_year__`). O escopo é essencial: um
  vazio de um ano se dilui na série inteira — com 2023 e 2025 zerados o total
  ainda dava 72% preenchido, bem acima do piso de 5%.

Os códigos também vêm com **espaço à esquerda** (`" 5611203"`). O crawler aplica
`strip` em todas as colunas antes da limpeza de códigos inválidos; sem isso, um
`" 0000000"` não casa com a lista de inválidos e o `lpad` do modelo enxerga 8
caracteres em vez de 7.

Por fim, note que a coluna rotulada `CNAE 2.0 Classe - Código` **não contém
classe**: a fonte repete ali o mesmo código de 7 dígitos da subclasse. O modelo
ignora essa coluna do staging e deriva `cnae_2` via `left(cnae_2_subclasse, 5)`.

### 6.3 Coluna `tamanho_estabelecimento`

Até o ano de 2001 (inclusive), os códigos da coluna `tamanho_estabelecimento` seguiam uma numeração iniciada em 0, enquanto nos anos posteriores a numeração inicia em 1. Para padronizar o dicionário ao longo de toda a série histórica, os valores dos anos até 2001 foram incrementados em 1 quando o código original estava entre 0 e 9.

### 6.4 Comentários gerais:

**Comentários:**
  • Alguns valores relacionados à conexão com o diretório foram desconsiderados durante os testes.
  • Os códigos de cbo_2002 foram ignorados devido à descontinuidade de parte deles, conforme descrito no documento oficial (https://portalfat.mte.gov.br/wp-content/uploads/2016/04/CBO2002_Liv3.pdf).
  • A variável cnae_2_subclasse apresenta códigos que não existem oficialmente na documentação dos cnae, por isso, não são compatíveis com o diretório e portanto, ignorado nos testes.

### 6.5 Colunas sem correspondência na fonte

O rename é um dicionário que traduz cabeçalho da fonte para nome de coluna nossa.
Quando os dois lados deixam de casar, aparecem **dois tipos de lacuna**, e elas
não são idênticas:

| Lacuna | O que acontece | Como se manifesta |
| :--- | :--- | :--- |
| Coluna esperada que nenhum cabeçalho alimenta | A coluna sai vazia | Visível: dá coluna nula na tabela publicada, e desde a correção do §6.2 o crawler **interrompe** o processamento |
| Cabeçalho publicado que o rename não conhece | O dado é descartado na leitura | **Invisível:** nada na tabela indica que a variável existia |

A segunda é a perigosa. Nada no código olha para ela — `_fill_absent_columns`
verifica apenas a primeira direção, e nenhum teste do dbt pode acusar a falta de
uma coluna que ninguém declarou. Ela só aparece quando alguém compara o cabeçalho
do arquivo com o dicionário, na mão.

#### Colunas esperadas que a fonte não entrega

Estas estão declaradas em `ESTAB_ABSENT_IN_SOURCE` / `VINCULOS_ABSENT_IN_SOURCE`,
emitidas vazias de propósito e ignoradas no teste de proporção de nulos:

| Tabela | Coluna | Situação |
| :--- | :--- | :--- |
| estabelecimentos | `natureza_estabelecimento` | Referente apenas ao ano de 1994 |
| estabelecimentos | `subatividade_ibge` | Preenchida em 1985 (~87%), zerada de 2000 em diante |
| vínculos | `tipo_salario` | Não existe no arquivo de 2025 |
| vínculos | `valor_salario_contratual` | Não existe no arquivo de 2025 |
| vínculos | `subatividade_ibge` | Não existe no arquivo de 2025 (só `IBGE Subsetor`) |
| vínculos | `cbo_1994` | Não existe no arquivo de 2025 (substituída por `CBO 2002 Ocupação`) |
| vínculos | `grau_instrucao_1985_2005` | Não existe no arquivo de 2025 (substituída por `Escolaridade Após 2005`) |

As cinco de vínculos foram verificadas contra o cabeçalho do arquivo de 2025:
**nenhuma delas existe na fonte**. O arquivo traz 62 colunas, e a mais próxima de
cada uma é uma substituta, não a mesma variável — `CBO 2002 Ocupação` no lugar do
`cbo_1994`, `Escolaridade Após 2005` no lugar do `grau_instrucao_1985_2005`,
`IBGE Subsetor` sem a subatividade correspondente. Não há nada a recuperar; o
`ignore_values` do teste documenta um limite real da fonte.

#### Como refazer essa verificação

Vale repetir a cada ano novo, porque a fonte muda os cabeçalhos sem avisar (§6.2).
O arquivo `RAIS_VINC_PUB_NI.7z` serve bem: tem o mesmo cabeçalho dos demais e
apenas algumas centenas de KB.

```bash
curl -O ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/<ano>/RAIS_VINC_PUB_NI.7z
7z e -so RAIS_VINC_PUB_NI.7z | head -1 | iconv -f latin1 -t utf-8 | tr ',' '\n'
```

A verificação tem **duas direções**, e a segunda é a que costuma ser esquecida:

- *esperadas e ausentes* — colunas de `*_VARS` que nenhum cabeçalho alimenta;
- *na fonte e não mapeadas* — cabeçalhos publicados que o rename descarta.

#### Cabeçalhos publicados que o rename não consumia

A verificação na segunda direção encontrou dois em vínculos:

| Cabeçalho na fonte | Situação |
| :--- | :--- |
| `Tipo Estabelecimento - Nome` | Redundante — é o rótulo da variável já capturada por `Tipo Estabelecimento - Código`. Ignorado de propósito |
| `Categoria Trabalhador - Código` | Estava sendo descartado. **Recuperado** — ver abaixo |

#### `categoria_trabalhador`

Categoria do trabalhador do eSocial, publicada pela fonte a partir de 2023.
Classificação mais granular que `tipo_vinculo`, agrupada por faixa: 1xx
empregados, 2xx sem vínculo de emprego, 3xx servidores públicos, 4xx, 7xx
contribuintes individuais. Os rótulos estão na Tabela 01 do eSocial.

Não é derivável de `tipo_vinculo`. Em amostra de 112 mil vínculos de 2025, um
mesmo `tipo_vinculo` comporta até 5 categorias e uma mesma categoria aparece em
até 8 tipos. O `tipo_vinculo = 10` (CLT por prazo indeterminado) contém as
categorias 101 (empregado geral), 111 (doméstico) e 301 (servidor estatutário).

O parâmetro `has_categoria_trabalhador` do macro está desligado e a coluna não é
emitida. Ligá-lo antes de a coluna existir na staging `microdados_vinculos_2023`
faz o `dbt run` falhar com `Unrecognized name: categoria_trabalhador`. Para
habilitar:

1. reprocessar vínculos de 2023 em diante, com o `VINCULOS_RENAME` já corrigido;
2. confirmar a coluna na staging;
3. ligar `has_categoria_trabalhador=true` na CTE `from_2023`;
4. registrar a coluna na API;
5. acrescentar `categoria_trabalhador` ao `ignore_values` do teste de proporção
   enquanto os anos anteriores a 2023 seguirem nulos.

O modelo de vínculos é incremental e fixa `on_schema_change="append_new_columns"`,
que absorve a coluna quando ela aparecer; o padrão do dbt é `ignore`, que a
descartaria sem erro.

Em estabelecimentos a mesma verificação não achou nenhum descarte: depois da
correção do §6.2, o crawler captura tudo que o arquivo de 2025 publica.

### 6.6 Coluna `id_municipio_trabalho`

Desde 2023 a fonte envia `999999` no município de trabalho, o código de "não
informado". O crawler converte o código IBGE de 6 dígitos no de 7 por um `merge`
com `br_bd_diretorios_brasil.municipio` (`tasks.py`); `999999` não existe no
diretório, não acha par, e a coluna sai vazia.

| Ano | Linhas | Preenchida |
| :--- | ---: | ---: |
| 2021 | 211,6 M | 71,1% |
| 2022 | 235,5 M | 95,2% |
| 2023 | 83,0 M | 0,36% |
| 2024 | 87,7 M | 0,39% |
| 2025 | 91,7 M | 0,28% |

O dado não existe na origem a partir de 2023. A coluna está no `ignore_values` do
teste de proporção de nulos.

O `merge` mapeia para nulo tanto o `999999` quanto a ausência da coluna, então a
tabela não distingue os dois casos.

### 6.7 Códigos de `motivo_desligamento` sem rótulo

Seis códigos aparecem a partir de 2025 e não constam do dicionário da RAIS. Somam
~100 mil linhas em 91,7 milhões (0,11%):

| Código | Linhas em 2025 | Família pela dezena |
| :--- | ---: | :--- |
| 81 | 86.745 | Aposentadoria |
| 82 | 5.965 | Aposentadoria |
| 24 | 3.555 | Iniciativa do empregado |
| 35 | 1.667 | Transferência / cessão |
| 65 | 1.428 | Falecimento |
| 36 | 896 | Transferência / cessão |

A família na terceira coluna é inferida do agrupamento por dezena que a RAIS usa
(1x empregador, 2x empregado, 3x transferência, 6x falecimento, 7x-8x
aposentadoria, 9x acordo), não do rótulo oficial.

O vocabulário de `motivo_desligamento` é o da própria RAIS, não a Tabela 19 do
eSocial, que vai de 01 a 46 e não contém 65, 81 nem 82.

Os seis estão registrados no `br_me_rais__dicionario.sql` como `Código não
encontrado nos dicionários oficiais.`, mesmo tratamento dos códigos 1-9, 89 e 99.

---

## 7. Observações sobre a divulgação dos dados

A RAIS é divulgada duas vezes ao ano:

- **Divulgação parcial:** setembro
- **Divulgação completa:** início do ano seguinte

Entre essas divulgações, o último ano da série apresenta subcobertura.

**Exemplo:**
Em novembro de 2025, o ano de 2024 apresenta aproximadamente 46 milhões de vínculos, enquanto 2022 e 2023 ultrapassam 50 milhões.

> **Importante**
> Essa diferença não indica queda no número de vínculos, mas sim que os dados do ano mais recente ainda não foram totalmente disponibilizados.

### 7.1 O calendário acima não descreve o que está no FTP

Na prática a fonte também **republica anos antigos**, sem aviso, e o diretório
não segue o calendário. Estado observado em 08/2026:

| Pasta no FTP | `RAIS_ESTAB_PUB.7z` | Publicado em |
| :--- | ---: | :--- |
| `2023/Legado/` | 120.943.063 | 30/12/2025 |
| `2023/` | 127.348.274 | 18/05/2026 |
| `2024 Parcial/` | 136.902.586 | 25/09/2025 |
| `2024/` | 140.661.991 | 18/05/2026 |
| `2025/` | 143.766.046 | 13/05/2026 |

Duas consequências, ambas já observadas:

- **2023 foi reprocessado pela fonte sob o sistema novo** (o arquivo antigo foi
  movido para `Legado/`). É isso que explica o salto de ~8,45 milhões de
  estabelecimentos em 2022 para ~11,77 milhões em 2023 — mudança de universo na
  origem, não erro nosso.
- **Verifique de qual arquivo veio cada ano do staging.** Um ano baixado antes de
  18/05/2026 pode ser a divulgação parcial, e o definitivo já estar no ar. Os
  cabeçalhos ajudam a datar: os arquivos de 2024 e 2025 publicados em maio/2026
  trazem `CNAE 2.0 Subclasse - Codigo` sem acento (ver §6.2).

Os `Layouts/` publicados no FTP só vão até 2019 e não servem para os anos
recentes; para conferir o schema, extraia a primeira linha do próprio `.7z`.

## 8. Verificação

**Observação: Recomendamos fortemente que se utilize a plataforma Dardo (https://bi.mte.gov.br/bgcaged/) para fazer a verificação dos dados antes de leva-lo para produção.**

> **Importante**
> O Dardo é uma plataforma do Governo, onde você conseguimos validar nossos dados (https://acesso.mte.gov.br/portal-pdet/o-pdet/portifolio-de-produtos/bases-de-dados.htm)

## 9. Materialização

- Quando for atualizar os dados definitivos da RAIS, aconselhamos a adicionar a seguinte estratégia incremental: `incremental_strategy="insert_overwrite` nas configs do dbt, uma vez que ela irá subrescrever os dados existentes na tabela com os novos dados definitivos da RAIS. Para maiores informações, leia: https://docs.getdbt.com/docs/build/incremental-strategy e https://downloads.apache.org/spark/docs/3.1.1/sql-ref-syntax-dml-insert-overwrite-table.html

### 9.1 Correção em ano antigo exige `--full-refresh`

O modelo de estabelecimentos filtra `where safe_cast(ano as int64) > 2022` no
modo incremental. Um `dbt run` comum, portanto, **só reescreve 2023 em diante** —
qualquer correção que mire anos anteriores fica no repositório sem entrar nos
dados, e nada avisa. Foi o que aconteceu com a padronização de
`tamanho_estabelecimento` (#1580, 08/2026), que mira os anos até 2001.

Para conferir se uma faixa foi de fato reescrita, olhe a data de cada partição —
não a data do commit:

```sql
SELECT partition_id, total_rows, last_modified_time
FROM `basedosdados.br_me_rais.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'microdados_estabelecimentos'
ORDER BY partition_id DESC;
```

Em produção o full-refresh sai pelo deployment "BD template: Executa DBT model",
passando o flag — não precisa de credencial local. Antes de disparar, compare as
contagens por ano entre o staging e a tabela: o staging pode ter sido reescrito
em bloco sem que a tabela tenha absorvido os anos antigos.

### 9.2 Partições fora do range

O `range` do `partition_by` tem `end` **exclusivo**. Quando ele ficou em `2024`,
todos os anos de 2023 em diante caíram numa única gaveta `__UNPARTITIONED__` —
38 milhões de linhas sem poda de partição, e todo filtro por ano recente varria
as três faixas. Mantenha `end` em pelo menos o último ano + 5.

Note que dbt **não altera o particionamento de uma tabela incremental já
existente**: corrigir o `range` no arquivo só tem efeito num `--full-refresh`.
Até lá o arquivo e a tabela divergem silenciosamente.

---

## 10. Arquitetura do modelo de vínculos

### 10.1 Dois staging tables, uma tabela final

A tabela `microdados_vinculos` é materializada a partir da união de dois staging tables distintos:

| Staging table | Anos cobertos | Observação |
| :--- | :---: | :--- |
| `br_me_rais_staging.microdados_vinculos` | até 2022 | Schema original |
| `br_me_rais_staging.microdados_vinculos_2023` | 2023 em diante | Schema estendido com `indicador_vinculo_abandonado` |

A separação existe porque a partir de 2023 o MTE adicionou a coluna `indicador_vinculo_abandonado`, que não está presente nos arquivos anteriores. O modelo dbt usa uma CTE por staging table e as une via `UNION ALL`:

```sql
with
    pre_2023 as ({{ vinculos_select("br_me_rais_staging.microdados_vinculos") }}),
    from_2023 as ({{ vinculos_select("br_me_rais_staging.microdados_vinculos_2023", has_vinculo_abandonado=true) }})

select * from pre_2023
union all
select * from from_2023
```

Para os anos anteriores a 2023, a coluna `indicador_vinculo_abandonado` é emitida como `cast(null as string)` a fim de manter o schema uniforme na tabela final.

### 10.2 Macro `vinculos_select` (`macros/br_me_rais_vinculos_select.sql`)

Toda a lógica de seleção e normalização de colunas está centralizada no macro `vinculos_select(source_table, has_vinculo_abandonado=false)`. O uso de um macro evita duplicação de código entre as duas CTEs e facilita a inclusão de novos anos com schemas distintos.

O parâmetro `has_vinculo_abandonado` controla se a coluna é lida do staging table ou substituída por `null`:

```sql
{% if has_vinculo_abandonado %}
    safe_cast(indicador_vinculo_abandonado as string) indicador_vinculo_abandonado,
{% else %}
    cast(null as string) as indicador_vinculo_abandonado,
{% endif %}
```

Se futuramente novos anos introduzirem outras colunas adicionais, o mesmo padrão deve ser aplicado: adicionar um parâmetro booleano ao macro e condicionar a expressão SQL correspondente.

### 10.3 Normalização de CNAE no macro

O macro aplica as mesmas normalizações de CNAE descritas nas seções 6.1 e 6.2:

- **`cnae_1`**: LEFT JOIN contra `basedosdados.br_bd_diretorios_brasil.cnae_1` para corrigir códigos de 4 dígitos introduzidos em 2023/2024. O `coalesce` garante que anos anteriores (onde o código já tem 5 dígitos e não cruza com o JOIN) mantenham o valor original.
- **`cnae_2`**: derivado diretamente de `cnae_2_subclasse` após padding — `left(lpad(cnae_2_subclasse, 7, '0'), 5)`.
- **`cnae_2_subclasse`**: normalizado para 7 dígitos via `lpad(cnae_2_subclasse, 7, '0')`.
