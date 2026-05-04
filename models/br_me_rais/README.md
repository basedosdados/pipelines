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

## 5. Particularidades da fonte (a partir de 2024)

> **Nota oficial do MTE**

A partir do ano-base 2024, o MTE implementou uma nova solução tecnológica, resultando em alterações estruturais nos arquivos disponibilizados.

Principais mudanças:

### 5.1 Formato e compactação a partir de 2024

- Após a descompactação, os dados são fornecidos com extensão `.comt`.
- Houve mudanças nos nomes das colunas a partir do ano de 2024.

O formato `.comt` é um arquivo de texto estruturado, equivalente ao padrão `.csv`, podendo ser lido pelos mesmos softwares estatísticos e ferramentas de processamento de dados anteriormente utilizados.

> Antes de 2024, os dados eram disponibilizados no formato `.txt`, também equivalente a `.csv`.

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

### 6.3 Comentários gerais:

**Comentários:**
  • Alguns valores relacionados à conexão com o diretório foram desconsiderados durante os testes.
  • Os códigos de cbo_2002 foram ignorados devido à descontinuidade de parte deles, conforme descrito no documento oficial (https://portalfat.mte.gov.br/wp-content/uploads/2016/04/CBO2002_Liv3.pdf).
  • A variável cnae_2_subclasse apresenta códigos que não existem oficialmente na documentação dos cnae, por isso, não são compatíveis com o diretório e portanto, ignorado nos testes.

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

## 8. Verificação

**Observação: Recomendamos fortemente que se utilize a plataforma Dardo (https://bi.mte.gov.br/bgcaged/) para fazer a verificação dos dados antes de leva-lo para produção.**

> **Importante**
> O Dardo é uma plataforma do Governo, onde você conseguimos validar nossos dados (https://acesso.mte.gov.br/portal-pdet/o-pdet/portifolio-de-produtos/bases-de-dados.htm)

## 9. Materialização

- Quando for atualizar os dados definitivos da RAIS, aconselhamos a adicionar a seguinte estratégia incremental: `incremental_strategy="insert_overwrite` nas configs do dbt, uma vez que ela irá subrescrever os dados existentes na tabela com os novos dados definitivos da RAIS. Para maiores informações, leia: https://docs.getdbt.com/docs/build/incremental-strategy e https://downloads.apache.org/spark/docs/3.1.1/sql-ref-syntax-dml-insert-overwrite-table.html
