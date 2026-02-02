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
Nos dados oficiais de 2024, a coluna `cnae_1` passou a conter apenas 4 caracteres, enquanto nos anos anteriores e na base de diretórios da Base dos Dados (BD) o padrão era de 5 caracteres.

Essa divergência inviabiliza a validação do relacionamento conforme os critérios anteriormente adotados.

**Recomendação:**
Para análises e relacionamentos, utilizar as colunas:

- `cnae_2`
- `cnae_2_subclasse`

Essas classificações permanecem consistentes ao longo da série histórica.

### 6.2 Comentários gerais:

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

