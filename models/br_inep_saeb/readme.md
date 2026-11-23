# SAEB

Este documento descreve a carga das tabelas `brasil`, `uf` e `municipio` e os testes de consistência aplicados sobre as tabelas do SAEB referentes aos alunos dos anos avaliados. O objetivo é registrar os problemas encontrados e fornecer contexto para futuras validações e aprimoramentos dos dados.

### Carga das tabelas `brasil`, `uf` e `municipio`

#### Fonte

As três tabelas vêm da planilha de resultados do SAEB que o INEP publica a cada edição. A da edição 2025 é <https://download.inep.gov.br/saeb/resultados/saeb_2025_brasil_estados_municipios_censitario.xlsx>.

A planilha tem as abas Dicionário, Brasil, Estados e Municípios. Cada script lê uma aba:

| Tabela | Aba | Script |
| --- | --- | --- |
| `brasil` | Brasil | `code/br_inep_saeb_brasil.py` |
| `uf` | Estados | `code/br_inep_saeb_uf.py` |
| `municipio` | Municípios | `code/br_inep_saeb_municipio.py` |

As abas Brasil e Estados têm a coluna `CAPITAL`, com linhas "Total" e "Capital". Os scripts guardam só as linhas "Total".

#### Séries, disciplinas e redes

A planilha traz só o 5º e o 9º ano do ensino fundamental e o ensino médio, nas disciplinas LP e MT. Valores da coluna `serie`:

| `serie` | Etapa |
| --- | --- |
| 5 | 5º ano do ensino fundamental |
| 9 | 9º ano do ensino fundamental |
| 12 | Ensino médio tradicional |
| 13 | Ensino médio integrado |
| 14 | Ensino médio, somando tradicional e integrado |

Na tabela `brasil`, o ano de 2023 não traz as redes `privada` nem `total - federal, estadual, municipal e privada`; o de 2025 traz.

#### Execução

A carga é manual; o conjunto não tem pipeline recorrente. Cada script baixa a planilha, transforma a aba, junta o ano novo ao histórico já carregado e substitui a staging em `basedosdados-dev`. Os scripts gravam em `input/br_inep_saeb/` e `output/br_inep_saeb/`, relativos ao diretório de execução; as duas pastas são ignoradas pelo git.

Para carregar uma edição nova:

1. Em cada um dos três scripts, troque a `url` e o valor fixo da coluna `ano`.
2. Da raiz do repositório, rode cada script, com `<tabela>` igual a `brasil`, `uf` ou `municipio`:

    ```bash
    uv run python models/br_inep_saeb/code/br_inep_saeb_<tabela>.py
    ```

Os dados chegam à produção no merge da PR com a label `table-approve`, que copia a staging de dev para prod e materializa as tabelas.

#### Duplicação do ano

O script junta o ano novo à tabela inteira já carregada, sem remover esse ano antes. A tabela de onde vem o histórico depende do script:

| Tabela | Histórico lido de |
| --- | --- |
| `brasil` | `basedosdados-dev` |
| `uf` | `basedosdados-dev` |
| `municipio` | `basedosdados` (produção) |

Rodar o mesmo script duas vezes, ou rodá-lo depois que o ano já chegou à tabela de onde ele lê, duplica o ano.

#### Tabelas de taxa de alfabetização

As tabelas `brasil_taxa_alfabetizacao` e `uf_taxa_alfabetizacao` não vêm dessa planilha, que não inclui o 2º ano. Elas vêm do arquivo `saeb_2021_brasil_estados_municipios_c_tx_alfabetizado.xlsx`, em <https://download.inep.gov.br/saeb/resultados/>. Esse endereço não tem arquivo equivalente de 2023 nem de 2025.

### Testes realizados

Foram avaliadas as seguintes tabelas:

- ``aluno_ef_2ano``

- ``aluno_ef_5ano``

- ``aluno_ef_9ano``

---

1. **Relação Município (Relationship: Município)**

    Observou-se que parte dos municípios presentes nas bases é mascarada. Como consequência, o campo de identificação municipal não corresponde ao código oficial do IBGE, inviabilizando a validação direta dessa chave.

---

2. **Chave Única (Primary Key)**

    As bases referentes aos anos mais antigos apresentam inconsistências na composição ou integridade da chave primária. Os anos com divergências identificadas foram: `1995, 1997, 1999, 2001, 2003, 2005`

    Para esses anos, não é possível garantir unicidade do registro com os identificadores disponibilizados.

---
3. **Não anulidade (Not-null)**

    Identificou-se alto percentual de valores ausentes em diversas variáveis, especialmente nas tabelas aluno_ef_5ano e aluno_ef_9ano. Algumas colunas exemplificativas com baixa taxa de preenchimento: `particula_trabalho_solidario`, `opiniao_teste`, `utiliza_biblioteca_externa`
