# SAEB

Este documento descreve os testes de consistência aplicados sobre as bases do SAEB, com foco nas tabelas referentes aos alunos dos anos avaliados. O objetivo é registrar os problemas encontrados e fornecer contexto para futuras validações e aprimoramentos dos dados.

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
