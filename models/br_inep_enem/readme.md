# ENEM

  Este documento descreve os testes de consistência aplicados sobre as bases do ENEM e relatar mudanças de organização na estrutura de dados a partir de 2024. O objetivo é registrar os problemas encontrados e fornecer contexto para futuras validações e aprimoramentos dos dados.

### Testes realizados

---
 * Sobre a tabela ``resultados``
**Relação id_escola (Relationship: ID Escola)**
O código real da escola foi substituído por uma máscara quando a instituição possui menos de 10 participantes no exame.  Como consequência, o campo de identificação da escola nem sempre corresponde ao código oficial, inviabilizando a validação direta dessa chave.

---
### Mudanças na organização dos dados

- A estrutura dos microdados mudou em 2024: o que antes era disponibilizado em uma base única passa a ser distribuído em diferentes tabelas, separando participantes (onde se encontra o questionário socioeconômico nos dados brutos) e resultados. Essa divisão reduz o potencial de cruzamentos identificáveis e segue o modelo oficial de disseminação do Inep.
- Aplicação de máscara para id_escola nas instituições com menos de 10 participantes no exame
- As informações de escola não estão mais no mesmo arquivo dos dados individuais dos participantes.
