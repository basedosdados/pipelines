
### Relatório Fases 2 e 3
#### Queries para verificação de padrões

1. DDD (ddd_1)
- Os valores que falhamvêm com zero à esquerda: 0 (2,5 milhões), 011, 021, 031 (3 dígitos) e 0000/0011 (4 dígitos).
- ddd_2 e ddd_fax não foram rodados ainda.

2. Telefone (telefone_1)
- 0 com 1 dígito: 2,67 milhões. (É o placeholder da fonte para "sem telefone"?)
- 8 caracteres com espaço (621 1399,        0): 7.787 linhas.
- 3, 4, 5 ou 2 dígitos só com números (0000, 00000, 99): cerca de 6 mil linhas no total.
- 7 caracteres com separador (341-500): 146 linhas.

3. CEP
- 82.085 linhas com 7 dígitos numéricos (9234070). O zero à esquerda se perdeu em algum ponto do pipeline.
- 2.912 linhas com        0 (espaços e zero).
- 2 linhas com 0.

4. cpf_representante_legal
- Só existem dois padrões: nulo (27,7 milhões) e ***999999** (761.727 linhas).
- O regex ^\*{3}[0-9]{6}\*{2}$ cobre tudo.

5. socios.documento por tipo
- tipo=1: 8 dígitos (99999999), não 14. O documento de pessoa jurídica é o cnpj_basico, e o plano dizia "CNPJ de 14 dígitos" (Erro já mapeado e originário da fonte)
- tipo=2: sempre ***999999** (27,7 milhões de linhas).
- tipo=3: sempre nulo (12.806 linhas). O plano deixava esse tipo livre, mas o dado mostra que ele é sempre nulo.

6. E-mail
- A maioria é texto digitado de forma errada: dois e-mails na mesma célula, @ faltando (hotmail com), espaços, 0 e xxxxx.
- Os 38.897 avisos refletem isso. Deve ficar como warn.

7. id_municipio nulo
- Os 622 nulos são todos de MT.

Mudanças que isso sugere nos testes
- DDD, telefone e CEP: sugiro um padrão mais flexível, algo que aceite o placeholder 0 e zeros à esquerda, ou tratar o formato no modelo sql. Preciso da sua decisão.
- cpf_representante_legal: adicionar o regex ^\*{3}[0-9]{6}\*{2}$.
- socios.documento: customizar o teste por tipo?

