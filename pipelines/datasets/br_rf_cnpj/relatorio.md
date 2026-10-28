
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

8. data_inicio_atividade > data_situacao_cadastral (estabelecimentos) — PRELIMINAR
- O teste `expression_is_true` falhou com 1065 linhas na partição mais recente.
- Verificação no :
  - 10468291000362: situação 01 (nula) em 2008-11-12, início em 2024-09-24 (~5.800 dias).
  - 25266246000726 e 25266246001374: mesma empresa, ambas com situação 03 (suspensa) em 2016-07-22 e inícios em 2023-11-27 e 2024-08-01. A mesma data de situação em filiais abertas depois sugere que a Receita copiou a data da empresa/matriz.
  - 12920561001089 (situação 08, baixada, em 2023-12-31, início 2024-01-18) e 02511629003309 (baixada em 2026-06-01, início 2026-06-18): diferença de 17 a 18 dias, possivelmente baixa retroativa ou erro de digitação na origem.
- Comparação com o BigQuery (amostra de 15 CNPJs, os primeiros casos encontrados na leitura parcial do arquivo bruto; partição mais recente de `estabelecimentos`, uma consulta filtrada por CNPJ): as 15 linhas têm data da situação, data de início e diferença em dias IDÊNTICAS às do arquivo bruto. A única diferença é a representação do código de situação (`08` no arquivo, `8` no BigQuery), por causa do `cast ... as int64` do modelo.
- Mapeamento de colunas verificado no código: `COLUNAS_ESTABELECIMENTO` posiciona `data_situacao_cadastral` no índice 6 e `data_inicio_atividade` no índice 10, iguais ao layout da Receita. O restante do tratamento é só `to_datetime(format="%Y%m%d")` e `safe_cast(... as date)`. Nenhuma etapa troca ou altera as datas.
- Conclusão: nesta amostra a inversão vem da fonte, e não do pipeline. A amostra não é aleatória (são os primeiros casos encontrados), então não permite estimar a proporção de casos de cada tipo: 9 de 15 são baixadas (08), 3 são nulas (01), 2 suspensas (03) e 1 inapta (04). Não foi feita comparação das 1065 linhas.
- Encaminhamento sugerido: tratar como `simples`, com `custom_matching_rows_limit` e tolerância, em vez de corrigir o modelo. Taxa observada no dbt: 1065 em ~74M linhas (~1,4e-5). O limite ainda precisa ser decidido.
- Pendente: a contagem completa no arquivo bruto (o arquivo ainda estava sendo escrito quando foi lido e a análise foi interrompida para evitar custo).

Mudanças que isso sugere nos testes
- DDD, telefone e CEP: sugiro um padrão mais flexível, algo que aceite o placeholder 0 e zeros à esquerda, ou tratar o formato no modelo sql. Preciso da sua decisão.
- cpf_representante_legal: adicionar o regex ^\*{3}[0-9]{6}\*{2}$.
- socios.documento: customizar o teste por tipo?

arquivo bruto da Receita (`Estabelecimentos0`: coluna 7 = data da situação, coluna 11 = data de início). A leitura foi feita com o arquivo ainda sendo escrito, então os totais NÃO valem. Falta reexecutar com o arquivo completo. Os exemplos abaixo são linhas reais e completas