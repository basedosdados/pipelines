"""
Constant values for br_cgu_servidores_executivo_federal.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_cgu_servidores_executivo_federal"

# As 7 tabelas do dataset — migradas pro pipeline em estágios (staged
# pipeline), substituindo o antigo `_run_cgu_servidores_publicos` monolítico
# (que segue existindo em `pipelines/shared/cgu/flows.py`, compartilhado com
# outros datasets br_cgu_* ainda não migrados). Cada tabela combina um ou
# mais subsistemas do Portal da Transparência (SIAPE, BACEN, Militares,
# DEFESA, Reserva/Reforma — ver README.md, seção "Tabelas e Subsistemas") e
# baixa de forma independente das outras — sem download/ref() compartilhado
# entre elas no dbt (`models/br_cgu_servidores_executivo_federal/*.sql`).
CADASTRO_APOSENTADOS_TABLE_ID = "cadastro_aposentados"
CADASTRO_PENSIONISTAS_TABLE_ID = "cadastro_pensionistas"
CADASTRO_SERVIDORES_TABLE_ID = "cadastro_servidores"
CADASTRO_RESERVA_REFORMA_MILITARES_TABLE_ID = (
    "cadastro_reserva_reforma_militares"
)
REMUNERACAO_TABLE_ID = "remuneracao"
AFASTAMENTOS_TABLE_ID = "afastamentos"
OBSERVACOES_TABLE_ID = "observacoes"

# Mesma coverage pras 7 tabelas — todas mensais (ano/mes), sem diferença de
# tier. Mesma granularidade do antigo `_part_bdpro_year_month("ano", "mes")`
# em `pipelines/shared/cgu/flows.py`.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
