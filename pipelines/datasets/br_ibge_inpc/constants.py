"""
Constant values for br_ibge_inpc.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_ibge_inpc"

# As 4 tabelas do dataset — migradas pro pipeline em estágios (staged pipeline),
# substituindo o antigo `_inpc_flow`/`_run_ibge_inflacao` monolítico (removido;
# lógica compartilhada agora mora em `pipelines/utils/ibge_inflacao.py`).
MES_BRASIL_TABLE_ID = "mes_brasil"
MES_CATEGORIA_BRASIL_TABLE_ID = "mes_categoria_brasil"
MES_CATEGORIA_RM_TABLE_ID = "mes_categoria_rm"
MES_CATEGORIA_MUNICIPIO_TABLE_ID = "mes_categoria_municipio"

# Mesma coverage pras 4 tabelas — todas mensais, sem diferença de tier.
COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
