"""
Constant values for br_cgu_beneficios_cidadao.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_cgu_beneficios_cidadao"

# As 3 tabelas ativas do dataset — cada uma com download independente (URL
# própria em `pipelines.crawler.cgu.constants.constants.TABELAS`), sem
# tabela compartilhando ZIP com outra. Os demais modelos em `models/
# br_cgu_beneficios_cidadao/` (auxilio_brasil, bolsa_familia_pagamento,
# auxilio_emergencial) não têm fonte de download configurada — fora de
# escopo aqui.
NOVO_BOLSA_FAMILIA_TABLE_ID = "novo_bolsa_familia"
GARANTIA_SAFRA_TABLE_ID = "garantia_safra"
BPC_TABLE_ID = "bpc"

# Quantos meses somar à última data registrada nos metadados pra chegar no
# período a baixar — mesmo valor do flow monolítico antigo
# (`_run_cgu_beneficios_cidadao`/`pipelines/crawler/cgu/flows.py`).
RELATIVE_MONTH = 1

# Formato em que `read_and_partition_beneficios_cidadao`
# (`pipelines/crawler/cgu/tasks.py`) grava cada tabela em disco — precisa
# casar com o que `upload_to_gcs` espera encontrar, senão `dump_header`
# estoura `FileNotFoundError` antes de qualquer escrita (issue #1677).
SOURCE_FORMAT = {
    NOVO_BOLSA_FAMILIA_TABLE_ID: "parquet",
    GARANTIA_SAFRA_TABLE_ID: "parquet",
    BPC_TABLE_ID: "csv",
}

# Coluna de ano/mês usada como cobertura de cada tabela — mesma convenção do
# antigo `dict_for_table()` (`pipelines/crawler/cgu/tasks.py`), repassada a
# `_part_bdpro_year_month()` em `pipelines/crawler/cgu/flows.py`.
COVERAGE = {
    NOVO_BOLSA_FAMILIA_TABLE_ID: PartBdpro(
        date_column=YearMonth(year="ano_competencia", month="mes_competencia"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    GARANTIA_SAFRA_TABLE_ID: PartBdpro(
        date_column=YearMonth(year="ano_referencia", month="mes_referencia"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    BPC_TABLE_ID: PartBdpro(
        date_column=YearMonth(year="ano_competencia", month="mes_competencia"),
        date_format=DateFormat.YEAR_MONTH,
    ),
}
