"""Constants for br_cvm_administradores_carteira."""

from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
)

DATASET_ID = "br_cvm_administradores_carteira"

RESPONSAVEL_TABLE_ID = "responsavel"
PESSOA_FISICA_TABLE_ID = "pessoa_fisica"
PESSOA_JURIDICA_TABLE_ID = "pessoa_juridica"

# `responsavel` só tem cnpj/nome/tipo, sem coluna de data — mesma cobertura
# que o flow antigo usava de fato (nunca chamava
# register_table_materialization_task pra essa tabela).
RESPONSAVEL_COVERAGE = NonHistorical()

# pessoa_fisica/pessoa_juridica têm `data_registro` — mesma coverage que o
# flow antigo já registrava via register_table_materialization_task.
PESSOA_FISICA_COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_registro"),
    date_format=DateFormat.YEAR_MD,
)
PESSOA_JURIDICA_COVERAGE = PartBdpro(
    date_column=DateOnly(col="data_registro"),
    date_format=DateFormat.YEAR_MD,
)
