"""
Constant values for br_ans_beneficiario.
"""

from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth

DATASET_ID = "br_ans_beneficiario"

INFORMACAO_CONSOLIDADA_TABLE_ID = "informacao_consolidada"
INFORMACAO_CONSOLIDADA_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/"

COVERAGE = PartBdpro(
    date_column=YearMonth(year="ano", month="mes"),
    date_format=DateFormat.YEAR_MONTH,
)
