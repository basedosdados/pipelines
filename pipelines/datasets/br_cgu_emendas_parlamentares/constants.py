from enum import Enum

from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearOnly,
)

DATASET_ID = "br_cgu_emendas_parlamentares"

MICRODADOS_TABLE_ID = "microdados"

# Mesma coverage do flow monolítico antigo (`register_table_materialization_task`
# em `flows.py`): anual, com a parte recente BD pro.
COVERAGE = PartBdpro(
    date_column=YearOnly(col="ano_emenda"),
    date_format=DateFormat.YEAR,
    free_lag=FreeLag(unit="years", value=1),
)


class constants(Enum):
    URL = "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO"
    VALUES_FLOAT = [
        "Valor Empenhado",
        "Valor Liquidado",
        "Valor Pago",
        "Valor Restos A Pagar Inscritos",
        "Valor Restos A Pagar Cancelados",
        "Valor Restos A Pagar Pagos",
    ]
    QUERY = """SELECT
    (SELECT count(*) as total FROM `basedosdados.br_cgu_emendas_parlamentares.microdados`) AS total,
    (SELECT TIMESTAMP_MILLIS(creation_time) as last_modified_time
    FROM `basedosdados.br_cgu_emendas_parlamentares.__TABLES_SUMMARY__`
    WHERE table_id = 'microdados') AS last_modified_time;
    """
