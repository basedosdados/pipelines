# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    URL = "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO"
    INPUT = "/tmp/input/"
    OUTPUT = "/tmp/output/"
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
