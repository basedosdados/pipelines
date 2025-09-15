# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_isp_estatisticas_seguranca project
    """

    dict_table = {
        "evolucao_mensal_cisp": {
            "name_table": "BaseDPEvolucaoMensalCisp.csv",  # Normal
            "sheets_name": "https://docs.google.com/spreadsheets/d/1jibGPOYF6Tack3n9MmiQKdBagbeK-oLr/edit#gid=55379267",
        },
        "evolucao_mensal_uf": {
            "name_table": "DOMensalEstadoDesde1991.csv",  # Normal
            "sheets_name": "https://docs.google.com/spreadsheets/d/1seN6LQ9WQnobVNpFw6KX5BMhYr0yyZa2/edit#gid=1349095453",
        },
        "evolucao_mensal_municipio": {  # Normal
            "name_table": "BaseMunicipioMensal.csv",
            "sheets_name": "https://docs.google.com/spreadsheets/d/1cHPcIBfmFwSxgMTsvGX-Ha-Efz7ZIDpn/edit#gid=347509921",
        },
        "armas_apreendidas_mensal": {  # Errado
            "name_table": "ArmasApreendidasEvolucaoCisp.csv",
            "sheets_name": "https://docs.google.com/spreadsheets/d/14wV3BkjG_9GDWKDUAbOVe2KOh0Bi6FAA/edit#gid=1673208544",
        },
        "evolucao_policial_morto_servico_mensal": {  # Normal
            "name_table": "PoliciaisMortos.csv",
            "sheets_name": "https://docs.google.com/spreadsheets/d/1wuRr-I73jje0nkSeF_9LEJSpRmuwIftX/edit#gid=1573015202",
        },
        "feminicidio_mensal_cisp": {  # Normal
            "name_table": "BaseFeminicidioEvolucaoMensalCisp.csv",
            "sheets_name": "https://docs.google.com/spreadsheets/d/1DLb9GQAZR-TRbJp0YYc1w71OsEwXPYa8/edit#gid=1573015202",
        },
    }

    INPUT_PATH = "tmp/input/"
    OUTPUT_PATH = "tmp/output/"

    URL = "http://www.ispdados.rj.gov.br/Arquivos/"


def QUERY(file_name):
    print(
        f"SELECT COUNT(*) AS total FROM `basedosdados-dev.br_rj_isp_estatisticas_seguranca.{file_name}`"
    )
    return f"SELECT COUNT(*) AS total FROM `basedosdados-dev.br_rj_isp_estatisticas_seguranca.{file_name}`"
