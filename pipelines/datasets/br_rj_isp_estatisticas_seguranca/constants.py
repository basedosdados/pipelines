# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br_isp_estatisticas_seguranca project
    """

    # datasets raw name

    EVOLUCAO_MENSAL_CISP = "BaseDPEvolucaoMensalCisp.csv"
    EVOLUCAO_MENSAL_UF = "DOMensalEstadoDesde1991.csv"
    TAXA_EVOLUCAO_MENSAL_UF = "BaseEstadoTaxaMes.csv"
    EVOLUCAO_MENSAL_MUNICIPIO = "BaseMunicipioMensal.csv"
    TAXA_EVOLUCAO_MENSAL_MUNICIPIO = "BaseMunicipioTaxaMes.csv"
    ARMAS_APREENDIDADAS_MENSAL = "ArmasApreendidasEvolucaoCisp.xlsx"
    EVOLUCAO_POLICIAL_MORTO = "PoliciaisMortos.csv"
    FEMINICIDIO_MENSAL_CISP = "BaseFeminicidioEvolucaoMensalCisp.csv"

    # paths
    INPUT_PATH = "tmp/input/"
    OUTPUT_PATH = "tmp/output/"

    # urls =
    URL = "http://www.ispdados.rj.gov.br/Arquivos/"

    # build a dict that maps a table name to a architectura and
    # another dict that maps an original table name to a
    # trated table name


    dict_original = {
        "BaseDPEvolucaoMensalCisp.csv": "evolucao_mensal_cisp.csv",
        "DOMensalEstadoDesde1991.csv": "evolucao_mensal_uf.csv",
        "BaseEstadoTaxaMes.csv": "taxa_evolucao_mensal_uf.csv",
        "BaseMunicipioMensal.csv": "evolucao_mensal_municipio.csv",
        "BaseMunicipioTaxaMes.csv": "taxa_evolucao_mensal_municipio.csv",
        "ArmasApreendidasEvolucaoCisp.xlsx": "armas_apreendidas_mensal.csv",
        "PoliciaisMortos.csv": "evolucao_policial_morto_servico_mensal.csv",
        "BaseFeminicidioEvolucaoMensalCisp.csv": "feminicidio_mensal_cisp.csv"
    }


    dict_arquitetura = {
        "evolucao_mensal_cisp.csv": "https://docs.google.com/spreadsheets/d/1jibGPOYF6Tack3n9MmiQKdBagbeK-oLr/edit#gid=55379267",
        "evolucao_mensal_uf.csv": "https://docs.google.com/spreadsheets/d/1seN6LQ9WQnobVNpFw6KX5BMhYr0yyZa2/edit#gid=1349095453",
        "taxa_evolucao_mensal_uf.csv": "https://docs.google.com/spreadsheets/d/1fQ7MnfHm8vrlfdUhAYlU7F-CJouORKkc/edit#gid=1414191356",
        "taxa_evolucao_anual_uf.csv": "https://docs.google.com/spreadsheets/d/117EyqVw5a6_0PFPISjLZuZLIsRqyZiOo/edit#gid=588874333",
        "evolucao_mensal_municipio.csv": "https://docs.google.com/spreadsheets/d/1cHPcIBfmFwSxgMTsvGX-Ha-Efz7ZIDpn/edit#gid=347509921",
        "taxa_evolucao_mensal_municipio.csv": "https://docs.google.com/spreadsheets/d/1VKorutzmHUl71a2J--auChJm8tC-652i/edit#gid=199121203",
        "taxa_evolucao_anual_municipio.csv": "https://docs.google.com/spreadsheets/d/1LeH92JhPkr59NoUwepOgHKlsJIS6k-2W/edit#gid=786684819",
        "evolucao_mensal_upp.csv": "https://docs.google.com/spreadsheets/d/1TGG8T5xzmO_tzo9RScnIOt2NKjyIHu2O/edit#gid=1336604684",
        "armas_apreendidas_mensal.csv": "https://docs.google.com/spreadsheets/d/14wV3BkjG_9GDWKDUAbOVe2KOh0Bi6FAA/edit#gid=1673208544",
        "armas_fogo_apreendidas_mensal.csv": "https://docs.google.com/spreadsheets/d/19gynYMOxzfgjd7HsPjH4LbOkgV9844WSoVBiqAgp340/edit#gid=0",
        "evolucao_policial_morto_servico_mensal.csv": "https://docs.google.com/spreadsheets/d/1wuRr-I73jje0nkSeF_9LEJSpRmuwIftX/edit#gid=1573015202",
        "feminicidio_mensal_cisp.csv": "https://docs.google.com/spreadsheets/d/1DLb9GQAZR-TRbJp0YYc1w71OsEwXPYa8/edit#gid=1573015202",
        "taxa_letalidade.csv": "https://docs.google.com/spreadsheets/d/1wMbutt7Gs17ZlGEZ_-SBT4bwtF_QSWd5KLR-m8KaBMo/edit#gid=0",
    }
