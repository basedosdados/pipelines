# -*- coding: utf-8 -*-
"""
Constant values for the datasets projects
"""


from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the br-bcb-taxa-selic project
    """

    ARCHITECTURE_URL = {
        "taxa_selic": "https://docs.google.com/spreadsheets/d/13VF9dcVweBip5I5fEgoOF3fX5niXkFL7CDY0c-lPbro/edit#gid=0"
    }

    API_URL = {
        "taxa_selic": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"
    }
