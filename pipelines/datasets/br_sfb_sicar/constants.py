# -*- coding: utf-8 -*-
"""
Constants for br_sfb_sicar
"""

from enum import Enum

class Constants(Enum): # pylint: disable=c0103

    INPUT_PATH = '/tmp/car/input'
    OUTPUT_PATH = '/tmp/car/output'

    UF_SIGLAS = [

        'SP','BA','MT','RS','MG',
        'AL', 'AP', 'AM',  'CE', 'DF', 'ES', 'GO', 'MA',
         'MS',
        'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
         'RO', 'RR', 'SC', 'AC', 'SE', 'TO'
    ]

