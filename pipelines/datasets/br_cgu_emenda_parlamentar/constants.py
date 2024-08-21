# -*- coding: utf-8 -*-
from enum import Enum

import numpy as np


class constants(Enum):  # pylint: disable=c0103
    URL = "https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/UNICO"
    INPUT = "tmp/input/"
    OUTPUT = "tmp/output/"
    VALUES_FLOAT = ['Valor Empenhado', 'Valor Liquidado', 'Valor Pago', 'Valor Restos A Pagar Inscritos', 'Valor Restos A Pagar Cancelados', 'Valor Restos A Pagar Pagos']