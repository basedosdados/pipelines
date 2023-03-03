# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bd_teste project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_bd_teste.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_bd_teste.utils import foo
# foo()
# ```
#
###############################################################################

import prefect

def log (msg):
    logger = prefect.context.get("logger")
    logger.info(msg)
