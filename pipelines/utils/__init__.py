# -*- coding: utf-8 -*-
"""
Prefect flows for basedosdados project
"""
###############################################################################
# Automatically managed, please do not touch
###############################################################################
from pipelines.utils.crawler_ibge_inflacao.flows import *
from pipelines.utils.dump_to_gcs.flows import *
from pipelines.utils.execute_dbt_model.flows import *
