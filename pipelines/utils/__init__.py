"""
Prefect flows for basedosdados project
"""
# ruff: noqa: F403

###############################################################################
# Automatically managed, please do not touch
###############################################################################
from pipelines.utils.dump_to_gcs.flows import *
from pipelines.utils.execute_dbt_model.flows import *

# from pipelines.utils.apply_architecture_to_dataframe.flows import *
from pipelines.utils.metadata.flows import *
from pipelines.utils.to_download.flows import *
from pipelines.utils.traceroute.flows import *
from pipelines.utils.transfer_files_to_prod.flows import *
