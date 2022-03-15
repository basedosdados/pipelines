"""
Imports all flows for every project so we can register all of them.
"""
# pylint: disable=W0401,W0614,W0611
###############################################################################
# Automatically managed, please do not touch
###############################################################################
from pipelines.cvm import *
from pipelines.ibge import *
from pipelines.data_tweet.flows import *
