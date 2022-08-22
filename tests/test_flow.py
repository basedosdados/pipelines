# -*- coding: utf-8 -*-
from pipelines.utils.utils import run_local
from pipelines.datasets.br_fgv_igp.flows import br_fgv_igp_flow


def test_igpdi_flow():
    run_local(br_fgv_igp_flow, parameters=dict(teste=True))
