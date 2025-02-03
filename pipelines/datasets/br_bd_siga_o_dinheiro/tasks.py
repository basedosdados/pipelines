# -*- coding: utf-8 -*-
from prefect import task


@task
def get_table_ids() -> tuple[str, str, str, str]:
    table_ids = (
        "eleicao_perfil_candidato_2024",
        "eleicao_prestacao_contas_candidato_origem_2024",
        "eleicao_prestacao_contas_candidato_2024",
        "eleicao_prestacao_contas_partido_2024",
    )

    return table_ids
