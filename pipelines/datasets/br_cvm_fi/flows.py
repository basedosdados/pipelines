"""
Flows for br_cvm_fi — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cvm.flows import _run_cvm_fi


def _cvm_fi_flow(table_id: str, cron: str, date_column_name: dict):
    @flow(
        name=f"br_cvm_fi__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cvm_fi",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        url: str | None = None,
    ) -> None:
        _run_cvm_fi(
            dataset_id=dataset_id,
            table_id=table_id,
            date_column_name=date_column_name,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            url=url,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


# As seis tabelas rodam de 10 em 10 minutos a partir das 17h para não disputarem
# slot no BigQuery no mesmo instante — a fonte é mensal, e o poll faz a run virar
# no-op nos dias em que a CVM não publicou nada.
br_cvm_fi__documentos_informe_diario = _cvm_fi_flow(
    table_id="documentos_informe_diario",
    cron="0 17 * * *",
    date_column_name={"date": "data_competencia"},
)
br_cvm_fi__documentos_carteiras_fundos_investimento = _cvm_fi_flow(
    table_id="documentos_carteiras_fundos_investimento",
    cron="10 17 * * *",
    date_column_name={"date": "data_competencia"},
)
br_cvm_fi__documentos_extratos_informacoes = _cvm_fi_flow(
    table_id="documentos_extratos_informacoes",
    cron="20 17 * * *",
    date_column_name={"date": "data_competencia"},
)
br_cvm_fi__documentos_balancete = _cvm_fi_flow(
    table_id="documentos_balancete",
    cron="30 17 * * *",
    date_column_name={"date": "data_competencia"},
)
br_cvm_fi__documentos_informacao_cadastral = _cvm_fi_flow(
    table_id="documentos_informacao_cadastral",
    cron="40 17 * * *",
    date_column_name={"date": "data_inicio_situacao"},
)
br_cvm_fi__documentos_perfil_mensal = _cvm_fi_flow(
    table_id="documentos_perfil_mensal",
    cron="50 17 * * *",
    date_column_name={"date": "data_competencia"},
)
