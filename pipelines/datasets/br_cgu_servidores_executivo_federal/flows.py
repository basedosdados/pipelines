"""
Flows para br_cgu_servidores_executivo_federal — Prefect 3.

Cada flow corresponde a uma tabela do conjunto e combina um ou mais
pacotes/subsistemas do Portal da Transparência (SIAPE, BACEN, Militares,
DEFESA, Reserva/Reforma).

Disponibilidade tudo-ou-nada: o Portal publica os subsistemas em datas
escalonadas dentro do mês. O gate ``verify_all_url_exists_to_download`` exige
que todos os subsistemas da tabela estejam disponíveis antes de baixar; se
algum ainda não foi publicado, o flow encerra sem persistir e tenta novamente
no próximo run. Assim, um mês só é ingerido quando está completo.

Dependência de User-Agent: requisições sem User-Agent de browser são
bloqueadas pelo Portal com HTTP 405. Por isso as chamadas usam
``source_url_is_available``, que envia o UA e ainda trata o HTTP 202 (retornado
enquanto o ZIP é gerado de forma assíncrona).
"""

from prefect import flow

from pipelines.crawler.cgu.flows import _run_cgu_servidores_publicos


def _flow_factory(table_id: str, cron: str):
    @flow(
        name=f"br_cgu_servidores_executivo_federal__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cgu_servidores_executivo_federal",
        table_id: str = table_id,
        relative_month: int = 1,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_cgu_servidores_publicos(
            dataset_id=dataset_id,
            table_id=table_id,
            relative_month=relative_month,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_cgu_servidores_executivo_federal__cadastro_aposentados = _flow_factory(
    "cadastro_aposentados", "0 6 * * *"
)
br_cgu_servidores_executivo_federal__cadastro_pensionistas = _flow_factory(
    "cadastro_pensionistas", "15 6 * * *"
)
br_cgu_servidores_executivo_federal__cadastro_servidores = _flow_factory(
    "cadastro_servidores", "30 6 * * *"
)
br_cgu_servidores_executivo_federal__cadastro_reserva_reforma_militares = (
    _flow_factory("cadastro_reserva_reforma_militares", "45 6 * * *")
)
br_cgu_servidores_executivo_federal__remuneracao = _flow_factory(
    "remuneracao", "0 7 * * *"
)
br_cgu_servidores_executivo_federal__afastamentos = _flow_factory(
    "afastamentos", "15 7 * * *"
)
br_cgu_servidores_executivo_federal__observacoes = _flow_factory(
    "observacoes", "30 7 * * *"
)
