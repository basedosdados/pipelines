"""
Flows para br_bcb_sicor — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.crawler.bcb.flows import _run_bcb_sicor
from pipelines.crawler.bcb.tasks import create_load_dictionary
from pipelines.utils.flow import flow
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _sicor_flow(
    table_id: str,
    cron: str,
    dump_mode: str = "overwrite",
    source_format: str = "parquet",
    coverage_type: str = "part_bdpro",
    historical_database: bool = True,
):
    @flow(
        name=f"br_bcb_sicor__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_bcb_sicor",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        download_all_files: bool = False,
        local_redis_execution: bool = False,
    ) -> None:
        _run_bcb_sicor(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            dump_mode=dump_mode,
            source_format=source_format,
            coverage_type=coverage_type,
            historical_database=historical_database,
            download_all_files=download_all_files,
            local_redis_execution=local_redis_execution,
        )

    _flow.deploy_schedules = [Cron(cron, timezone="America/Sao_Paulo")]
    return _flow


# As três tabelas abaixo usam `dump_mode="append"` porque a fonte divulga um
# arquivo por ano e o histórico é acumulado na staging — trocar para "overwrite"
# apagaria os anos anteriores, já que o flow baixa apenas o ano corrente.
#
# Consequência: a tabela de staging é criada uma única vez e seu schema não é
# recriado nos runs seguintes. Quando o BCB adiciona uma coluna (como o
# `IB_RENEGOCIADA` em 2026), `_sync_staging_schema` a acrescenta à definição da
# tabela externa durante o upload. Registrar a coluna em `constants.py`, no
# `.sql` e no `schema.yml` continua sendo manual — ver a seção
# `br_bcb_sicor__saldo` do README para a cobertura da coluna e seus testes.
br_bcb_sicor__operacao = _sicor_flow(
    table_id="operacao",
    cron="5 2 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__saldo = _sicor_flow(
    table_id="saldo",
    cron="15 4 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__liberacao = _sicor_flow(
    table_id="liberacao",
    cron="25 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_complemento_operacao = _sicor_flow(
    table_id="recurso_publico_complemento_operacao",
    cron="35 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_cooperado = _sicor_flow(
    table_id="recurso_publico_cooperado",
    cron="45 4 * * 1-5",
)

br_bcb_sicor__recurso_publico_gleba = _sicor_flow(
    table_id="recurso_publico_gleba",
    cron="55 4 * * 1-5",
    dump_mode="append",
)

br_bcb_sicor__recurso_publico_mutuario = _sicor_flow(
    table_id="recurso_publico_mutuario",
    cron="5 5 * * 1-5",
)

br_bcb_sicor__recurso_publico_propriedade = _sicor_flow(
    table_id="recurso_publico_propriedade",
    cron="15 5 * * 1-5",
)

br_bcb_sicor__operacoes_desclassificadas = _sicor_flow(
    table_id="operacoes_desclassificadas",
    cron="25 5 * * 1-5",
)

br_bcb_sicor__empreendimento = _sicor_flow(
    table_id="empreendimento",
    cron="35 5 * * 1-5",
    source_format="csv",
    coverage_type="all_free",
    historical_database=False,
)

# Tabelas de domínio: não têm `ano_emissao`/`mes_emissao`, então a cobertura é
# única (`NonHistorical`) e os dados são integralmente livres.
br_bcb_sicor__instituicao_financeira = _sicor_flow(
    table_id="instituicao_financeira",
    cron="45 5 * * 1-5",
    coverage_type="all_free",
    historical_database=False,
)

br_bcb_sicor__fonte_recurso = _sicor_flow(
    table_id="fonte_recurso",
    cron="55 5 * * 1-5",
    source_format="csv",
    coverage_type="all_free",
    historical_database=False,
)

# ── Proagro ──────────────────────────────────────────────────────────────────
# Todas em `overwrite`: a fonte publica um arquivo único por tabela, sem quebra
# anual, então cada run rebaixa e substitui a tabela inteira. Herdam o
# permissionamento padrão (`part_bdpro` sobre `ano_emissao`/`mes_emissao`), que
# vem do join com `operacao` — verificado: as chaves do Proagro casam com
# `operacao` em toda a amostra testada.
br_bcb_sicor__proagro_cop = _sicor_flow(
    table_id="proagro_cop",
    cron="5 6 * * 1-5",
)

br_bcb_sicor__proagro_complemento_cop = _sicor_flow(
    table_id="proagro_complemento_cop",
    cron="15 6 * * 1-5",
)

br_bcb_sicor__proagro_rcp = _sicor_flow(
    table_id="proagro_rcp",
    cron="25 6 * * 1-5",
)

br_bcb_sicor__proagro_complemento_rcp = _sicor_flow(
    table_id="proagro_complemento_rcp",
    cron="35 6 * * 1-5",
)

br_bcb_sicor__proagro_rcp_gleba = _sicor_flow(
    table_id="proagro_rcp_gleba",
    cron="45 6 * * 1-5",
)

br_bcb_sicor__proagro_parcela = _sicor_flow(
    table_id="proagro_parcela",
    cron="55 6 * * 1-5",
)

br_bcb_sicor__proagro_sumula_julgamento = _sicor_flow(
    table_id="proagro_sumula_julgamento",
    cron="5 7 * * 1-5",
)


# O dicionário é materializado antes das demais tabelas porque o teste
# `custom_dictionary_coverage` de cada uma delas lê este modelo: se ele estiver
# desatualizado, códigos novos da fonte derrubam o teste da tabela que os usa.
@flow(
    name="br_bcb_sicor__dicionario",
    log_prints=True,
)
def br_bcb_sicor__dicionario(
    dataset_id: str = "br_bcb_sicor",
    table_id: str = "dicionario",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = False,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    dicionario_filepath = create_load_dictionary()

    upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )


# Sem `deploy_schedules` o flow nunca executava por agendamento, e o
# `dbt_alias=False` acima fazia o seletor virar `models/br_bcb_sicor/
# dicionario.sql`, arquivo que não existe — `run_dbt` levantava
# FileNotFoundError depois de já ter subido o staging. Os dois defeitos juntos
# deixavam a tabela `dicionario` congelada. Roda às 01:45, antes de `operacao`
# (02:05) e das demais.
br_bcb_sicor__dicionario.deploy_schedules = [
    Cron("45 1 * * 1-5", timezone="America/Sao_Paulo")
]
