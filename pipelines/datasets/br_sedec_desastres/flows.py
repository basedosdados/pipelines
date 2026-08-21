"""Flows do br_sedec_desastres — Prefect 3.

Relatório "Reconhecimentos vigentes" do S2ID (SEDEC/MIDR): os reconhecimentos
federais de situação de emergência e estado de calamidade pública em vigor.

A tabela é uma série de retratos — cada execução grava o conjunto vigente na data
da extração. As decisões de desenho e o que ainda está aberto estão no README do
diretório.

**Este flow não roda hoje.** O S2ID barra o IP de saída do cluster por
geolocalização e a raspagem morre no download. Decidido em 2026-08-10, com a
supervisão: o retrato é gerado na máquina de quem mantém a base, pelo
`run_local.py` deste diretório, e promovido por PR com a label `table-approve`.
A receita mensal está no README. O flow fica aqui porque o código é o mesmo — se
o IP for liberado, basta devolver o `deploy_schedules`.

Deploy: `.github/scripts/deploy_flows.py` descobre `br_sedec_desastres_flow`
automaticamente, desde que o flow esteja definido neste arquivo (o script filtra
por `obj.fn.__code__.co_filename`). O pool de dev ignora o schedule; o de prod o
ativa, mas entra pausado.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_sedec_desastres.constants import constants
from pipelines.datasets.br_sedec_desastres.tasks import (
    clean_reconhecimentos,
    download_reconhecimentos,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATE_FORMAT = constants.DATE_FORMAT.value

_COVERAGE = AllFree(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)


# Os parâmetros são documentados aqui, e não numa seção `Args:` do docstring, porque
# o Prefect 3 converte essa seção no `description` de cada parâmetro do JSON schema
# do deployment — e o formulário de run passa a exibir os textos, diferente dos
# demais flows do repo. Ao editar, note que o formulário só reflete a mudança depois
# de um novo deploy.
#
# materialize_after_dump: seguir além da materialização em dev, escrevendo no bucket
#     de staging de prod e rodando dbt com o `target`. Passar False para exercitar só
#     a metade de dev, que é o teste seguro, já que o padrão escreve em produção.
#     Também desliga o poll, que consulta o backend de prod.
# update_metadata: depois de materializar prod com sucesso, registrar a cobertura da
#     tabela e gravar o update da fonte. Sem efeito quando materialize_after_dump é
#     False.
# force_run: materializar mesmo quando o poll não achar dado novo.
@flow(name="br_sedec_desastres", log_prints=True)
def br_sedec_desastres_flow(
    dataset_id: str = constants.DATASET_ID.value,
    table_id: str = constants.TABLE_ID.value,
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Baixa o relatório do S2ID, remonta a tabela e materializa."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    work_dir = tempfile.mkdtemp(prefix="br_sedec_desastres_")
    try:
        input_dir = download_reconhecimentos(work_dir=work_dir)
        result = clean_reconhecimentos(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]
        # A chave do `result` vem do `constants.TABLE_ID`, que é o que o
        # `clean_all` usa para nomear o diretório de saída — não do parâmetro
        # `table_id`, que só endereça o destino no BigQuery.
        data_path = result[constants.TABLE_ID.value]

        # O poll é pinado em env="prod" e o return dele vem antes do upload de
        # dev: consultado sem condição, um run de dev depende do backend de
        # produção e pode encerrar sem ingerir nada, reportando COMPLETED.
        if materialize_after_dump:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
            )
            if not has_new_data and not force_run:
                return

            # Comita o Update da fonte já aqui, antes de baixar/materializar: se
            # o flow falhar no meio, o metadado da fonte ainda reflete que havia
            # dado novo publicado, mesmo que a tabela não tenha sido atualizada.
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
                update_metadata=update_metadata,
            )

        dump_mode = "append"

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_after_dump:
            upload_to_gcs(
                data_path=data_path,
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name="basedosdados-dev",
                dump_mode=dump_mode,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                dbt_alias=dbt_alias,
                target="dev",
            )
            return

        upload_to_gcs(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name="basedosdados",
            dump_mode=dump_mode,
            source_format="parquet",
        )
        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target=target,
        )

        if update_metadata:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Sem schedule de propósito: o pod não alcança a fonte, então um deployment
# armado falharia todo dia 1º. Um retrato por mês continua sendo a cadência — a
# fonte é contínua e a vigência é de 180 dias, então mensal basta —, só que ela é
# executada à mão pelo run_local.py. Se o IP for liberado, descomentar:
#
# br_sedec_desastres_flow.deploy_schedules = [
#     {"cron": "10 9 1 * *", "timezone": "America/Sao_Paulo"}
# ]


# pyrefly: ignore [missing-attribute]
br_sedec_desastres_flow.job_variables = {"memory": "4Gi"}
