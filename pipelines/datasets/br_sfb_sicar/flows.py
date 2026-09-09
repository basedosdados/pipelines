"""Flows para br_sfb_sicar — Prefect 3.

Migrado pro pipeline orientado a eventos (issue #1867) — parcialmente,
por design: `check_update` usa a mesma convenção de nome/dispatch de
qualquer outro dataset (`check_update_and_dispatch`, genérico), mas o
estágio seguinte **não** usa `CheckThenDownloadPipeline`/`mat_test_flow`
genérico.

Motivo: este dataset materializa **9 tabelas por execução** (não 1), com
checkpoint de upload resumível por UF/tema em GCS (corrida de ~30h no
backfill nacional) e testes dbt **cruzados** entre as 9 tabelas depois
que todas estão construídas — o `mat_test_flow` genérico testa 1 tabela
de cada vez, não encaixa sem quebrar essa dependência cruzada. Por isso
o estágio de download (`br_sfb_sicar_download_flow`) chama
`download_and_materialize` (tasks.py) diretamente — a mesma lógica do
antigo flow monolítico, só sem a parte de poll/commit (que já rola no
`check_update`, via `check_update_and_dispatch`).

O `check_for_update` deste dataset é genuinamente leve (`get_release_dates_task`
só lê a página de datas de release do SICAR — ver comentário original
"Cheap: one page fetch. Do the poll BEFORE downloading gigabytes of
zips."), ao contrário de `br_ibge_ipca`, que era `check_and_download`
disfarçado.
"""

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.br_sfb_sicar.constants import ANCHOR_TABLE, DATASET_ID
from pipelines.datasets.br_sfb_sicar.tasks import (
    check_for_update,
    download_and_materialize,
)
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table

# Mesma convenção de `CheckThenDownloadPipeline.prefect_dataset_id`
# (`f"{dataset_id}__{table_id}"`, sempre) — usando a tabela-âncora
# (`area_imovel`) como identidade, já que o poll/commit de source-update
# é feito sobre ela (ver `crawler/sfb_sicar/constants.py::ANCHOR_TABLE`).
_PREFECT_DATASET_ID = f"{DATASET_ID}__{ANCHOR_TABLE}"


@flow(
    name=f"{Etapa.CHECK_UPDATE}: {_PREFECT_DATASET_ID}",
    log_prints=True,
)
def br_sfb_sicar_check_update_flow() -> None:
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Check Update: ",
            dataset_id=DATASET_ID,
            table_id=ANCHOR_TABLE,
        )
    )

    result = check_for_update()

    check_update_and_dispatch(
        prefect_dataset_id=_PREFECT_DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=ANCHOR_TABLE,
        reference_date=result.reference_date,
        next_deployment=br_sfb_sicar_download_flow.fn.__name__,
        date_format="%Y-%m-%d",
        extra_download_params=result.extra_download_params,
    )


# pyrefly: ignore [missing-attribute]
br_sfb_sicar_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# SICAR publica por UF, sem calendário fixo. Poll em alguns dias do meio
# do mês; o guard de source-poll (dentro de check_update_and_dispatch)
# faz o disparo pro download ser um no-op barato entre releases.
# pyrefly: ignore [missing-attribute]
br_sfb_sicar_check_update_flow.deploy_schedules = [
    {"cron": "0 16 10,11,12,13,14,15 * *", "timezone": "America/Sao_Paulo"}
]


@flow(
    name=f"{Etapa.DOWNLOAD}: {_PREFECT_DATASET_ID}",
    log_prints=True,
)
def br_sfb_sicar_download_flow(
    download_params: dict | None = None,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    only_themes: str = "",
    only_ufs: str = "",
    clean_only: bool = False,
    stage_only: bool = False,
) -> None:
    """Baixa+limpa+stagea+materializa as 9 tabelas de tema.

    `download_params` (repassado pelo `check_update`) não é usado hoje —
    `download_and_materialize` busca `release_iso` de novo sozinho
    (barato, mesma página do check). Mantido como parâmetro só pra bater
    com a assinatura que `run_deployment(parameters={"download_params": ...})`
    espera do lado de quem dispara.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Download: ", dataset_id=DATASET_ID, table_id=ANCHOR_TABLE
        )
    )
    download_and_materialize(
        materialize_to_prod=materialize_to_prod,
        update_metadata=update_metadata,
        only_themes=only_themes,
        only_ufs=only_ufs,
        clean_only=clean_only,
        stage_only=stage_only,
    )


# pyrefly: ignore [missing-attribute]
br_sfb_sicar_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
# Memória alta pro clean/stage das 9 tabelas x 27 UFs — mesmo tier do
# flow antigo (ver comentário original sobre MALLOC_ARENA_MAX/pool job
# template aceitando `memory`/`memory_limit`/`memory_request`).
# pyrefly: ignore [missing-attribute]
br_sfb_sicar_download_flow.job_variables = {
    "memory": "12Gi",
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
    "env": [
        {"name": "MALLOC_ARENA_MAX", "value": "2"},
        {"name": "MALLOC_TRIM_THRESHOLD_", "value": "131072"},
    ],
}
