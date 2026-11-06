"""
Flows for us_fed_fred — Prefect 3.

FRED (Federal Reserve Bank of St. Louis) public-domain economic series. FRED
serves the full history of each series on every call (latest revision only), so
each run is a **full replace** (``dump_mode="overwrite"``), not an incremental
append.

Migrated to check_update -> extract_and_load, SEM ``build_and_promote``: ao
contrário do shape padrão de um ``pipeline_factory``/``CheckThenExtractLoadPipeline``
(ver ``pipelines/utils/stage_dispatch.py``), este dataset tem duas restrições que o
building block genérico por tabela não resolve sozinho (mesmo par de restrições
que ``br_bcb_ifdata``/``au_rba_statistical_tables``).

1. Download compartilhado e não decomponível por tabela: ``download_all()`` busca
   cada série semente uma vez (metadado + observações) e ``clean_all()`` lê esse
   mesmo JSON cru para produzir AMBAS as tabelas (``series``, ``observation``) —
   não há como baixar/limpar uma tabela sem reprocessar a série inteira. Por
   isso há só UM ``check_update`` e UM ``extract_and_load`` para o dataset
   inteiro, não um par por tabela. O check_update ainda baixa a série inteira só
   para achar a data de referência, e o extract_and_load baixa de novo,
   independentemente — custo duplicado aceito (ver o módulo
   ``pipelines/utils/stage_dispatch.py``), já que a variante ``check_and_download``
   que evitaria isso foi cancelada.

2. Ordem obrigatória no dbt: ``us_fed_fred__observation.series_id`` tem um teste
   ``relationships`` referenciando ``ref('us_fed_fred__series')``
   (``models/us_fed_fred/schema.yml``) — então as duas tabelas precisam ser
   ``run`` antes de QUALQUER uma ser ``test``ada (ver o docstring de
   ``_materialize`` abaixo), e ``series`` precisa existir no BigQuery antes do
   teste de ``observation`` rodar. O ``dispatch_build_and_promote`` genérico não
   serve aqui: ele dispara um ``build_and_promote`` POR TABELA, cada um de forma
   assíncrona e independente (cada deployment roda ``run`` + ``test`` só da sua
   própria tabela, sem esperar a outra) — isso reabriria exatamente essa race
   (teste de ``observation`` rodando antes de ``series`` existir, numa build
   limpa). A solução adotada é não usar ``dispatch_build_and_promote``:
   ``extract_and_load`` chama ``_materialize`` diretamente, que sobe as 2
   tabelas, roda ``dbt run`` de ambas e só depois ``dbt test`` de ambas —
   exatamente a lógica do antigo flow monolítico, preservada aqui, só
   reparticionada para depois do poll de ``check_update`` em vez de antes dele.

``register_table_materialization_task`` (atualização de cobertura/Update no
backend) também continua chamada diretamente dentro de ``extract_and_load``, não
via um ``build_and_promote`` genérico, pelo mesmo motivo.

Coverage tier: ``observation`` é a tabela de alta frequência (séries
diárias/semanais), então carrega a janela rolante do BD Pro: os 6 meses mais
recentes são pro, o resto é livre. ``series`` é um catálogo de metadados sem
dimensão de tempo e fica totalmente livre (``NonHistorical``, a partir do
``last_modified`` da tabela).

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers os dois
flows abaixo; o pool de dev ignora o schedule do check_update, o pool de prod o
ativa (deployado pausado).
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import date

from prefect.schedules import Cron

from pipelines.datasets.us_fed_fred.constants import constants
from pipelines.datasets.us_fed_fred.tasks import clean_fred, download_fred
from pipelines.utils.flow import flow
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
# A tabela de maior frequência é o sinal de atualização do dataset inteiro —
# mesma tabela usada para o poll no antigo flow monolítico.
POLL_TABLE = "observation"

# Nome do deployment de `us_fed_fred_extract_and_load` abaixo (`deploy_flows.py`
# registra um deployment com o nome da variável Python do `@flow`). Passado
# explicitamente para `check_update_and_dispatch` porque o nome do flow não é
# literalmente `str(Etapa.EXTRACT_AND_LOAD)`.
_EXTRACT_AND_LOAD_DEPLOYMENT = "us_fed_fred_extract_and_load"


def _materialize(target: str, bucket: str, paths: dict) -> None:
    """Sobe o staging e materializa as duas tabelas, depois testa as duas.

    A separação entre `run` e `test` é obrigatória aqui, não estilística: o
    teste `relationships` de `us_fed_fred__observation.series_id` referencia
    `ref('us_fed_fred__series')`. Testando tabela a tabela, o teste de
    `observation` pode rodar antes de `series` existir numa build limpa e
    falhar com "Table ... us_fed_fred.series was not found". Num ambiente já
    povoado isso passa despercebido porque `series` sobrou da build anterior;
    só quebra num ambiente limpo, então a ordem precisa ser estrutural —
    `constants.ALL_TABLES` já lista `series` antes de `observation` por esse
    motivo.
    """
    for table in ALL_TABLES:
        upload_to_gcs(
            data_path=paths[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode="overwrite",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target=target,
        )

    for table in ALL_TABLES:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target=target,
        )


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def us_fed_fred_check_update() -> bool:
    """Baixa as séries semente, acha a data de referência, dispara extract_and_load.

    Baixa e limpa a série inteira só para achar `max_date` — o mesmo trabalho
    que `extract_and_load` refaz de forma independente um instante depois, caso
    haja dado novo. Custo duplicado aceito: a variante `check_and_download` que
    evitaria isso foi cancelada (ver o banner do módulo).

    Returns:
        `True` se havia observação mais nova do que a cobertura atual de
        `observation` (e `extract_and_load` foi disparado); `False` caso
        contrário.

    Raises:
        RuntimeError: se nenhuma série semente tiver observações (fonte vazia,
            não "sem dado novo" — não há o que comparar).
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_fed_fred_check_")
    try:
        input_dir = download_fred(work_dir=work_dir)
        result = clean_fred(work_dir=work_dir, input_dir=input_dir)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    max_date = result["max_date"]
    if max_date is None:
        raise RuntimeError(
            "us_fed_fred: no observations found across the seed series"
        )
    print(f"latest observation date at the source: {max_date}")
    year, month, day = (int(part) for part in max_date.split("-"))

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        reference_date=date(year, month, day),
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y-%m-%d",
    )


us_fed_fred_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# FRED updates on US business days through the afternoon (ET). Poll once daily at
# 21:00 BRT (~19:00-20:00 ET); the source-poll guard no-ops on days with no new
# observation, so a plain daily cron is cheap.
us_fed_fred_check_update.deploy_schedules = [
    Cron("25 21 * * *", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def us_fed_fred_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Rebaixa as séries semente, reconstrói as duas tabelas, materializa em lote.

    A reconstrução é completa a cada run, não incremental: FRED serve a
    história inteira de cada série em toda chamada (apenas a última revisão),
    então cada run é um full replace (`dump_mode="overwrite"` dentro de
    `_materialize`), não um append incremental.

    Args:
        download_params: dict recebido do `check_update` via `run_deployment()`
            — só carrega `reference_date` (a data que disparou esta run);
            `download_fred`/`clean_fred` sempre refazem a série inteira, então
            o valor não é usado para decidir o que baixar, só para
            rastreabilidade.
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção. Disparar este flow
            manualmente com `materialize_to_prod=False` substitui o antigo
            parâmetro `force_run` do flow monolítico: como `extract_and_load`
            agora é disparável direto (sem depender do poll de `check_update`
            achar dado novo), rematerializar sob demanda não exige mais uma
            flag separada.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_fed_fred_")
    try:
        input_dir = download_fred(work_dir=work_dir)
        result = clean_fred(work_dir=work_dir, input_dir=input_dir)

        _materialize("dev", "basedosdados-dev", result)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", result)

        if update_metadata:
            for table in ALL_TABLES:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


us_fed_fred_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
