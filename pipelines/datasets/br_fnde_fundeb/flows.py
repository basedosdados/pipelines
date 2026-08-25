"""Flow do br_fnde_fundeb — Prefect 3.

Atualiza o exercício corrente dos Indicadores do SIOPE a partir do produto 54 da
Plataforma Antonieta de Barros. O histórico, 2021 a 2024, vem do produto 53 e é
carregado fora deste flow; a divisão está na seção "Carga e atualização" do
README do diretório.

O `dump_mode` é `append`, não `overwrite`: as duas cargas compartilham o prefixo
de staging, e `overwrite` recria a tabela levando o histórico junto. Com
`append`, o `if_exists="replace"` do upload age por blob e o `write_partitioned`
grava sempre um `ano=<ano>/data.parquet`, então o exercício corrente substitui a
própria partição e as de 2021 a 2024 permanecem.

O `dicionario` não entra no flow: as 112 linhas estão fixas em
`constants.DICTIONARY_ROWS` e mudam quando a fonte reescreve o nome de um
indicador, caso em que a limpeza registra um WARNING.

As tabelas são materializadas na ordem `run` de todas, depois `test` de todas,
porque o `custom_dictionary_coverage` de cada uma lê o modelo do `dicionario` e
um `test` intercalado executa antes de a tabela irmã existir num ambiente limpo.

A cobertura registrada é anual: o grão do dado é ano e bimestre, e `DateColumn`
não tem variante de bimestre — só ano, ano-mês, ano-trimestre e data. O poll
continua distinguindo bimestres, porque usa o `max_date` do `clean_all`, que é o
primeiro dia do último mês do bimestre em `%Y-%m-%d`.

O poll é consultado somente quando o run vai até prod, já que lê o backend de
produção; incondicional, ele faria um run de dev depender de produção e encerrar
sem ingerir nada, reportando COMPLETED. O commit do update da fonte é o último
passo, porque registra até onde a fonte foi ingerida — gravá-lo antes faria um
run interrompido no meio parecer concluído para o poll seguinte.

A cadência é de quatro execuções por mês. A fonte é bimestral, mas o bimestre
publicado é revisado por semanas: os entes declaram com prazo e o FNDE reexporta
o arquivo. Nas execuções sem período novo o poll encerra antes de materializar.

Deploy: `.github/scripts/deploy_flows.py` descobre `br_fnde_fundeb_flow`
automaticamente, desde que o flow esteja definido neste arquivo (o script filtra
por `obj.fn.__code__.co_filename`). O pool de dev ignora o schedule; o de prod o
ativa, mas entra pausado — armar é um passo manual no admin do Django.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.datasets.br_fnde_fundeb.tasks import clean_siope, download_siope
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
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

TABLES = [
    constants.TABLE_STATE.value,
    constants.TABLE_MUNICIPALITY.value,
]

# As duas tabelas apontam para a mesma raw data source, então uma âncora basta.
POLL_TABLE = constants.TABLE_MUNICIPALITY.value

_COVERAGE = AllFree(
    date_column=YearOnly(col="ano"),
    date_format=DateFormat.YEAR,
)


# Os parâmetros ficam em comentário, e não numa seção `Args:`, porque o Prefect 3
# converte essa seção no `description` de cada parâmetro do deployment e o formulário
# de run passa a exibir os textos.
#
# materialize_to_prod: seguir além da materialização em dev, escrevendo no bucket de
#     staging de prod e rodando dbt com o `target`. False materializa somente em dev
#     e não consulta o poll, que lê o backend de produção.
# update_metadata: depois de materializar prod com sucesso, registrar a cobertura das
#     tabelas e gravar o update da fonte. Sem efeito quando materialize_to_prod é
#     False.
# force_run: materializar mesmo quando o poll não achar dado novo.
@flow(name="br_fnde_fundeb", log_prints=True)
def br_fnde_fundeb_flow(
    dataset_id: str = constants.DATASET_ID.value,
    materialize_to_prod: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Baixa o exercício corrente do SIOPE, remonta as tabelas e materializa."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="br_fnde_fundeb_")
    try:
        source_path = download_siope(
            work_dir=work_dir, product_id=constants.PRODUCT_CURRENT.value
        )
        result = clean_siope(work_dir=work_dir, source_path=source_path)
        max_date = result["max_date"]

        if materialize_to_prod:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=POLL_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
            )
            if not has_new_data and not force_run:
                return

        bucket_name = (
            "basedosdados" if materialize_to_prod else "basedosdados-dev"
        )
        dbt_target = target if materialize_to_prod else "dev"

        for table_id in TABLES:
            upload_to_gcs(
                data_path=result[table_id],
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name=bucket_name,
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run",
                dbt_alias=dbt_alias,
                target=dbt_target,
            )

        for table_id in TABLES:
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="test",
                dbt_alias=dbt_alias,
                target=dbt_target,
            )

        if not materialize_to_prod or not update_metadata:
            return

        for table_id in TABLES:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )

        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=POLL_TABLE,
            source_max_date=max_date,
            env="prod",
            date_format=DATE_FORMAT,
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Minuto 17 por não estar em uso no repo: vinte flows disparam no minuto 0.
# pyrefly: ignore [missing-attribute]
br_fnde_fundeb_flow.deploy_schedules = [
    {"cron": "17 10 5,12,19,26 * *", "timezone": "America/Sao_Paulo"}
]

# pyrefly: ignore [missing-attribute]
br_fnde_fundeb_flow.job_variables = {"memory": "4Gi"}
