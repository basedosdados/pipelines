"""Flows do br_sedec_desastres — Prefect 3.

Relatório "Reconhecimentos vigentes" do S2ID (SEDEC/MIDR): os reconhecimentos
federais de situação de emergência e estado de calamidade pública em vigor. A
tabela é uma série de retratos, um por execução, `dump_mode="append"`,
particionada por `data_extracao`.

Migrado pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote (genérico, dispatch via
`CheckThenExtractLoadPipeline` — ver `pipelines/utils/stage_dispatch.py`).
Dataset de uma tabela só (`reconhecimentos_vigentes` — ver `constants.py`), daí
não haver a decomposição em pares por tabela do `pipeline_factory`: a
"concatenação de 27 downloads" do levantamento original é dentro DESSA única
tabela (1 export CSV por UF, concatenados em
`utils.build_reconhecimentos_vigentes`), não 27 tabelas.

A fonte não expõe nenhum sinal de frescor mais barato que o download inteiro:
"vigentes" é um retrato do momento, sem data de publicação própria, e a única
data disponível é a que o próprio retrato carimba
(`utils.build_reconhecimentos_vigentes`, sempre `date.today()`). Por isso
`get_latest_update` (`tasks.py`) baixa e monta o retrato inteiro — os 27
exports por UF, via Selenium atrás do proxy brasileiro — só para ler essa
data, e `extract_load_data` repete o mesmo download de forma independente
para materializar de verdade. Custo duplicado aceito, mesmo padrão (e mesma
decisão) de `br_bcb_taxa_selic`/`au_rba_statistical_tables`: uma variante
"check_and_download" que reaproveitaria um único download entre as duas
etapas foi cogitada e cancelada — todo dataset usa o mesmo formato
check_update -> extract_and_load, mesmo quando o check precisa baixar dado
pra achar a data de referência.

O guarda do poll compara a `data_extracao` do retrato contra o fim da
`Coverage` já registrada (`compare_against="coverage"`, o padrão de
`SourceInspection`): um retrato novo passa, um segundo run no mesmo dia não —
é o que evita duplicar o retrato do dia.

A promoção pra prod (`build_and_promote` -> `transfer_files_to_prod_flow`)
promove só a(s) pasta(s) de partição novas desta run
(`ExtractAndLoad.partition_folders`, preenchido automaticamente pela cápsula a
partir de `discover_partition_folders`), não o prefixo de staging inteiro —
os retratos antigos, em prod, não são tocados. É um mecanismo diferente do da
action `table-approve` (que espelha staging de dev por cima do de prod e
apagaria os retratos que só existem em prod — por isso esta tabela nunca usou
aquela action, ver README).

As decisões de desenho e o que ainda está aberto estão no README do diretório
(parte dele — deployment names e parâmetros de disparo manual — ficou
desatualizada por esta migração; a atualização do README é trabalho
pendente).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_sedec_desastres.constants import constants
from pipelines.datasets.br_sedec_desastres.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value

_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def check_update() -> None:
    _pipeline.run_check_update()


check_update.deploy_tags = deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
# Mesmo cron do flow monolítico antigo (main).
check_update.deploy_schedules = [
    Cron("10 2 * * *", timezone="America/Sao_Paulo")
]
# O check baixa os 27 exports via Selenium/Chrome (ver tasks.py) — mesma
# margem de memória do flow monolítico antigo.
check_update.job_variables = {
    "memory_limit": "4Gi",
    "memory_request": "1Gi",
}


@flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
def extract_and_load(download_params: dict) -> None:
    _pipeline.run_extract_and_load(download_params)


extract_and_load.deploy_tags = deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD)
# Mesmo custo de Selenium/Chrome do check_update — baixa o retrato de novo.
extract_and_load.job_variables = {
    "memory_limit": "4Gi",
    "memory_request": "1Gi",
}
