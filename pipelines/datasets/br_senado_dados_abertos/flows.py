"""
Flows for br_senado_dados_abertos -- Prefect 3, staged pipeline.

Migrado por completo pro pipeline em estagios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela -- 18 pares independentes (36 flows), um por table_id. Logica de
negocio mora em `tasks.py`/`senado_clean.py` (reaproveitada sem alteracao),
constantes em `constants.py` -- aqui so a fiacao
(`CheckThenExtractLoadPipeline` + `@flow`).

Por que 18 pares independentes em vez de agrupar as tabelas que compartilham
crawl: download/crawl compartilhado nao e motivo pra agrupar em 1 flow -- so
dependencia real de ordem no dbt justificaria isso, e nao ha nenhuma entre as
18 tabelas deste dataset (nenhum `ref()` cruzado em
`models/br_senado_dados_abertos/*.sql`/`schema.yml`). Onde ha acoplamento no
crawl (`sc.clean_votacao`/`sc.clean_votacao_comissao` devolvem duas tabelas de
uma vez; `sc.clean_senador_mandato/filiacao/comissao/cargo` e
`sc.clean_votacao_comissao` precisam de `codes`/`siglas` derivados de
`sc.clean_senador`/`sc.clean_comissao`), cada tabela chama tudo que precisa de
novo, de forma independente e redundante -- ver o banner de `tasks.py` pelos
casos exatos. Mesmo trade-off ja tomado em `au_ato_abr`/`us_bls_oes` para uma
fonte parecida (1 download/crawl, varias tabelas).

Grupo A -- 10 dimensoes (`constants.DIMS`): senador, partido, bloco,
lideranca, comissao, mesa, senador_mandato, senador_filiacao,
senador_comissao, senador_cargo. Cadastro do estado atual, sem nenhum sinal de
data pra comparar -- "sempre dispara" por natureza, nao e um bug. Materializam
com `NonHistorical` (`constants.GROUP_A_COVERAGE`), que atualiza so
`Table.Update.latest` e nunca mexe em `Coverage.DateTimeRange` -- preserva a
`Coverage` do onboarding exatamente como o flow monolitico antigo preservava
(ele nunca re-registrava essas 10 -- ver o comentario removido do antigo
`_COVERAGE`, que so cobria as 8 do Grupo B).

Grupo B -- 8 series temporais (`constants.PARTITIONED`): votacao,
votacao_parlamentar, votacao_orientacao_bancada, processo, relatoria,
votacao_comissao, votacao_comissao_parlamentar, discurso. Janela de rotina =
ano corrente + `REFRESH_PRIOR_YEARS` anos anteriores (pega edicoes tardias),
upload com `dump_mode="append"` (default de `ExtractAndLoad`) -- substitui so
as particoes `ano=` tocadas, preserva o historico. BD Pro rolling window de 6
meses (`PartBdpro`, `constants.COVERAGE`) -- `register_table_materialization_
task`/`build_and_promote` recalcula `free_end` e reemite as Row Access
Policies a cada run.

Cron escalonado: o flow antigo era um monolito diario unico (08:15 BRT, ver o
`git log` deste arquivo). Os 18 check_update novos competiriam por recurso
(rede da API do Senado, slots de worker) se disparassem todos juntos, entao
ficam escalonados a cada 8 minutos a partir do mesmo horario -- 08:15 ate
10:31 BRT, na ordem `constants.DIMS + constants.PARTITIONED` (== `ALL_TABLES`).

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-descobre todo `@flow`
daqui pelas tags/schedules (`deploy_tags`/`deploy_schedules`).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_senado_dados_abertos.constants import (
    BLOCO_TABLE_ID,
    COMISSAO_TABLE_ID,
    DATASET_ID,
    DISCURSO_TABLE_ID,
    LIDERANCA_TABLE_ID,
    MESA_TABLE_ID,
    PARTIDO_TABLE_ID,
    PROCESSO_TABLE_ID,
    RELATORIA_TABLE_ID,
    SENADOR_CARGO_TABLE_ID,
    SENADOR_COMISSAO_TABLE_ID,
    SENADOR_FILIACAO_TABLE_ID,
    SENADOR_MANDATO_TABLE_ID,
    SENADOR_TABLE_ID,
    VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID,
    VOTACAO_COMISSAO_TABLE_ID,
    VOTACAO_ORIENTACAO_BANCADA_TABLE_ID,
    VOTACAO_PARLAMENTAR_TABLE_ID,
    VOTACAO_TABLE_ID,
)
from pipelines.datasets.br_senado_dados_abertos.tasks import (
    make_pipeline_group_a,
    make_pipeline_group_b,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# senador  (Grupo A)
# check_update: br_senado_dados_abertos__senador
# extract_and_load: br_senado_dados_abertos__senador
# ──────────────────────────────────────────────────────────────────────────────

_senador_pipeline = make_pipeline_group_a(SENADOR_TABLE_ID)


@flow(name=_senador_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_check_update() -> None:
    _senador_pipeline.run_check_update()


br_senado_dados_abertos_senador_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SENADOR_TABLE_ID
)
br_senado_dados_abertos_senador_check_update.deploy_schedules = [
    Cron("15 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_senador_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_download(download_params: dict) -> None:
    _senador_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SENADOR_TABLE_ID
)
_senador_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# partido  (Grupo A)
# check_update: br_senado_dados_abertos__partido
# extract_and_load: br_senado_dados_abertos__partido
# ──────────────────────────────────────────────────────────────────────────────

_partido_pipeline = make_pipeline_group_a(PARTIDO_TABLE_ID)


@flow(name=_partido_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_partido_check_update() -> None:
    _partido_pipeline.run_check_update()


br_senado_dados_abertos_partido_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PARTIDO_TABLE_ID
)
br_senado_dados_abertos_partido_check_update.deploy_schedules = [
    Cron("23 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_partido_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_partido_download(download_params: dict) -> None:
    _partido_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_partido_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PARTIDO_TABLE_ID
)
_partido_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_partido_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# bloco  (Grupo A)
# check_update: br_senado_dados_abertos__bloco
# extract_and_load: br_senado_dados_abertos__bloco
# ──────────────────────────────────────────────────────────────────────────────

_bloco_pipeline = make_pipeline_group_a(BLOCO_TABLE_ID)


@flow(name=_bloco_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_bloco_check_update() -> None:
    _bloco_pipeline.run_check_update()


br_senado_dados_abertos_bloco_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, BLOCO_TABLE_ID
)
br_senado_dados_abertos_bloco_check_update.deploy_schedules = [
    Cron("31 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_bloco_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_bloco_download(download_params: dict) -> None:
    _bloco_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_bloco_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, BLOCO_TABLE_ID
)
_bloco_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_bloco_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# lideranca  (Grupo A)
# check_update: br_senado_dados_abertos__lideranca
# extract_and_load: br_senado_dados_abertos__lideranca
# ──────────────────────────────────────────────────────────────────────────────

_lideranca_pipeline = make_pipeline_group_a(LIDERANCA_TABLE_ID)


@flow(name=_lideranca_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_lideranca_check_update() -> None:
    _lideranca_pipeline.run_check_update()


br_senado_dados_abertos_lideranca_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, LIDERANCA_TABLE_ID
)
br_senado_dados_abertos_lideranca_check_update.deploy_schedules = [
    Cron("39 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_lideranca_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_lideranca_download(download_params: dict) -> None:
    _lideranca_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_lideranca_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, LIDERANCA_TABLE_ID
)
_lideranca_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_lideranca_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# comissao  (Grupo A)
# check_update: br_senado_dados_abertos__comissao
# extract_and_load: br_senado_dados_abertos__comissao
# ──────────────────────────────────────────────────────────────────────────────

_comissao_pipeline = make_pipeline_group_a(COMISSAO_TABLE_ID)


@flow(name=_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_comissao_check_update() -> None:
    _comissao_pipeline.run_check_update()


br_senado_dados_abertos_comissao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, COMISSAO_TABLE_ID
)
br_senado_dados_abertos_comissao_check_update.deploy_schedules = [
    Cron("47 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_comissao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_comissao_download(download_params: dict) -> None:
    _comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_comissao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, COMISSAO_TABLE_ID
)
_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_comissao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# mesa  (Grupo A)
# check_update: br_senado_dados_abertos__mesa
# extract_and_load: br_senado_dados_abertos__mesa
# ──────────────────────────────────────────────────────────────────────────────

_mesa_pipeline = make_pipeline_group_a(MESA_TABLE_ID)


@flow(name=_mesa_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_mesa_check_update() -> None:
    _mesa_pipeline.run_check_update()


br_senado_dados_abertos_mesa_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MESA_TABLE_ID
)
br_senado_dados_abertos_mesa_check_update.deploy_schedules = [
    Cron("55 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_mesa_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_mesa_download(download_params: dict) -> None:
    _mesa_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_mesa_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MESA_TABLE_ID
)
_mesa_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_mesa_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# senador_mandato  (Grupo A)
# check_update: br_senado_dados_abertos__senador_mandato
# extract_and_load: br_senado_dados_abertos__senador_mandato
# ──────────────────────────────────────────────────────────────────────────────

_senador_mandato_pipeline = make_pipeline_group_a(SENADOR_MANDATO_TABLE_ID)


@flow(name=_senador_mandato_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_mandato_check_update() -> None:
    _senador_mandato_pipeline.run_check_update()


br_senado_dados_abertos_senador_mandato_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SENADOR_MANDATO_TABLE_ID
)
br_senado_dados_abertos_senador_mandato_check_update.deploy_schedules = [
    Cron("3 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_mandato_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_mandato_download(
    download_params: dict,
) -> None:
    _senador_mandato_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_mandato_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SENADOR_MANDATO_TABLE_ID
)
_senador_mandato_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_mandato_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# senador_filiacao  (Grupo A)
# check_update: br_senado_dados_abertos__senador_filiacao
# extract_and_load: br_senado_dados_abertos__senador_filiacao
# ──────────────────────────────────────────────────────────────────────────────

_senador_filiacao_pipeline = make_pipeline_group_a(SENADOR_FILIACAO_TABLE_ID)


@flow(name=_senador_filiacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_filiacao_check_update() -> None:
    _senador_filiacao_pipeline.run_check_update()


br_senado_dados_abertos_senador_filiacao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, SENADOR_FILIACAO_TABLE_ID)
)
br_senado_dados_abertos_senador_filiacao_check_update.deploy_schedules = [
    Cron("11 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_filiacao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_filiacao_download(
    download_params: dict,
) -> None:
    _senador_filiacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_filiacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SENADOR_FILIACAO_TABLE_ID
)
_senador_filiacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_filiacao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# senador_comissao  (Grupo A)
# check_update: br_senado_dados_abertos__senador_comissao
# extract_and_load: br_senado_dados_abertos__senador_comissao
# ──────────────────────────────────────────────────────────────────────────────

_senador_comissao_pipeline = make_pipeline_group_a(SENADOR_COMISSAO_TABLE_ID)


@flow(name=_senador_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_comissao_check_update() -> None:
    _senador_comissao_pipeline.run_check_update()


br_senado_dados_abertos_senador_comissao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, SENADOR_COMISSAO_TABLE_ID)
)
br_senado_dados_abertos_senador_comissao_check_update.deploy_schedules = [
    Cron("19 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_senador_comissao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_senador_comissao_download(
    download_params: dict,
) -> None:
    _senador_comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_comissao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SENADOR_COMISSAO_TABLE_ID
)
_senador_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_comissao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# senador_cargo  (Grupo A)
# check_update: br_senado_dados_abertos__senador_cargo
# extract_and_load: br_senado_dados_abertos__senador_cargo
# ──────────────────────────────────────────────────────────────────────────────

_senador_cargo_pipeline = make_pipeline_group_a(SENADOR_CARGO_TABLE_ID)


@flow(name=_senador_cargo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_cargo_check_update() -> None:
    _senador_cargo_pipeline.run_check_update()


br_senado_dados_abertos_senador_cargo_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, SENADOR_CARGO_TABLE_ID
)
br_senado_dados_abertos_senador_cargo_check_update.deploy_schedules = [
    Cron("27 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_senador_cargo_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_senador_cargo_download(
    download_params: dict,
) -> None:
    _senador_cargo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_senador_cargo_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, SENADOR_CARGO_TABLE_ID
)
_senador_cargo_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_senador_cargo_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# votacao  (Grupo B)
# check_update: br_senado_dados_abertos__votacao
# extract_and_load: br_senado_dados_abertos__votacao
# ──────────────────────────────────────────────────────────────────────────────

_votacao_pipeline = make_pipeline_group_b(VOTACAO_TABLE_ID)


@flow(name=_votacao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_check_update() -> None:
    _votacao_pipeline.run_check_update()


br_senado_dados_abertos_votacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, VOTACAO_TABLE_ID
)
br_senado_dados_abertos_votacao_check_update.deploy_schedules = [
    Cron("35 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_votacao_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_download(download_params: dict) -> None:
    _votacao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, VOTACAO_TABLE_ID
)
_votacao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# votacao_parlamentar  (Grupo B)
# check_update: br_senado_dados_abertos__votacao_parlamentar
# extract_and_load: br_senado_dados_abertos__votacao_parlamentar
# ──────────────────────────────────────────────────────────────────────────────

_votacao_parlamentar_pipeline = make_pipeline_group_b(
    VOTACAO_PARLAMENTAR_TABLE_ID
)


@flow(
    name=_votacao_parlamentar_pipeline.check_update_flow_name, log_prints=True
)
def br_senado_dados_abertos_votacao_parlamentar_check_update() -> None:
    _votacao_parlamentar_pipeline.run_check_update()


br_senado_dados_abertos_votacao_parlamentar_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, VOTACAO_PARLAMENTAR_TABLE_ID)
)
br_senado_dados_abertos_votacao_parlamentar_check_update.deploy_schedules = [
    Cron("43 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_parlamentar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_parlamentar_download(
    download_params: dict,
) -> None:
    _votacao_parlamentar_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_parlamentar_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, VOTACAO_PARLAMENTAR_TABLE_ID
)
_votacao_parlamentar_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_parlamentar_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# votacao_orientacao_bancada  (Grupo B)
# check_update: br_senado_dados_abertos__votacao_orientacao_bancada
# extract_and_load: br_senado_dados_abertos__votacao_orientacao_bancada
# ──────────────────────────────────────────────────────────────────────────────

_votacao_orientacao_bancada_pipeline = make_pipeline_group_b(
    VOTACAO_ORIENTACAO_BANCADA_TABLE_ID
)


@flow(
    name=_votacao_orientacao_bancada_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_orientacao_bancada_check_update() -> None:
    _votacao_orientacao_bancada_pipeline.run_check_update()


br_senado_dados_abertos_votacao_orientacao_bancada_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, VOTACAO_ORIENTACAO_BANCADA_TABLE_ID
    )
)
br_senado_dados_abertos_votacao_orientacao_bancada_check_update.deploy_schedules = [
    Cron("51 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_orientacao_bancada_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_orientacao_bancada_download(
    download_params: dict,
) -> None:
    _votacao_orientacao_bancada_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_orientacao_bancada_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, VOTACAO_ORIENTACAO_BANCADA_TABLE_ID
    )
)
_votacao_orientacao_bancada_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_orientacao_bancada_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# processo  (Grupo B)
# check_update: br_senado_dados_abertos__processo
# extract_and_load: br_senado_dados_abertos__processo
# ──────────────────────────────────────────────────────────────────────────────

_processo_pipeline = make_pipeline_group_b(PROCESSO_TABLE_ID)


@flow(name=_processo_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_processo_check_update() -> None:
    _processo_pipeline.run_check_update()


br_senado_dados_abertos_processo_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PROCESSO_TABLE_ID
)
br_senado_dados_abertos_processo_check_update.deploy_schedules = [
    Cron("59 9 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_processo_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_processo_download(download_params: dict) -> None:
    _processo_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_processo_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PROCESSO_TABLE_ID
)
_processo_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_processo_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# relatoria  (Grupo B)
# check_update: br_senado_dados_abertos__relatoria
# extract_and_load: br_senado_dados_abertos__relatoria
# ──────────────────────────────────────────────────────────────────────────────

_relatoria_pipeline = make_pipeline_group_b(RELATORIA_TABLE_ID)


@flow(name=_relatoria_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_relatoria_check_update() -> None:
    _relatoria_pipeline.run_check_update()


br_senado_dados_abertos_relatoria_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, RELATORIA_TABLE_ID
)
br_senado_dados_abertos_relatoria_check_update.deploy_schedules = [
    Cron("7 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_relatoria_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_relatoria_download(download_params: dict) -> None:
    _relatoria_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_relatoria_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, RELATORIA_TABLE_ID
)
_relatoria_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_relatoria_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# votacao_comissao  (Grupo B)
# check_update: br_senado_dados_abertos__votacao_comissao
# extract_and_load: br_senado_dados_abertos__votacao_comissao
# ──────────────────────────────────────────────────────────────────────────────

_votacao_comissao_pipeline = make_pipeline_group_b(VOTACAO_COMISSAO_TABLE_ID)


@flow(name=_votacao_comissao_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_votacao_comissao_check_update() -> None:
    _votacao_comissao_pipeline.run_check_update()


br_senado_dados_abertos_votacao_comissao_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, VOTACAO_COMISSAO_TABLE_ID)
)
br_senado_dados_abertos_votacao_comissao_check_update.deploy_schedules = [
    Cron("15 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_comissao_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_senado_dados_abertos_votacao_comissao_download(
    download_params: dict,
) -> None:
    _votacao_comissao_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_votacao_comissao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, VOTACAO_COMISSAO_TABLE_ID
)
_votacao_comissao_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_comissao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# votacao_comissao_parlamentar  (Grupo B)
# check_update: br_senado_dados_abertos__votacao_comissao_parlamentar
# extract_and_load: br_senado_dados_abertos__votacao_comissao_parlamentar
# ──────────────────────────────────────────────────────────────────────────────

_votacao_comissao_parlamentar_pipeline = make_pipeline_group_b(
    VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID
)


@flow(
    name=_votacao_comissao_parlamentar_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_comissao_parlamentar_check_update() -> (
    None
):
    _votacao_comissao_parlamentar_pipeline.run_check_update()


br_senado_dados_abertos_votacao_comissao_parlamentar_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID
)
br_senado_dados_abertos_votacao_comissao_parlamentar_check_update.deploy_schedules = [
    Cron("23 10 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_votacao_comissao_parlamentar_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_senado_dados_abertos_votacao_comissao_parlamentar_download(
    download_params: dict,
) -> None:
    _votacao_comissao_parlamentar_pipeline.run_extract_and_load(
        download_params
    )


br_senado_dados_abertos_votacao_comissao_parlamentar_download.deploy_tags = (
    deploy_tags(
        DATASET_ID,
        Etapa.EXTRACT_AND_LOAD,
        VOTACAO_COMISSAO_PARLAMENTAR_TABLE_ID,
    )
)
_votacao_comissao_parlamentar_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_votacao_comissao_parlamentar_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# discurso  (Grupo B)
# check_update: br_senado_dados_abertos__discurso
# extract_and_load: br_senado_dados_abertos__discurso
# ──────────────────────────────────────────────────────────────────────────────

_discurso_pipeline = make_pipeline_group_b(DISCURSO_TABLE_ID)


@flow(name=_discurso_pipeline.check_update_flow_name, log_prints=True)
def br_senado_dados_abertos_discurso_check_update() -> None:
    _discurso_pipeline.run_check_update()


br_senado_dados_abertos_discurso_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DISCURSO_TABLE_ID
)
br_senado_dados_abertos_discurso_check_update.deploy_schedules = [
    Cron("31 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_discurso_pipeline.extract_and_load_flow_name, log_prints=True)
def br_senado_dados_abertos_discurso_download(download_params: dict) -> None:
    _discurso_pipeline.run_extract_and_load(download_params)


br_senado_dados_abertos_discurso_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DISCURSO_TABLE_ID
)
_discurso_pipeline.extract_load_deployment = (
    br_senado_dados_abertos_discurso_download.fn.__name__
)
