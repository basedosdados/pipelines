"""
Tasks for br_ibge_ipca.
"""

from collections.abc import Callable
from datetime import date

from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates,
    collect_data_utils,
    json_to_csv,
)
from pipelines.datasets.br_ibge_ipca.constants import DATASET_ID
from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# As 4 tabelas (issue #1867) — ver constants.py
#
# Particularidade deste dataset: `check_for_update` não é uma checagem
# leve independente — a única forma de descobrir a data mais recente é
# baixar o período mais novo da API do IBGE e inspecionar o JSON
# (`check_for_updates`/`get_date_api`, que lê o arquivo que
# `collect_data_utils` acabou de escrever). Como `check_update` e
# `download` rodam em pods separados, `download_data` busca o mesmo
# período de novo (`collect_data_utils` com o período já resolvido) em vez
# de tentar repassar o JSON entre pods — o download é pequeno o bastante
# (1 período, poucas variáveis) pra baixar duas vezes sem problema (ver
# critério do limiar de 5 GB, `levantamento-datasets-por-categoria-de-check.md`
# no ftwca).
#
# `make_check_for_update`/`make_download_data` são fábricas parametrizadas
# por `table_id` — a lógica é idêntica pras 4 tabelas (só `geo_level`/
# `classificacao` mudam dentro de `collect_data_utils`/`json_to_csv`, já
# tratado lá). Diferente dos `@flow` em `flows.py`, não tem restrição de
# nome aqui — os callables viram atributos de instância de
# `CheckThenDownloadPipeline`, nunca são introspectados por `__name__`.
# ──────────────────────────────────────────────────────────────────────────────


def make_check_for_update(table_id: str) -> Callable[[], CheckResult]:
    def check_for_update() -> CheckResult:
        collect_data_utils(
            dataset_id=DATASET_ID, table_id=table_id, periodo=None
        )
        reference_date = check_for_updates(
            dataset_id=DATASET_ID, table_id=table_id
        )
        # pyrefly: ignore [bad-argument-type]
        return CheckResult(reference_date=reference_date)

    return check_for_update


def make_download_data(table_id: str) -> Callable[[dict], DownloadResult]:
    def download_data(download_params: dict) -> DownloadResult:
        ref = date.fromisoformat(download_params["reference_date"])
        periodo = f"{ref.year}{ref.month:02d}"

        collect_data_utils(
            dataset_id=DATASET_ID, table_id=table_id, periodo=periodo
        )
        filepath = json_to_csv(table_id=table_id, dataset_id=DATASET_ID)

        return DownloadResult(
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ).model_dump(),
            data_path=filepath,
            bq_project="basedosdados",
            partition_folders=[f"ano={ref.year}/mes={ref.month:02d}"],
        )

    return download_data
