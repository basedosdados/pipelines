"""
Tasks for br_ms_cnes.
"""

from collections.abc import Callable

from pipelines.crawler.datasus.tasks import (
    access_ftp_download_files_async,
    check_files_to_parse,
    decompress_dbc,
    decompress_dbf,
    get_datasus_source_max_date,
    pre_process_files,
)
from pipelines.datasets.br_ms_cnes.constants import DATASET_ID
from pipelines.utils.metadata.domain import DateFormat, PartBdpro, YearMonth
from pipelines.utils.metadata.tasks import task_get_api_most_recent_date
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult

# ──────────────────────────────────────────────────────────────────────────────
# As 13 tabelas (issue #1867) — ver constants.py
#
# Check_for_update é uma listagem FTP leve de verdade (`check_files_to_parse`
# -> `list_datasus_dbc_files` -> `ftp.nlst(...)`, sem baixar nenhum arquivo —
# diferente de br_ibge_ipca, aqui o check é genuinamente independente do
# download). A lista de arquivos FTP descobertos (`ftp_files`) é repassada
# pra `download_data` via `CheckResult.extra_download_params` — exatamente o
# caso de uso que esse campo foi desenhado pra cobrir (informação descoberta
# no check que o download precisa, não previsível de antemão).
#
# `get_datasus_source_max_date` devolve `None` quando não há arquivo novo no
# FTP pro próximo período — nesse caso caímos pra
# `task_get_api_most_recent_date` (a própria coverage atual) como
# `reference_date`, garantindo que `poll_source_for_update_task` compare data
# igual a data e conclua corretamente "sem dado novo" (mesmo efeito do
# `if not ftp_files` do flow antigo, mas sem violar o tipo de `CheckResult.
# reference_date`, que não aceita `None`).
# ──────────────────────────────────────────────────────────────────────────────


def make_check_for_update(table_id: str) -> Callable[[], CheckResult]:
    def check_for_update() -> CheckResult:
        ftp_files = check_files_to_parse(
            dataset_id=DATASET_ID,
            table_id=table_id,
            year_month_to_extract="",
        )
        source_max_date = get_datasus_source_max_date(ftp_files)
        if source_max_date is None:
            source_max_date = task_get_api_most_recent_date(
                dataset_id=DATASET_ID,
                table_id=table_id,
                date_format="%Y-%m",
            )

        return CheckResult(
            reference_date=source_max_date,
            extra_download_params={"ftp_files": ftp_files},
        )

    return check_for_update


def make_download_data(table_id: str) -> Callable[[dict], DownloadResult]:
    def download_data(download_params: dict) -> DownloadResult:
        ftp_files = download_params["ftp_files"]

        dbc_files = access_ftp_download_files_async(
            file_list=ftp_files, dataset_id=DATASET_ID, table_id=table_id
        )
        decompress_dbc(file_list=dbc_files, dataset_id=DATASET_ID)
        csv_files = decompress_dbf(file_list=dbc_files, table_id=table_id)
        # decompress_dbf está anotado -> str mas devolve list (mesmo bug de
        # tipo pré-existente em crawler/datasus/flows.py::_run_cnes).
        # pyrefly: ignore [no-matching-overload]
        files_path = pre_process_files(
            file_list=csv_files, dataset_id=DATASET_ID, table_id=table_id
        )

        return DownloadResult(
            coverage=PartBdpro(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ).model_dump(),
            data_path=files_path,
            bq_project="basedosdados",
            # `pre_process_files` grava parquet — sem declarar o formato, o
            # `dump_header` chamado por `_sync_staging_schema` procura .csv e
            # não encontra nada (mesmo aviso já existente no flow antigo).
            source_format="parquet",
        )

    return download_data
