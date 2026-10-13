"""
Tasks for br_rf_cnpj — Prefect 3.
"""

import asyncio
import datetime
from collections.abc import Callable
from pathlib import Path

from prefect import task

from pipelines.datasets.br_rf_cnpj.constants import constants as constants_cnpj
from pipelines.datasets.br_rf_cnpj.utils import (
    build_paths,
    data_url,
    download_unzip_csv,
    get_table_files,
    process_csv_dicionario,
    process_csv_empresas,
    process_csv_estabelecimentos,
    process_csv_simples,
    process_csv_socios,
    process_manual_dictionaries,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value

DATASET_ID = "br_rf_cnpj"

# simples/dicionario não têm baseline de coverage confiável (não são
# particionadas por competência) — comparam contra Table.Update em vez de
# Coverage. Mesma decisão do flow monolítico antigo.
_NON_HISTORICAL_TABLE_IDS = ("simples", "dicionario")

_COVERAGE_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="data_referencia"),
    date_format=DateFormat.YEAR_MD,
)
_COVERAGE_NON_HISTORICAL = NonHistorical()


@task(retries=3, retry_delay_seconds=30)
def get_data_source_max_date(
    folder_date: str | None = None,
) -> tuple[str, datetime.date]:
    """
    Checks if there are available updates for a specific dataset and table.

    Returns:
        tuple: Returns a tuple with the date extracted from the CNPJs API folder and today date
        to be used as partition
    """

    folder_date, last_modified_date = data_url(
        url=url, folder_date=folder_date
    )
    return folder_date, last_modified_date


@task(retries=3, retry_delay_seconds=30)
def main(
    tables: list[str],
    folder_date: str,
    last_modified_date: datetime.date,
    chunk_size: int = 100000,
    download_chunk_size: int = 15 * 1024 * 1024,
    download_max_retries: int = 5,
    download_max_parallel: int = 15,
    download_timeout: int = 5 * 60,
) -> Path:
    """
    Performs the download, processing, and organization of CNPJ data.

    Args:
        tables (list): A list of tables to be processed.
        folder_date (datetime | str): CNPJs max folder date
        last_modified_date (datetime | str): CNPJs max last modified date
        chunk_size (int): size of csv chunks

    Returns:
        str: The path to the output folder where the data has been organized.
    """
    arquivos_baixados = []  # List to track already downloaded files
    for table in tables:
        table_configs = constants_cnpj.TABLE_CONFIGS.value[table]

        # Creates dataset table paths (input and output)

        if table_configs["dicionario"]:
            if table_configs["manual"] is False:
                input_path, _ = build_paths(table_id=table, build_output=False)
            _, output_path = build_paths(
                table_id="dicionario", build_input=False
            )
        else:
            input_path, output_path = build_paths(table_id=table)

        if table_configs["segmentada"]:
            files = get_table_files(
                table_configs["table_name"],
                f"{constants_cnpj.URL.value}{folder_date}",
            )
            for i, item in enumerate(files):
                nome_arquivo = item[0]
                url_download = item[1]

                if nome_arquivo not in arquivos_baixados:
                    arquivos_baixados.append(nome_arquivo)
                    asyncio.run(
                        download_unzip_csv(
                            url_download,
                            # pyrefly: ignore [bad-argument-type]
                            # pyrefly: ignore [unbound-name]
                            input_path,
                            chunk_size=download_chunk_size,
                            max_retries=download_max_retries,
                            max_parallel=download_max_parallel,
                            timeout=download_timeout,
                        )
                    )

                    if table_configs["table_name"] == "Estabelecimentos":
                        process_csv_estabelecimentos(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )

                    elif table_configs["table_name"] == "Socios":
                        process_csv_socios(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
                    elif table_configs["table_name"] == "Empresas":
                        process_csv_empresas(
                            # pyrefly: ignore [bad-argument-type]
                            input_path,
                            # pyrefly: ignore [bad-argument-type]
                            output_path,
                            folder_date,
                            last_modified_date,
                            i,
                            chunk_size,
                        )
        else:
            nome_arquivo = f"{table_configs['table_name']}"
            url_download = f"{constants_cnpj.URL.value}{folder_date}/{table_configs['table_name']}.zip"

            if (nome_arquivo not in arquivos_baixados) and not table_configs[
                "manual"
            ]:
                arquivos_baixados.append(nome_arquivo)
                # pyrefly: ignore [bad-argument-type]
                # pyrefly: ignore [unbound-name]
                asyncio.run(download_unzip_csv(url_download, input_path))
                log(f"Nome Arquivo: {nome_arquivo}")

            if table_configs["dicionario"]:
                if table_configs["manual"]:
                    # pyrefly: ignore [bad-argument-type]
                    process_manual_dictionaries(output_path, table)
                else:
                    # pyrefly: ignore [bad-argument-type]
                    process_csv_dicionario(input_path, output_path, table)
            elif table_configs["table_name"] == "Simples":
                process_csv_simples(
                    # pyrefly: ignore [bad-argument-type]
                    input_path,
                    # pyrefly: ignore [bad-argument-type]
                    output_path,
                    folder_date,
                    last_modified_date,
                    table,
                    chunk_size,
                )
    # pyrefly: ignore[bad-return]
    # pyrefly: ignore [unbound-name]
    return output_path


# ──────────────────────────────────────────────────────────────────────────────
# As 5 tabelas (empresas, socios, estabelecimentos, simples, dicionario) —
# ver `constants.TABLES`/`constants.TABLE_CONFIGS`. `make_get_latest_update`/
# `make_extract_load_data` são fábricas parametrizadas por `table_id`: a
# checagem (`get_data_source_max_date`, leitura de índice via PROPFIND, sem
# baixar arquivo nenhum) é idêntica pras 5; o download (`main`) e a coverage
# mudam conforme `table_id` (simples/dicionario são NonHistorical e
# comparam contra Table.Update; as demais são PartBdpro particionadas por
# `data_referencia` e comparam contra Coverage).
# ──────────────────────────────────────────────────────────────────────────────


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    def get_latest_update() -> SourceInspection:
        folder_date, last_modified_date = get_data_source_max_date()
        compare_against = (
            "table_update"
            if table_id in _NON_HISTORICAL_TABLE_IDS
            else "coverage"
        )
        return SourceInspection(
            reference_date=last_modified_date,
            extra_download_params={"folder_date": folder_date},
            compare_against=compare_against,
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        folder_date = download_params["folder_date"]
        last_modified_date = datetime.date.fromisoformat(
            download_params["reference_date"]
        )

        tabelas = constants_cnpj.TABLES.value[table_id]
        output_filepath = main(
            tables=tabelas,
            folder_date=folder_date,
            last_modified_date=last_modified_date,
        )

        coverage = (
            _COVERAGE_NON_HISTORICAL
            if table_id in _NON_HISTORICAL_TABLE_IDS
            else _COVERAGE_PART_BDPRO
        )

        return ExtractAndLoad(
            coverage=coverage.model_dump(),
            data_path=str(output_filepath),
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
