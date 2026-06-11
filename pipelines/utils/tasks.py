"""
Tasks compartilhadas — Prefect 3.
"""

import json
from pathlib import Path
from time import sleep
from typing import Any

import basedosdados as bd
from basedosdados.download.download import _google_client
from dbt.cli.main import dbtRunner
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import TableReference
from prefect import task

from pipelines.utils.gcs import DBTArtifactUploader, dump_header
from pipelines.utils.vault import get_credentials_from_secret

# ──────────────────────────────────────────────────────────────────────────────
# Vault
# ──────────────────────────────────────────────────────────────────────────────


@task(retries=3, retry_delay_seconds=10)
def get_credentials(secret_path: str) -> dict:
    """Retorna credenciais do Vault para o caminho informado."""
    return get_credentials_from_secret(secret_path)


# ──────────────────────────────────────────────────────────────────────────────
# Flow run
# ──────────────────────────────────────────────────────────────────────────────


@task
async def rename_flow_run_dataset_table(
    prefix: str, dataset_id: str, table_id: str
) -> None:
    """Renomeia o flow run na UI do Prefect com o padrão '<prefix><dataset_id>.<table_id>'."""
    from prefect.client.orchestration import get_client
    from prefect.runtime import flow_run as flow_run_ctx

    async with get_client() as client:
        await client.update_flow_run(
            flow_run_id=flow_run_ctx.id,
            name=f"{prefix}{dataset_id}.{table_id}",
        )


# ──────────────────────────────────────────────────────────────────────────────
# Upload GCS
# ──────────────────────────────────────────────────────────────────────────────


def _upload_to_gcs(
    data_path: str | Path,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    dump_mode: str = "append",
    source_format: str = "csv",
) -> None:
    # billing_project_id casado com o bucket: a SA do pod (prefect@basedosdados-dev
    # ou prefect@basedosdados) tem `serviceusage.services.use` apenas no próprio
    # projeto; o default `basedosdados-staging` da basedosdados lib dispara 403
    # em `blob.exists()` quando user_project não coincide com o projeto do bucket.
    billing_project_id = bucket_name
    tb = bd.Table(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=billing_project_id,
    )
    st = bd.Storage(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        billing_project_id=billing_project_id,
    )
    storage_link = (
        f"https://console.cloud.google.com/storage/browser/"
        f"{bucket_name}/staging/{dataset_id}/{table_id}"
    )

    if dump_mode == "append":
        if not tb.table_exists(mode="staging"):
            header_path = dump_header(
                data_path=data_path, source_format=source_format
            )
            tb.create(
                path=header_path,
                if_storage_data_exists="replace",
                if_table_exists="replace",
                source_format=source_format,
            )
            st.delete_table(
                mode="staging", bucket_name=bucket_name, not_found_ok=True
            )
            print(
                f"Tabela criada: {tb.table_full_name['staging']}\n{storage_link}"
            )
        else:
            print(f"Tabela já existe: {tb.table_full_name['staging']}")

    elif dump_mode == "overwrite":
        if tb.table_exists(mode="staging"):
            st.delete_table(
                mode="staging", bucket_name=bucket_name, not_found_ok=True
            )
            tb.delete(mode="all")
            print(f"Tabela anterior removida: {tb.table_full_name['staging']}")
        header_path = dump_header(
            data_path=data_path, source_format=source_format
        )
        tb.create(
            path=header_path,
            if_storage_data_exists="replace",
            if_table_exists="replace",
            source_format=source_format,
        )
        st.delete_table(
            mode="staging", bucket_name=bucket_name, not_found_ok=True
        )
        print(
            f"Tabela recriada: {tb.table_full_name['staging']}\n{storage_link}"
        )

    else:
        raise ValueError(
            f"dump_mode inválido: {dump_mode!r}. Use 'append' ou 'overwrite'."
        )

    if not tb.table_exists(mode="staging"):
        raise RuntimeError(
            "Tabela não existe em staging após criação — upload abortado."
        )

    st.upload(path=data_path, mode="staging", if_exists="replace")
    print(
        f"Upload concluído: gs://{bucket_name}/staging/{dataset_id}/{table_id}"
    )


@task(retries=3, retry_delay_seconds=30)
def upload_to_gcs(
    data_path: str | Path,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    dump_mode: str = "append",
    source_format: str = "csv",
) -> None:
    """Faz upload dos dados para o bucket GCS e cria a tabela staging no BQ se necessário."""
    _upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        dump_mode=dump_mode,
        source_format=source_format,
    )


# ──────────────────────────────────────────────────────────────────────────────
# dbt
# ──────────────────────────────────────────────────────────────────────────────


@task(retries=1, retry_delay_seconds=10)
def run_dbt(
    dataset_id: str,
    table_id: str | None = None,
    dbt_alias: bool = True,
    dbt_command: str = "run/test",
    target: str = "dev",
    flags: str | None = None,
    _vars: dict[str, Any] | str | None = None,
) -> None:
    """
    Executa um modelo dbt (run e/ou test) e sobe os artefatos ao GCS ao final.

    dbt_command aceita: "run", "test", "run/test", "run and test"
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"dbt_command inválido: {dbt_command!r}")

    models_folder = Path("models") / dataset_id
    if table_id is not None:
        selected = models_folder / (
            f"{dataset_id}__{table_id}.sql" if dbt_alias else f"{table_id}.sql"
        )
    else:
        selected = models_folder

    if not selected.is_dir() and not selected.exists():
        raise FileNotFoundError(f"Modelo dbt não encontrado: {selected}")
    if selected.is_dir() and not any(selected.iterdir()):
        raise ValueError(f"Diretório de modelos vazio: {selected}")

    if target == "prod":
        with open("/credentials-prod/prod.json") as f:
            sa = json.loads(f.read())
        print(
            f"dbt target=prod | project={sa['project_id']} | account={sa['client_email']}"
        )

    vars_dict = json.loads(_vars) if isinstance(_vars, str) else (_vars or {})

    commands = []
    if "run" in dbt_command:
        commands.append("run")
    if "test" in dbt_command:
        commands.append("test")

    try:
        runner = dbtRunner()
        for cmd in commands:
            cli_args = [
                cmd,
                "--select",
                selected.as_posix(),
                "--target",
                target,
            ]
            if flags and flags.startswith("--full-refresh") and cmd == "run":
                cli_args.insert(1, "--full-refresh")
            elif flags:
                cli_args.extend(flags.split())
            if vars_dict:
                cli_args.extend(["--vars", json.dumps(vars_dict)])

            print(f"dbt {' '.join(cli_args)}")
            result = runner.invoke(cli_args)

            if result.exception:
                raise Exception(f"dbt {cmd} exception: {result.exception}")
            if not result.success:
                raise Exception(
                    f"dbt {cmd} falhou para {selected.as_posix()} (target={target})"
                )
            print(f"dbt {cmd} OK: {selected.as_posix()} (target={target})")
    finally:
        try:
            DBTArtifactUploader(
                dataset_id=dataset_id, table_id=table_id, target=target
            ).run()
        except Exception as e:
            print(f"Aviso: falha ao subir artefatos dbt: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Download BQ → GCS
# ──────────────────────────────────────────────────────────────────────────────


def _execute_query_in_bigquery(
    billing_project_id: str, query: str, path: str, location: str = "US"
) -> None:
    client = _google_client(billing_project_id, from_file=True, reauth=False)
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)
    dest = job._properties["configuration"]["query"]["destinationTable"]
    dataset_ref = bigquery.DatasetReference(
        dest["projectId"], dest["datasetId"]
    )
    table_ref = dataset_ref.table(dest["tableId"])
    job_config = bigquery.job.ExtractJobConfig(compression="GZIP")
    extract_job = client["bigquery"].extract_table(
        table_ref, path, location=location, job_config=job_config
    )
    extract_job.result()


@task(retries=2, retry_delay_seconds=30)
def download_data_to_gcs(
    dataset_id: str,
    table_id: str,
    project_id: str = "basedosdados",
    bd_project_mode: str = "prod",
    billing_project_id: str | None = None,
    location: str = "US",
) -> None:
    """Exporta tabela do BigQuery para GCS como CSV.gz.

    Regras:
    - > 1 GB: sem download
    - 100 MB - 1 GB: apenas BDPro
    - < 100 MB: open + BDPro (se tiver row access policy bdpro_filter)
    """
    from pipelines.utils.utils import log

    if not billing_project_id:
        billing_project_id = project_id

    client = _google_client(billing_project_id, from_file=True, reauth=False)

    bq_table_ref = TableReference.from_string(
        f"{project_id}.{dataset_id}.{table_id}"
    )
    bq_table = client["bigquery"].get_table(bq_table_ref)
    num_bytes = bq_table.num_bytes
    log(f"Tamanho da tabela: {num_bytes} bytes ({bq_table.num_rows} linhas)")

    # Views retornam num_bytes=0 — consulta o tamanho via Django API
    if num_bytes == 0:
        log("Tabela é uma view, consultando tamanho via API Django")
        b = bd.Backend(
            graphql_url="https://api.basedosdados.org/api/v1/graphql"
        )
        django_table_id = b._get_table_id_from_name(
            gcp_dataset_id=dataset_id, gcp_table_id=table_id
        )
        data = b._execute_query(
            f"""query {{ allTable(id: "{django_table_id}") {{
                edges {{ node {{ uncompressedFileSize }} }} }} }}"""
        )
        items = data.get("allTable", {}).get("items", [])
        if not items:
            log("Tabela não encontrada na API — download ignorado")
            return
        num_bytes = items[0]["uncompressedFileSize"] or 0

    if num_bytes > 1_000_000_000:
        log("Tabela > 1 GB — sem download disponível")
        return

    url_paths = get_credentials_from_secret("url_download_data")
    url_open = url_paths["URL_DOWNLOAD_OPEN"]
    url_closed = url_paths["URL_DOWNLOAD_CLOSED"]
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

    if num_bytes >= 100_000_000:
        log("Tabela entre 100 MB e 1 GB — apenas BDPro")
        _execute_query_in_bigquery(
            billing_project_id,
            query,
            f"{url_closed}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz",
            location,
        )
        return

    # < 100 MB: open + BDPro se houver filtro
    bdpro = False
    try:
        drop_policy = f"DROP ROW ACCESS POLICY bdpro_filter ON `{project_id}.{dataset_id}.{table_id}`"
        job = client["bigquery"].query(drop_policy)
        while not job.done():
            sleep(1)
        job.result()
        bdpro = True
        log("Row access policy bdpro_filter removida temporariamente")
    except NotFound:
        log("Sem row access policy bdpro_filter — todos os dados são abertos")
    except Exception as e:
        raise ValueError(f"Erro ao remover bdpro_filter: {e}") from e

    log("Exportando dados abertos")
    _execute_query_in_bigquery(
        billing_project_id,
        query,
        f"{url_open}{dataset_id}/{table_id}/{table_id}.csv.gz",
        location,
    )
    log("Exportação open concluída")

    if bdpro:
        restore_policy = (
            f"CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter "
            f"ON `{project_id}.{dataset_id}.{table_id}` "
            f'GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") '
            f"FILTER USING (TRUE)"
        )
        job = client["bigquery"].query(restore_policy)
        while not job.done():
            sleep(1)
        log("Row access policy bdpro_filter restaurada")

        log("Exportando dados BDPro")
        _execute_query_in_bigquery(
            billing_project_id,
            query,
            f"{url_closed}{dataset_id}/{table_id}/{table_id}_bdpro.csv.gz",
            location,
        )
        log("Exportação BDPro concluída")
