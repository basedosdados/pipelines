"""
Tasks compartilhadas — Prefect 3.
"""

import json
from pathlib import Path
from typing import Any

import basedosdados as bd
from dbt.cli.main import dbtRunner
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
    tb = bd.Table(
        dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name
    )
    st = bd.Storage(
        dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name
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
