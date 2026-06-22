"""
Script de deploy de flows para o Prefect 3.

Uso:
  # Deploy de flows alterados em um PR (dev)
  python deploy_flows.py --pool basedosdados-dev --branch feat/meu-flow --files pipelines/datasets/meu_dataset/flows.py

  # Deploy de todos os flows (prod, ao mergear na main)
  python deploy_flows.py --pool basedosdados --branch main --all
"""

import argparse
import importlib.util
import os
import sys
from pathlib import Path

from prefect import Flow
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

REPO_URL = "https://github.com/basedosdados/pipelines.git"


def load_flows_from_file(file_path: str) -> dict[str, Flow]:
    """
    Importa dinamicamente um arquivo Python e retorna os flows Prefect 3 encontrados.
    Arquivos que ainda usam Prefect 0.15.9 vão falhar na importação e serão pulados.
    """
    path = Path(file_path)
    module_name = path.stem

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        print(f"  Pulando {file_path}: não foi possível carregar o spec.")
        return {}

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    except ImportError as e:
        print(
            f"  Pulando {file_path}: ImportError (provavelmente Prefect 0.x) — {e}"
        )
        return {}
    except Exception as e:
        print(f"  Pulando {file_path}: erro ao carregar — {e}")
        return {}

    flows = {}
    for name, obj in vars(module).items():
        if isinstance(obj, Flow) and obj.fn.__code__.co_filename == str(
            path.resolve()
        ):
            flows[name] = obj

    return flows


def deploy_flow(
    flow: Flow,
    flow_name: str,
    file_path: str,
    pool_name: str,
    branch_name: str,
) -> bool:
    entrypoint = f"{file_path}:{flow_name}"
    is_dev = "dev" in pool_name

    schedules = getattr(flow, "deploy_schedules", None)
    if is_dev:
        schedules = None  # flows em dev não têm schedule
    elif schedules:
        # Convert dict {"cron": "...", "timezone": "..."} to Cron schedule objects
        schedules = [
            Cron(s["cron"], timezone=s.get("timezone", "UTC"))
            if isinstance(s, dict)
            else s
            for s in schedules
        ]

    job_variables = getattr(flow, "job_variables", None)

    print(f"  Registrando {flow_name} → {entrypoint}")

    try:
        flow.from_source(
            source=GitRepository(
                url=REPO_URL,
                branch=branch_name,
            ),
            entrypoint=entrypoint,
        ).deploy(
            name=flow_name,
            work_pool_name=pool_name,
            tags=["automated-deploy"],
            schedules=schedules,
            job_variables=job_variables,
            build=False,
        )
        status = (
            "(sem schedule)"
            if not schedules
            else f"com schedules: {schedules}"
        )
        print(f"  ✓ {flow_name} registrado {status}")
        return True
    except Exception as e:
        print(f"  ✗ Falha ao registrar {flow_name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Deploy de flows Prefect 3")
    parser.add_argument("--pool", required=True, help="Nome do Work Pool")
    parser.add_argument(
        "--branch", required=True, help="Branch do repositório"
    )
    parser.add_argument(
        "--files", nargs="*", help="Arquivos específicos para deploy"
    )
    parser.add_argument(
        "--all", action="store_true", help="Deploy de todos os flows"
    )
    args = parser.parse_args()

    files_to_process = []

    if args.all:
        for root, _, files in os.walk("pipelines"):
            for file in files:
                if file.endswith(".py") and file != "__init__.py":
                    files_to_process.append(os.path.join(root, file))
    elif args.files:
        files_to_process = args.files
    else:
        print("Nenhum arquivo especificado. Use --files ou --all.")
        sys.exit(0)

    print(f"\nWork Pool : {args.pool}")
    print(f"Branch    : {args.branch}")
    print(f"Arquivos  : {len(files_to_process)}\n")

    success, skipped, failed = 0, 0, 0

    for file_path in files_to_process:
        if not os.path.exists(file_path):
            continue

        print(f"→ {file_path}")
        flows = load_flows_from_file(file_path)

        if not flows:
            skipped += 1
            continue

        for name, flow_obj in flows.items():
            ok = deploy_flow(flow_obj, name, file_path, args.pool, args.branch)
            if ok:
                success += 1
            else:
                failed += 1

    print(
        f"\nResultado: {success} registrados, {skipped} pulados, {failed} com erro"
    )

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
