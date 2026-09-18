"""
Script de deploy de flows para o Prefect 3.

Uso:
  # Deploy de arquivos específicos, sem expansão (uso manual)
  python deploy_flows.py --pool basedosdados-dev --branch feat/meu-flow --files pipelines/datasets/meu_dataset/flows.py

  # Deploy a partir de uma lista de arquivos alterados (CI, dev e prod) —
  # expande pra pasta inteira do dataset e escala pra --all quando a
  # mudança é em infra compartilhada (ver expand_changed_files)
  python deploy_flows.py --pool basedosdados-dev --branch feat/meu-flow --changed pipelines/datasets/meu_dataset/tasks.py

  # Deploy de todos os flows (recuperação manual, ex. depois de um drift)
  python deploy_flows.py --pool basedosdados --branch main --all

Nome do deployment: em prod, é `<flow_name>` (mesmo nome de sempre — não
mude, `sync-deployments`/`set_deployment_schedule_active` no backend
dependem disso). Em dev, é `dev-<flow_name>`, um registro separado do de
prod — nunca compartilham o mesmo nome, pra um deploy de PR não "roubar"
o deployment de prod movendo-o pro pool dev.
"""

import argparse
import glob
import importlib.util
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from prefect import Flow
from prefect.runner.storage import GitRepository
from prefect.schedules import Cron

REPO_URL = "https://github.com/basedosdados/pipelines.git"

# Pastas cuja mudança pode afetar deploy de flows em mais de um dataset
# (lógica compartilhada, ex. CheckThenDownloadPipeline em stage_dispatch.py,
# ou um crawler usado por vários datasets como pipelines/crawler/datasus) —
# não dá pra saber quais datasets são afetados sem reprocessar tudo, então
# escala pra --all nesse caso, em vez de arriscar deixar algo desatualizado.
SHARED_PREFIXES = ("pipelines/utils/", "pipelines/crawler/")


def all_python_files() -> list[str]:
    """Lista todo arquivo `.py` de `pipelines/`, exceto `__init__.py`.

    Usado tanto por `--all` quanto como fallback de
    `expand_changed_files()` quando a mudança exige redeploy completo.

    Returns:
        Caminhos relativos à raiz do repo de todo `.py` em `pipelines/`,
        exceto `__init__.py`.
    """
    files = []
    for root, _, filenames in os.walk("pipelines"):
        for filename in filenames:
            if filename.endswith(".py") and filename != "__init__.py":
                files.append(os.path.join(root, filename))
    return files


def expand_changed_files(changed: list[str]) -> list[str] | None:
    """Expande uma lista de arquivos alterados pra decidir o que deployar.

    `deploy_flow()` só registra um flow se o arquivo processado contém
    literalmente um objeto `Flow` — `tasks.py`/`constants.py` nunca
    definem `@flow`. Mas `deploy_tags`/`job_variables`/`deploy_schedules`
    de um flow costumam ser computados a partir de constantes de
    `constants.py` e atribuídos dentro do `flows.py` na hora da
    importação (ver `pipelines/datasets/br_ibge_ipca/flows.py`). Se só
    `constants.py` mudar, sem `flows.py` mudar junto, o deploy seletivo
    "não veria" a mudança e o flow ficaria com metadado desatualizado no
    Prefect — silenciosamente, sem erro. Por isso, mudança em qualquer
    arquivo de `pipelines/datasets/<dataset>/` expande pra todos os `.py`
    daquela pasta, recursivamente, não só o que mudou.

    Args:
        changed: caminhos de arquivos `.py` alterados, relativos à raiz
            do repo (ex. saída do `tj-actions/changed-files`).

    Returns:
        Lista ordenada e sem duplicatas de arquivos a passar pro deploy,
        com cada mudança em `pipelines/datasets/<dataset>/` expandida pra
        todos os `.py` daquele dataset. `None` se algum arquivo alterado
        estiver em `SHARED_PREFIXES` — sinal pra quem chamar escalar pra
        `--all`, já que o impacto não é computável sem reprocessar tudo.
    """
    expanded: set[str] = set()

    for file_path in changed:
        normalized = file_path.replace(os.sep, "/")

        if normalized.startswith(SHARED_PREFIXES):
            return None

        parts = normalized.split("/")
        if parts[:2] == ["pipelines", "datasets"] and len(parts) > 2:
            dataset_dir = "/".join(parts[:3])
            expanded.update(
                glob.glob(f"{dataset_dir}/**/*.py", recursive=True)
            )
        else:
            expanded.add(file_path)

    return sorted(expanded)


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
) -> tuple[bool, str]:
    """Registra um flow no Prefect 3.

    Não imprime nada diretamente — devolve a mensagem pronta pra quem
    chamar imprimir. Isso é o que permite chamar essa função de várias
    threads ao mesmo tempo (`main()`, via `ThreadPoolExecutor`) sem
    linhas de saída de flows diferentes se misturando no meio.

    Args:
        flow: Objeto `Flow` do Prefect já carregado do arquivo.
        flow_name: Nome do flow.
        file_path: Caminho do arquivo onde o flow foi encontrado.
        pool_name: Work Pool de destino (`basedosdados` ou `basedosdados-dev`).
        branch_name: Branch do repositório a partir da qual o Prefect vai
            ler o código do flow em runtime.

    Returns:
        Tupla `(sucesso, mensagem)` pronta pra impressão.
    """
    entrypoint = f"{file_path}:{flow_name}"
    is_dev = "dev" in pool_name

    # O Prefect identifica um deployment por `<flow>/<name>`, não pelo work
    # pool — `work_pool_name` é só um campo mutável do mesmo registro. Usar
    # o mesmo `name` em prod e dev faz o deploy de uma PR "roubar" o
    # deployment de prod, movendo-o pro pool dev e zerando o schedule (ver
    # issue de colisão de nomes). O prefixo `dev-` garante que cada ambiente
    # tenha seu próprio registro, sem nunca competir pelo mesmo pool.
    deployment_name = f"dev-{flow_name}" if is_dev else flow_name

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

    try:
        flow.from_source(
            source=GitRepository(
                url=REPO_URL,
                branch=branch_name,
            ),
            entrypoint=entrypoint,
        ).deploy(
            name=deployment_name,
            work_pool_name=pool_name,
            tags=["automated-deploy"],
            schedules=schedules,
            job_variables=job_variables,
            build=False,
            paused=True,  # schedules activated by backend sync (prod) or manually (dev)
        )
        status = (
            "(sem schedule)"
            if not schedules
            else f"com schedules: {schedules}"
        )
        return True, f"  ✓ {deployment_name} registrado {status}"
    except Exception as e:
        return False, f"  ✗ Falha ao registrar {deployment_name}: {e}"


def _positive_int(value: str) -> int:
    """Valida `--workers` como argparse `type=`.

    `ThreadPoolExecutor(max_workers=...)` levanta `ValueError` não tratado
    pra qualquer valor <= 0, derrubando o script antes de registrar
    qualquer flow. Rejeitar aqui, no parse, dá um erro de CLI claro em vez
    disso.

    Args:
        value: String recebida da linha de comando.

    Returns:
        O valor convertido pra `int`.

    Raises:
        argparse.ArgumentTypeError: Se `value` não for um inteiro positivo.
    """
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError(
            f"--workers precisa ser um inteiro positivo, recebeu {value!r}"
        )
    return parsed


def main() -> None:
    """Ponto de entrada da CLI.

    Lê os argumentos de linha de comando (`--pool`, `--branch`, e
    exatamente um de `--files`/`--changed`/`--all`) e deploya os flows
    selecionados. Sai com código 0 sem deployar nada se nenhum dos três
    foi especificado, ou com código 1 se algum flow falhou ao registrar.
    """
    parser = argparse.ArgumentParser(description="Deploy de flows Prefect 3")
    parser.add_argument("--pool", required=True, help="Nome do Work Pool")
    parser.add_argument(
        "--branch", required=True, help="Branch do repositório"
    )
    parser.add_argument(
        "--files",
        nargs="*",
        help="Arquivos específicos para deploy, sem expansão (uso manual)",
    )
    parser.add_argument(
        "--changed",
        nargs="*",
        help=(
            "Arquivos alterados (ex. saída do tj-actions/changed-files) — "
            "expandido por dataset e escalado pra --all quando necessário, "
            "ver expand_changed_files()"
        ),
    )
    parser.add_argument(
        "--all", action="store_true", help="Deploy de todos os flows"
    )
    parser.add_argument(
        "--workers",
        type=_positive_int,
        default=8,
        help="Quantos flows registrar em paralelo (default: 8)",
    )
    args = parser.parse_args()

    files_to_process = []

    if args.all:
        files_to_process = all_python_files()
    elif args.changed is not None:
        expanded = expand_changed_files(args.changed)
        if expanded is None:
            print(
                "Mudança em infra compartilhada "
                f"({', '.join(SHARED_PREFIXES)}) — escalando para --all.\n"
            )
            files_to_process = all_python_files()
        else:
            files_to_process = expanded
    elif args.files:
        files_to_process = args.files
    else:
        print("Nenhum arquivo especificado. Use --files, --changed ou --all.")
        sys.exit(0)

    print(f"\nWork Pool : {args.pool}")
    print(f"Branch    : {args.branch}")
    print(f"Arquivos  : {len(files_to_process)}")
    print(f"Workers   : {args.workers}\n")

    skipped = 0
    to_deploy: list[tuple[Flow, str, str]] = []

    for file_path in files_to_process:
        if not os.path.exists(file_path):
            continue

        print(f"→ {file_path}")
        flows = load_flows_from_file(file_path)

        if not flows:
            skipped += 1
            continue

        for name, flow_obj in flows.items():
            to_deploy.append((flow_obj, name, file_path))

    print(
        f"\nRegistrando {len(to_deploy)} flow(s) (até {args.workers} em paralelo)...\n"
    )

    success, failed = 0, 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                deploy_flow, flow_obj, name, file_path, args.pool, args.branch
            ): name
            for flow_obj, name, file_path in to_deploy
        }
        for future in as_completed(futures):
            ok, message = future.result()
            print(message)
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
