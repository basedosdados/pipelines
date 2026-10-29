"""Convenção de nome dos flows e deployments do pipeline em estágios.

`_flow_name` dá o nome do `@flow`, `deployment_name` resolve o deployment de
uma etapa e `deploy_tags` monta as tags de deploy. `_dev_prefix` e
`_is_local` leem as tags do flow run em execução para saber se ele roda no
pool de dev ou no local.
"""

from prefect import runtime

from pipelines.utils.stage_dispatch.contracts import Etapa


def deploy_tags(
    dataset_id: str, etapa: Etapa, table_id: str | None = None
) -> list[str]:
    """Tags de deploy pra achar deployments relacionados no Prefect UI/CI.

    Usar em `<flow>.deploy_tags = deploy_tags(...)`.

    Args:
        dataset_id: ID do dataset.
        etapa: etapa do flow.
        table_id: ID da tabela, quando o dataset tem mais de uma — adiciona
            a tag `<dataset_id>__<table_id>`, pra filtrar uma tabela
            específica sem misturar com as outras do mesmo dataset (a tag
            `<dataset_id>` sozinha não distingue).

    Returns:
        Lista com `"staged-pipeline"` (todo flow que passa por essa
        cápsula, pra achar todos os migrados de uma vez — issue #1867), a
        tag da etapa, `<dataset_id>`, e `<dataset_id>__<table_id>` quando
        `table_id` é passado — todas sem prefixo, mesmo padrão da tag de
        etapa.
    """
    tags = ["staged-pipeline", str(etapa), dataset_id]
    if table_id is not None:
        tags.append(f"{dataset_id}__{table_id}")
    return tags


def _flow_name(dataset_id: str, etapa: Etapa) -> str:
    """Nome do `@flow` por convenção: `"<etapa>: <dataset_id>"`.

    Args:
        dataset_id: ID do dataset (ou `prefect_dataset_id`, quando o
            dataset tem mais de uma tabela).
        etapa: etapa do flow.

    Returns:
        O nome formatado do flow.
    """
    return f"{etapa}: {dataset_id}"


def _dev_prefix() -> str:
    """Prefixo `"dev-"` do deployment atual, ou `""` fora do pool de dev.

    Lê `prefect.runtime.flow_run.tags` (já disponível no contexto do flow
    run, sem chamada de API) procurando a tag `env:dev`, que `deploy_flow()`
    (`.github/workflows/scripts/deploy_flows.py`) adiciona só quando o
    deploy é pro pool `basedosdados-dev`. Fora de um flow run real (ex.
    chamada local em teste), `runtime.flow_run.tags` vem vazio — resolve
    pra `""`, mesmo comportamento de hoje (assume prod).

    Returns:
        `"dev-"` quando a tag `env:dev` está presente; `""` caso contrário.
    """
    return "dev-" if "env:dev" in runtime.flow_run.tags else ""


def _is_local() -> bool:
    """`True` quando o flow run atual está no pool local do `scripts/dev_local.sh`.

    Lê a tag `env:local` (já disponível no contexto do flow run, sem
    chamada de API), que `deploy_flow()` (`deploy_flows.py`) adiciona só
    quando o deploy é feito com `--local` — mesmo padrão de `_dev_prefix()`.

    Returns:
        `True` se a tag `env:local` está presente nas tags do flow run.
    """
    return "env:local" in runtime.flow_run.tags


def deployment_name(
    dataset_id: str, etapa: Etapa, deployment: str | None = None
) -> str:
    """Resolve o identificador de um deployment por convenção.

    Mesmo formato `"<flow name>/<deployment name>"` aceito por
    `run_deployment(name=...)`. Repete o prefixo `dev-` (ver `_dev_prefix`)
    quando o flow que está chamando também foi deployado em dev — sem
    isso, o dispatch dev sempre tentaria achar o nome de prod.

    Args:
        dataset_id: ID do dataset (ou `prefect_dataset_id`).
        etapa: etapa do deployment. `build_and_promote` é genérico — um
            deployment só, nome fixo, **sempre no pool de prod**, mesmo
            quando quem despacha rodou em dev (só a service account do
            pool prod tem permissão de escrita real pra promover dado) —
            por isso não leva o prefixo `dev-`, ao contrário das outras
            etapas. Exceção: no pool local, `deploy_flows.py` prefixa
            `dev-` em **tudo** (inclusive `build_and_promote`, que lá não
            existe separado por ambiente) — `_is_local()` cobre esse caso.
        deployment: sobrescreve a segunda metade (depois da barra), quando
            a variável do flow não se chama literalmente `<etapa>`.

    Returns:
        O identificador `"<flow name>/<deployment name>"`.
    """
    if etapa == Etapa.BUILD_AND_PROMOTE:
        prefix = "dev-" if _is_local() else ""
        return f"build_and_promote/{prefix}build_and_promote"
    prefix = _dev_prefix()
    return (
        f"{_flow_name(dataset_id, etapa)}/{prefix}{deployment or str(etapa)}"
    )
