"""Tasks do pipeline br_bcb_ifdata — invólucros Prefect sobre `utils`."""

from __future__ import annotations

import pathlib
from typing import Any

from prefect import task

from pipelines.constants import constants as pipeline_constants
from pipelines.datasets.br_bcb_ifdata.utils import (
    build_dicionario,
    build_municipio_crosswalk,
    clean_period,
    fetch_index,
    source_max_period,
)


@task(
    retries=pipeline_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=pipeline_constants.TASK_RETRY_DELAY.value,
)
def get_source_max_period() -> str:
    """Competência mais recente na fonte, como `AAAA-MM`."""
    dt = source_max_period()
    return f"{dt // 100:04d}-{dt % 100:02d}"


@task(
    retries=pipeline_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=pipeline_constants.TASK_RETRY_DELAY.value,
)
def clean_all(work_dir: str) -> dict[str, Any]:
    """Reconstrói todas as competências e devolve o caminho de cada tabela.

    A reconstrução é **completa**, não incremental: na data-base de dezembro o
    BCB republica os dados contábeis dos últimos quatro trimestres, então uma
    competência já carregada pode mudar depois. Anexar só o trimestre novo
    deixaria as revisões para trás.
    """
    outdir = pathlib.Path(work_dir)
    exact, squashed = build_municipio_crosswalk()
    index = fetch_index()
    print(f"competências a processar: {len(index)}")

    totais: dict[str, int] = {"instituicao": 0, "coluna": 0, "relatorio": 0}
    nao_resolvidos: list[tuple[str, str]] = []
    for n, entry in enumerate(index, 1):
        st = clean_period(entry, outdir, exact, squashed)
        for k in totais:
            totais[k] += st[k]
        nao_resolvidos.extend(st["municipios_nao_resolvidos"])
        if n % 10 == 0 or n == len(index):
            print(f"  {n}/{len(index)} — {st['dt']}")

    totais.update(build_dicionario(index, outdir))
    if nao_resolvidos:
        # Não derruba a run: o teste dbt `relationships` de id_municipio é o
        # portão de verdade. Mas registra, porque um nome novo sem match é
        # sinal de município renomeado que precisa entrar em ALIAS_MUNICIPIO.
        print(f"ATENÇÃO: {len(nao_resolvidos)} municípios não resolvidos")
        for uf_nome in sorted(set(nao_resolvidos))[:20]:
            print(f"  {uf_nome}")

    print(f"totais: {totais}")
    return {
        **{t: str(outdir / t) for t in totais},
        "totais": totais,
    }
