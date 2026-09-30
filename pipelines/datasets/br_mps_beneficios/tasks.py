"""Prefect tasks for br_mps_beneficios.

Thin wrappers over the pure transform in ``utils.py``, which the one-shot
bootstrap under ``models/br_mps_beneficios/code/`` imports as well, so the
cleaning logic exists in one place.

The refresh is **incremental**, unlike a source that reships its whole history
each release: INSS publishes one file per competência, and a month of mantidos
is 12 GB uncompressed. Each run therefore aggregates only the new month and
writes it as its own parquet inside the year's partition directory.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.datasets.br_mps_beneficios import utils as u
from pipelines.datasets.br_mps_beneficios.constants import constants


@task(retries=2, retry_delay_seconds=60)
def latest_source_competencia(table_id: str) -> int:
    """Highest competência the source lists for a table, as YYYYMM.

    Resolved through the CKAN API rather than by templating the S3 key: the
    object keys carry ad-hoc suffixes (``_consulta59807528``, inconsistent
    casing, three different PDA prefixes) and are not predictable.
    """
    if table_id == constants.TABLE_CONCEDIDO.value:
        return u.latest_competencia(u.resolve_concedido_resources())
    if table_id == constants.TABLE_MANTIDO.value:
        return u.latest_competencia(u.resolve_mantido_resources())
    raise ValueError(f"no source competência for table {table_id!r}")


@task(retries=0)
def build_dicionario(work_dir: str) -> str:
    """Write the espécie dictionary. Static, but both data tables test against it."""
    out = Path(work_dir) / "output"
    u.write_partitioned(
        u.build_dicionario_especie(),
        out,
        constants.TABLE_DICIONARIO.value,
        partition_cols=[],
    )
    return str(out / constants.TABLE_DICIONARIO.value)


@task(retries=2, retry_delay_seconds=120)
def refresh_month(
    table_id: str,
    competencia: int,
    work_dir: str,
    previous: dict | None = None,
) -> dict:
    """Download and aggregate one competência, or report why it was skipped.

    ``previous`` describes the last competência already in the table — its
    ``competencia``, ``cells`` and ``benefits`` — and drives the reissue guard.
    The publisher republishes a stale snapshot under a later month label instead
    of leaving the month unpublished, and nothing in the file says so: mantidos
    carries no competência column, so a reissue is indistinguishable from a real
    month by content alone. Two checks catch it:

    1. an identical ``Content-Length`` to the previous month's archive, which
       skips the download entirely;
    2. an aggregate identical to the previous month's — same cell count and same
       total quantidade. A national stock of 40 million benefits does not repeat
       to the unit, so this is a reissue, not a quiet month.

    Returns a dict with ``status`` in ``{"ok", "reissue", "empty"}``; only
    ``"ok"`` carries a ``path`` to upload.
    """
    if table_id == constants.TABLE_CONCEDIDO.value:
        resources = u.resolve_concedido_resources()
        aggregate, suffix = u.aggregate_concedido, "xlsx"
    elif table_id == constants.TABLE_MANTIDO.value:
        resources = u.resolve_mantido_resources()
        aggregate, suffix = u.aggregate_mantido, "zip"
    else:
        raise ValueError(f"not a refreshable table: {table_id!r}")

    resource = next(
        (r for r in resources if r["competencia"] == competencia), None
    )
    if resource is None:
        raise ValueError(
            f"{table_id}: source has no competência {competencia}"
        )

    # (1) Cheap reissue pre-check, before spending the download.
    if previous and previous.get("url"):
        size, prev_size = (
            u.remote_size(resource["url"]),
            u.remote_size(previous["url"]),
        )
        if size and prev_size and size == prev_size:
            print(
                f"{table_id} {competencia}: same Content-Length as "
                f"{previous['competencia']} ({size} bytes) — reissue, not downloaded"
            )
            return {"status": "reissue", "competencia": competencia}

    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    dest = work / f"{table_id}_{competencia}.{suffix}"
    u.download(resource["url"], dest)

    df, diag = aggregate(dest, competencia)
    dest.unlink(missing_ok=True)

    if df.empty:
        print(f"{table_id} {competencia}: aggregated to zero rows")
        return {
            "status": "empty",
            "competencia": competencia,
            "diagnostics": diag,
        }

    cells = len(df)
    benefits = int(pd.to_numeric(df["quantidade"], errors="coerce").sum())

    # (2) An aggregate identical to the previous month is a reissue.
    if (
        previous
        and previous.get("cells") == cells
        and previous.get("benefits") == benefits
    ):
        print(
            f"{table_id} {competencia}: aggregate identical to "
            f"{previous['competencia']} ({cells:,} cells, {benefits:,} benefits)"
            " — reissue, not staged"
        )
        return {"status": "reissue", "competencia": competencia}

    out = Path(work_dir) / "output"
    u.write_partitioned(
        df,
        out,
        table_id,
        partition_cols=["ano"],
        filename=f"data_{competencia}.parquet",
    )
    print(
        f"{table_id} {competencia}: {cells:,} cells, {benefits:,} benefits staged"
    )
    return {
        "status": "ok",
        "competencia": competencia,
        "cells": cells,
        "benefits": benefits,
        "path": str(out / table_id),
        "diagnostics": diag,
    }


@task(retries=1, retry_delay_seconds=30)
def previous_competencia_shape(table_id: str, bq_project: str) -> dict | None:
    """Shape of the newest competência already in the table, for the reissue guard.

    Returns None when the table does not exist yet or is empty, which makes the
    guard a no-op on a first run rather than an error.
    """
    import basedosdados as bd

    dataset_id = constants.DATASET_ID.value
    query = f"""
        with ultimo as (
          select max(ano * 100 + mes) as competencia
          from `{bq_project}.{dataset_id}.{table_id}`
        )
        select
          u.competencia,
          count(*) as cells,
          sum(t.quantidade) as benefits
        from `{bq_project}.{dataset_id}.{table_id}` t
        cross join ultimo u
        where t.ano * 100 + t.mes = u.competencia
        group by u.competencia
    """
    try:
        frame = bd.read_sql(query, billing_project_id=bq_project)
    except Exception as exc:  # table absent on a first run
        print(f"{table_id}: no previous competência ({type(exc).__name__})")
        return None
    if frame is None or frame.empty:
        return None
    row = frame.iloc[0]
    competencia = int(row["competencia"])
    resources = (
        u.resolve_concedido_resources()
        if table_id == constants.TABLE_CONCEDIDO.value
        else u.resolve_mantido_resources()
    )
    match = next(
        (r for r in resources if r["competencia"] == competencia), None
    )
    return {
        "competencia": competencia,
        "cells": int(row["cells"]),
        "benefits": int(row["benefits"]),
        "url": match["url"] if match else None,
    }
