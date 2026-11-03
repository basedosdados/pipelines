"""Prefect 3 tasks for br_cgu_sancoes — one check_update/extract_and_load pair
per table, thin wrappers over utils.py.

See the module banner in ``constants.py`` for the full picture: ceis/cnep/cepim
each own an independent portal registry; acordos_leniencia/
acordos_leniencia_efeitos share one registry zip (each table's extract_and_load
still redownloads and reparses it independently — the accepted 1-flow-per-table
trade-off, same as au_ato_abr/au_geoscape_gnaf); dicionario is authored
directly in Python (no download needed to build it) but borrows ceis's registry
in its check_update purely for a real freshness signal (it has none of its own).

``compare_against="table_update"`` for every table (not the default
``"coverage"``): the portal is a live on-demand snapshot with no publication
competência — ``source_max_date`` is always just "today's extraction date", a
wall-clock, never a stable competência the way a dated FTP folder would be. The
PartBdpro tables (ceis/cnep/acordos_leniencia) additionally recompute their
free/pro split every run (``free_end = max(<start_date>) - 6 months``), so
their ``Coverage.DateTimeRange`` end date slides forward on its own and isn't a
stable "already ingested this snapshot" baseline either — same reasoning
au_ato_abr's ``get_latest_update`` docstring spells out for its own rolling
BD Pro window.
"""

import shutil
import tempfile
from collections.abc import Callable
from datetime import date
from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_sancoes.constants import (
    DICIONARIO_CHECK_REGISTRY,
    DICIONARIO_TABLE_ID,
    REGISTRY_FOR_TABLE,
    constants,
)
from pipelines.datasets.br_cgu_sancoes.utils import (
    build_dicionario,
    build_table,
    download_registry,
)
from pipelines.utils.stage_dispatch import (
    ExtractAndLoad,
    SourceInspection,
    pipeline_factory,
)

DATASET_ID = constants.DATASET_ID.value


@task(retries=2, retry_delay_seconds=60)
def download_registry_task(registry: str, work_dir: str) -> tuple[str, str]:
    """Download+extract one registry's latest on-demand snapshot.

    Retries twice: the portal generates each zip on demand behind AWS WAF and
    can intermittently return 202 past the readiness poll or drop the
    connection.

    Args:
        registry: Portal path segment (e.g. ``"ceis"``, ``"acordos-leniencia"``).
        work_dir: Directory to download into; CSVs land in ``<work_dir>/input``.

    Returns:
        ``(input_dir, snapshot_date_isoformat)`` — both strings, Prefect
        serializes task results.
    """
    input_dir = Path(work_dir) / "input"
    snapshot = download_registry(registry, input_dir)
    return str(input_dir), snapshot.isoformat()


@task
def build_table_task(table_id: str, input_dir: str, work_dir: str) -> str:
    """Build ``table_id``'s partitioned parquet from its downloaded CSV.

    Args:
        table_id: Table slug to build (one of ``REGISTRY_FOR_TABLE``'s keys).
        input_dir: Directory holding the extracted registry CSV, from
            :func:`download_registry_task`.
        work_dir: Directory to write into; the table lands under
            ``<work_dir>/output``.

    Returns:
        The table's output directory path, as a string.
    """
    output_dir = Path(work_dir) / "output"
    return str(build_table(table_id, Path(input_dir), output_dir))


@task
def build_dicionario_task(work_dir: str) -> str:
    """Build the static ``dicionario`` table — no download needed.

    Args:
        work_dir: Directory to write into; the table lands under
            ``<work_dir>/output``.

    Returns:
        The dictionary's output directory path, as a string.
    """
    output_dir = Path(work_dir) / "output"
    return str(build_dicionario(output_dir))


def make_get_latest_update(table_id: str) -> Callable[[], SourceInspection]:
    """Build the check_update callable for one table.

    Args:
        table_id: Table slug (one of ``constants.ALL_TABLES``).

    Returns:
        A zero-arg callable that downloads ``table_id``'s registry (or, for
        ``dicionario``, ``DICIONARIO_CHECK_REGISTRY``'s — see the module
        banner) just to read off the snapshot date, discards the download,
        and returns a `SourceInspection` with that date and
        ``compare_against="table_update"`` (see the module banner for why).
    """
    registry = REGISTRY_FOR_TABLE.get(table_id, DICIONARIO_CHECK_REGISTRY)

    def get_latest_update() -> SourceInspection:
        work_dir = tempfile.mkdtemp(prefix=f"br_cgu_sancoes_check_{table_id}_")
        try:
            _, snapshot_iso = download_registry_task(
                registry=registry, work_dir=work_dir
            )
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)
        return SourceInspection(
            reference_date=date.fromisoformat(snapshot_iso),
            compare_against="table_update",
        )

    return get_latest_update


def make_extract_load_data(table_id: str) -> Callable[[dict], ExtractAndLoad]:
    """Build the extract_and_load callable for one table.

    Args:
        table_id: Table slug (one of ``constants.ALL_TABLES``).

    Returns:
        A callable that downloads ``table_id``'s own registry again
        (independently of ``get_latest_update`` — the accepted duplicated
        cost, see the module banner) and builds only ``table_id``'s parquet,
        except for ``dicionario`` which needs no download at all. Not cleaned
        up afterwards (no ``shutil.rmtree``/``finally``): the returned
        ``data_path`` points inside ``work_dir`` and is only read (via
        ``upload_to_gcs``) right after this returns, inside the same flow run
        — same trade-off as ``au_ato_abr``/``au_geoscape_gnaf``.
    """

    def extract_load_data(download_params: dict) -> ExtractAndLoad:
        work_dir = tempfile.mkdtemp(prefix=f"br_cgu_sancoes_{table_id}_")

        if table_id == DICIONARIO_TABLE_ID:
            data_path = build_dicionario_task(work_dir=work_dir)
        else:
            registry = REGISTRY_FOR_TABLE[table_id]
            input_dir, _ = download_registry_task(
                registry=registry, work_dir=work_dir
            )
            data_path = build_table_task(
                table_id=table_id, input_dir=input_dir, work_dir=work_dir
            )

        return ExtractAndLoad(
            data_path=data_path,
            dump_mode="overwrite",
            source_format="parquet",
        )

    return extract_load_data


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)
