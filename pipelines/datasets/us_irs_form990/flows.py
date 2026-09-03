"""Flows for us_irs_form990 — Prefect 3.

IRS Form 990 series: the e-file XML returns (Form 990 and 990-EZ header
financials and Part VII compensation), the monthly Exempt Organizations
Business Master File (the nonprofit registry) and the monthly automatic
revocation list.

Three sources, three refresh shapes, one flow:

* **e-file ZIPs** land in irregular batches through the year. The flow lists
  every ZIP on the IRS page, subtracts the batches already present in the
  staging table (``xml_batch_id``) and processes only the new ones — one ZIP at
  a time, deleted after parsing. Staging is appended; the dbt model keeps one
  return per (ein, year, form_type), so re-loading a batch is harmless.
* **BMF** is a monthly full snapshot; each run appends a new
  ``extraction_date`` partition and the incremental model stacks it.
* **revocation** is a monthly cumulative list, replaced wholesale.

The run polls cheaply first (HTTP HEAD on the newest ZIP and on the BMF) and
only downloads once the IRS has published something newer than the last
refresh, so a scheduled run between releases is a no-op.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``us_irs_form990_flow``;
the dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_irs_form990.constants import constants
from pipelines.datasets.us_irs_form990.tasks import (
    check_source_dates,
    list_efile_batches,
    loaded_batches,
    process_bmf,
    process_efile_batch,
    process_revocation,
    write_dicionario,
)
from pipelines.datasets.us_irs_form990.utils import batch_id
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# Every table is AllFree. The BMF refreshes monthly, which by the house rule
# would paywall its most recent window — but with a single onboarded snapshot a
# 6-month free_lag puts free_end before the only snapshot and inverts the free
# range (the br_senado_dados_abertos_administrativos failure). Switch the
# organization table to PartBdpro once several snapshots exist, creating its
# pro Coverage in the same change. The e-file tables are annual filings
# released in irregular batches and stay free. dicionario has no date column.
_COVERAGE = {
    "organization": AllFree(
        date_column=DateOnly(col="extraction_date"),
        date_format=DateFormat.YEAR_MD,
    ),
    "return_financial": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "compensation": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "revocation": AllFree(
        date_column=DateOnly(col="revocation_date"),
        date_format=DateFormat.YEAR_MD,
    ),
}

# dbt order matters: compensation refs return_financial, and the dictionary
# coverage tests ref dicionario.
_TABLE_ORDER = [
    "dicionario",
    "organization",
    "revocation",
    "return_financial",
    "compensation",
]


def _materialize(
    outputs: dict[str, str], bucket: str, target: str, dump_modes: dict
) -> None:
    for table in _TABLE_ORDER:
        if table not in outputs:
            continue
        upload_to_gcs(
            data_path=outputs[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode=dump_modes[table],
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target=target,
        )
    for table in _TABLE_ORDER:
        if table in outputs:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )


@flow(name="us_irs_form990", log_prints=True)
def us_irs_form990_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    max_batches: int = 12,
) -> None:
    """Refresh us_irs_form990 with any new IRS e-file batches and the monthly BMF.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register
            table coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports
            nothing new.
        max_batches: Upper bound on e-file ZIPs processed per run (each is
            0.1 to 1.2 GB); the rest are picked up by the next run.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="return_financial"
    )

    urls = list_efile_batches()
    dates = check_source_dates(urls)
    new_efile = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="return_financial",
        source_max_date=dates["efile"],
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    new_bmf = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="organization",
        source_max_date=dates["bmf"],
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not (new_efile or new_bmf or force_run):
        return

    # Which project's staging table records the loaded batches. Reading dev
    # on a dev-only run keeps the test path self-contained.
    bq_project = (
        "basedosdados-staging" if materialize_to_prod else "basedosdados-dev"
    )
    already = loaded_batches(bq_project)
    todo = [u for u in urls if batch_id(u) not in already][:max_batches]

    work_dir = tempfile.mkdtemp(prefix="us_irs_form990_")
    try:
        outputs: dict[str, str] = {}
        out = f"{work_dir}/output"
        for url in todo:
            res = process_efile_batch(url=url, work_dir=work_dir)
            print(
                f"{res['batch']}: {sum(res['return_financial'].values()):,} "
                f"returns, skipped {res['skipped']}"
            )
        if todo:
            outputs["return_financial"] = f"{out}/return_financial"
            outputs["compensation"] = f"{out}/compensation"
        if new_bmf or force_run:
            process_bmf(work_dir=work_dir, extraction_date=dates["bmf"])
            outputs["organization"] = f"{out}/organization"
            process_revocation(work_dir=work_dir)
            outputs["revocation"] = f"{out}/revocation"
        write_dicionario(work_dir=work_dir)
        outputs["dicionario"] = f"{out}/dicionario"

        # append everywhere: `overwrite` calls tb.delete(mode="all"), which
        # drops the materialized prod table even from a dev-only run. For the
        # fixed-name single files (revocation, dicionario) append + replace is
        # a wholesale swap; for the batch-named e-file parts it is a true append.
        dump_modes = {t: "append" for t in _TABLE_ORDER}

        if not materialize_to_prod:
            _materialize(outputs, "basedosdados-dev", "dev", dump_modes)
            return

        _materialize(outputs, "basedosdados", "prod", dump_modes)

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                if table in outputs:
                    register_table_materialization_task(
                        dataset_id=DATASET_ID,
                        table_id=table,
                        coverage=coverage,
                        env="prod",
                        bq_project="basedosdados",
                    )
            if todo:
                commit_source_update_task(
                    dataset_id=DATASET_ID,
                    table_id="return_financial",
                    source_max_date=dates["efile"],
                    env="prod",
                    date_format="%Y-%m-%d",
                    update_metadata=update_metadata,
                    materialize_after_dump=materialize_to_prod,
                )
            if "organization" in outputs:
                commit_source_update_task(
                    dataset_id=DATASET_ID,
                    table_id="organization",
                    source_max_date=dates["bmf"],
                    env="prod",
                    date_format="%Y-%m-%d",
                    update_metadata=update_metadata,
                    materialize_after_dump=materialize_to_prod,
                )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# The IRS posts e-file batches in irregular bursts and the BMF around the
# 10th of each month. Poll twice a month at 16:38 BRT (a minute nobody else
# uses); the HEAD-based source poll no-ops until something new appears.
# pyrefly: ignore [missing-attribute]
us_irs_form990_flow.deploy_schedules = [
    {"cron": "38 16 12,26 * *", "timezone": "America/Sao_Paulo"}
]
# One ZIP is parsed at a time (≤ 3.7 GB, streamed member by member) and the
# BMF stack is ~2M rows in memory; 8Gi leaves headroom.
# pyrefly: ignore [missing-attribute]
us_irs_form990_flow.job_variables = {"memory": "8Gi"}
