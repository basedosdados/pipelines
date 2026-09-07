"""
Flows for au_aph_hansard — Prefect 3.

Australian Parliament Hansard: every speech, interjection and continuation in
the House of Representatives and the Senate.

**Source: OpenAustralia's mirror, not ParlInfo.** ParlInfo answers this worker
with HTTP 403 on every request - 490 of 490 probes on 2026-09-02 - while
serving the identical code and headers from an Australian connection, so the
block is on the egress IP and nothing in the code can lift it. OpenAustralia
publishes a parsed mirror of the same Hansard, built for bulk access, covering
2006 onwards.

Two consequences, both deliberate and visible in the data:

* The mirror carries no parliament/session/period number, no page number and no
  debate type, so those are null on days sourced from it. Electorate and party
  are recovered by joining OpenAustralia's own rosters; ``party`` there is the
  *most recent* party rather than the affiliation held on the day.
* ``speaker_id`` changes scheme - OpenAustralia's member ids, not ParlInfo's.
  Because the rebuild works on whole years, a year is wholly one source or the
  other, never a mix.

Measured against ParlInfo for 2026-08-20, the mirror yields 5% fewer words in
the House and 15% fewer in the Senate: its parser targets member speech and
drops some procedural text.

The run rebuilds whole year partitions rather than appending single sitting
days, which makes it idempotent — a re-run cannot double-count a day, and a
proof transcript later replaced by the official one is corrected in place.
Uploads therefore use ``dump_mode="append"``: the year partition being rebuilt
overwrites its own blob, and the 1901-2024 partitions are left untouched.

That only holds while every writer names a year's parquet identically, which is
why the name comes from a single ``PARTITION_FILE`` constant shared with the
one-shot onboarding code. ``append`` overwrites a blob of the same name but
leaves two files side by side when the names differ, and the partition is then
read twice: the onboarding wrote ``part-<year>.parquet`` and this flow wrote
``data.parquet``, which doubled 2026 - 170 sitting-day rows for 85 sitting days.
If you change the filename here, change it there in the same commit.

**Every table is AllFree.** The source is CC BY-NC-ND, whose NonCommercial term
rules out putting any of this behind BD Pro, so there is no rolling paywall
window here and no Row Access Policy is ever issued.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `au_aph_hansard_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import tempfile

from prefect import flow

from pipelines.datasets.au_aph_hansard.constants import constants
from pipelines.datasets.au_aph_hansard.tasks import (
    clean_hansard,
    cleanup,
    download_hansard,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
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

# Both tables are day-grained and fully open. `dicionario` has no date column,
# so it takes no coverage spec at all.
_COVERAGE = {
    "speech": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
    "sitting_day": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
}

_TABLES = ("speech", "sitting_day")


@flow(name="au_aph_hansard", log_prints=True)
def au_aph_hansard_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the Hansard corpus with any newly published sitting days.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the poll reports no new sitting day.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="speech"
    )

    work_dir = tempfile.mkdtemp(prefix="au_aph_hansard_")
    try:
        downloaded = download_hansard(work_dir=work_dir)
        result = clean_hansard(
            work_dir=work_dir, input_dir=downloaded["input_dir"]
        )
        max_date = result["max_date"]

        if not max_date:
            # download_hansard already refuses to return an empty harvest, so
            # reaching here means transcripts parsed to no usable date.
            raise RuntimeError(
                "transcripts were downloaded but none yielded a sitting date; "
                "refusing to report success"
            )

        # Parliament sits roughly 20 weeks a year, so most daily runs find
        # nothing new and stop here.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="speech",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="speech",
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # Run every table before testing any of them: the dictionary-coverage
        # test on `speech` reads ref('..._dicionario'), so an interleaved
        # run/test would test against a sibling that does not exist yet in a
        # clean environment.
        if not materialize_to_prod:
            for table in _TABLES:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                )
            for table in _TABLES:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in _TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in _TABLES:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        cleanup(work_dir=work_dir)


# Hansard for a sitting day is published that evening, Canberra time. 20:26 BRT
# is 09:26 the next morning in Canberra (AEST), so a daily run picks up the
# previous sitting day once the official transcript has landed. The poll guard
# makes the ~30 weeks a year Parliament does not sit a cheap no-op.
# pyrefly: ignore [missing-attribute]
au_aph_hansard_flow.deploy_schedules = [
    {"cron": "26 20 * * *", "timezone": "America/Sao_Paulo"}
]
# A full year of both chambers is a few hundred MB of XML plus the parsed rows.
# pyrefly: ignore [missing-attribute]
au_aph_hansard_flow.job_variables = {"memory": "8Gi"}
