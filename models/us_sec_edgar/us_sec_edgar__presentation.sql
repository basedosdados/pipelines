{{
    config(
        schema="us_sec_edgar",
        alias="presentation",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
        cluster_by=["accession_number"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(accession_number as string) accession_number,
    safe_cast(report as string) report,
    safe_cast(line as string) line,
    safe_cast(statement as string) statement,
    safe_cast(parenthetical as string) parenthetical,
    safe_cast(render_file as string) render_file,
    safe_cast(tag as string) tag,
    safe_cast(version as string) version,
    safe_cast(preferred_label as string) preferred_label,
    safe_cast(negating as string) negating
from {{ set_datalake_project("us_sec_edgar_staging.presentation") }} as t
