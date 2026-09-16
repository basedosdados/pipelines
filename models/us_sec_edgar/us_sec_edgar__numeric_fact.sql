{{
    config(
        schema="us_sec_edgar",
        alias="numeric_fact",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2031, "interval": 1},
        },
        cluster_by=["accession_number", "tag"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(quarter as int64) quarter,
    safe_cast(accession_number as string) accession_number,
    safe_cast(tag as string) tag,
    safe_cast(version as string) version,
    safe_cast(period_end_date as date) period_end_date,
    safe_cast(quantity_quarters as int64) quantity_quarters,
    safe_cast(unit_of_measure as string) unit_of_measure,
    safe_cast(segments as string) segments,
    safe_cast(coregistrant as string) coregistrant,
    safe_cast(value as float64) value,
    safe_cast(footnote as string) footnote
from {{ set_datalake_project("us_sec_edgar_staging.numeric_fact") }} as t
