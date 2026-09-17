{{
    config(
        schema="us_nsf_ncses",
        alias="sed_data_table",
        materialized="table",
        partition_by={
            "field": "reference_year",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2029, "interval": 1},
        },
    )
}}


select
    safe_cast(reference_year as int64) reference_year,
    safe_cast(table_id as string) table_id,
    safe_cast(table_group as string) table_group,
    safe_cast(table_title as string) table_title,
    safe_cast(unit_statement as string) unit_statement,
    safe_cast(publication_id as string) publication_id,
    safe_cast(source_file as string) source_file,
    safe_cast(estimate_count as int64) estimate_count
from {{ set_datalake_project("us_nsf_ncses_staging.sed_data_table") }} as t
