{{
    config(
        schema="us_epa_tri",
        alias="release",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1987, "end": 2035, "interval": 1},
        },
        cluster_by=["tri_chemical_id", "release_category"],
    )
}}

-- Atualizado em 2026-09-03
select
    safe_cast(year as int64) year,
    safe_cast(tri_facility_id as string) tri_facility_id,
    safe_cast(document_control_number as string) document_control_number,
    safe_cast(tri_chemical_id as string) tri_chemical_id,
    safe_cast(management_category as string) management_category,
    safe_cast(release_category as string) release_category,
    safe_cast(quantity_pounds as float64) quantity_pounds,
    safe_cast(quantity_grams as float64) quantity_grams
from {{ set_datalake_project("us_epa_tri_staging.release") }} as t
