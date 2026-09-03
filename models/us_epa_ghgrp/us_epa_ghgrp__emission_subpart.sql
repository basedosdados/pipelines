{{
    config(
        schema="us_epa_ghgrp",
        alias="emission_subpart",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2030, "interval": 1},
        },
        cluster_by=["facility_id", "subpart"],
    )
}}


select
    safe_cast(year as int64) year,
    safe_cast(facility_id as string) facility_id,
    safe_cast(subpart as string) subpart,
    safe_cast(subpart_type as string) subpart_type,
    safe_cast(gas as string) gas,
    safe_cast(emission as float64) emission
from {{ set_datalake_project("us_epa_ghgrp_staging.emission_subpart") }} as t
