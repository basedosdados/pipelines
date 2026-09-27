{{
    config(
        schema="us_epa_tri",
        alias="chemical",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1987, "end": 2035, "interval": 1},
        },
        cluster_by=["tri_chemical_id"],
    )
}}

-- Atualizado em 2026-09-03
select
    safe_cast(year as int64) year,
    safe_cast(tri_chemical_id as string) tri_chemical_id,
    safe_cast(chemical_name as string) chemical_name,
    safe_cast(cas_number as string) cas_number,
    safe_cast(srs_id as string) srs_id,
    safe_cast(clean_air_act_chemical as string) clean_air_act_chemical,
    safe_cast(classification as string) classification,
    safe_cast(metal as string) metal,
    safe_cast(metal_category as string) metal_category,
    safe_cast(carcinogen as string) carcinogen,
    safe_cast(pbt as string) pbt,
    safe_cast(pfas as string) pfas
from {{ set_datalake_project("us_epa_tri_staging.chemical") }} as t
