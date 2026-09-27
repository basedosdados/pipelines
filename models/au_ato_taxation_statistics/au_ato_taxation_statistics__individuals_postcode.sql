{{
    config(
        schema="au_ato_taxation_statistics",
        alias="individuals_postcode",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2028, "interval": 1},
        },
    )
}}


select
    safe_cast(t.year as int64) year,
    safe_cast(t.state_abbreviation as string) state_abbreviation,
    safe_cast(d.id_sa4 as string) sa4_id,
    safe_cast(t.sa4_name as string) sa4_name,
    safe_cast(t.postcode as string) postcode,
    safe_cast(t.taxable_status as string) taxable_status,
    safe_cast(t.item as string) item,
    safe_cast(t.record_count as int64) record_count,
    safe_cast(t.amount as float64) amount
from
    {{
        set_datalake_project(
            "au_ato_taxation_statistics_staging.individuals_postcode"
        )
    }} as t
left join basedosdados.br_bd_diretorios_au.sa4_2021 as d on t.sa4_name = d.name
