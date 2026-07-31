{{
    config(
        alias="correspondence_lga_2016_2021",
        schema="br_bd_diretorios_au",
        materialized="table",
    )
}}
select
    safe_cast(id_lga_2016 as string) id_lga_2016,
    safe_cast(name_lga_2016 as string) name_lga_2016,
    safe_cast(id_lga_2021 as string) id_lga_2021,
    safe_cast(name_lga_2021 as string) name_lga_2021,
    safe_cast(ratio as float64) ratio,
    safe_cast(quality_indicator as string) quality_indicator
from
    {{
        set_datalake_project(
            "br_bd_diretorios_au_staging.correspondence_lga_2016_2021"
        )
    }} as t
