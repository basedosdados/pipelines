{{
    config(
        alias="cbo_1994",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(cbo_1994 as string) as cbo_1994,
    safe_cast(descricao as string) as descricao
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cbo_1994") }} as t
