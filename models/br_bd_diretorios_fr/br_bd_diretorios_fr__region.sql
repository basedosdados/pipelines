{{
    config(
        alias="region",
        schema="br_bd_diretorios_fr",
        materialized="table",
    )
}}
select
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_comuna_sede as string) id_comuna_sede,
    safe_cast(nome_regiao as string) nome_regiao,
    safe_cast(nome_regiao_maiusculo as string) nome_regiao_maiusculo
from {{ set_datalake_project("br_bd_diretorios_fr_staging.region") }} as t
