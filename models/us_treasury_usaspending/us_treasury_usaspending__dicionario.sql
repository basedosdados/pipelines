{{
    config(
        schema="us_treasury_usaspending",
        alias="dicionario",
        materialized="table",
    )
}}


select
    safe_cast(as string) id_tabela,
    safe_cast(as string) nome_coluna,
    safe_cast(as string) chave,
    safe_cast(as string) cobertura_temporal,
    safe_cast(as string) valor
from {{ set_datalake_project("us_treasury_usaspending_staging.dicionario") }} as t
