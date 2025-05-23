{{
    config(
        alias="dataset_new_arch",
        schema="tabela_new_arch",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(equipe_dados as string) equipe_dados,
    safe_cast(github as string) github,
    safe_cast(idade as int64) idade,
    safe_cast(sexo as string) sexo
from {{ set_datalake_project("dataset_new_arch_staging.tabela_new_arch") }} as t
