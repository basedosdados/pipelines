{{
    config(
        alias="responsavel",
        schema="br_cvm_administradores_carteira",
        materialized="table",
    )
}}
select
    safe_cast(cnpj as string) cnpj,
    safe_cast(nome as string) nome,
    safe_cast(tipo as string) tipo
from
    {{ set_datalake_project("br_cvm_administradores_carteira_staging.responsavel") }}
    as t
