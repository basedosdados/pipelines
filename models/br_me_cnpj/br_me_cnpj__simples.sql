{{
    config(
        schema="br_me_cnpj",
        alias="simples",
        materialized="table",
    )
}}

select
    safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
    safe_cast(opcao_simples as int64) opcao_simples,
    safe_cast(data_opcao_simples as date) data_opcao_simples,
    safe_cast(data_exclusao_simples as date) data_exclusao_simples,
    safe_cast(opcao_mei as int64) opcao_mei,
    safe_cast(data_opcao_mei as date) data_opcao_mei,
    safe_cast(data_exclusao_mei as date) data_exclusao_mei
from {{ set_datalake_project("br_me_cnpj_staging.simples") }} as t
where opcao_mei != "opcao_mei"
