{{
    config(
        schema="br_rf_cnpj",
        alias="simples",
        materialized="table",
    )
}}
-- Atualizado em 2026-08-11
select
    lpad(safe_cast(cnpj_basico as string), 8, '0') cnpj_basico,
    safe_cast(opcao_simples as int64) opcao_simples,
    safe_cast(data_opcao_simples as date) data_opcao_simples,
    safe_cast(data_exclusao_simples as date) data_exclusao_simples,
    safe_cast(opcao_mei as int64) opcao_mei,
    safe_cast(data_opcao_mei as date) data_opcao_mei,
    safe_cast(data_exclusao_mei as date) data_exclusao_mei
from {{ set_datalake_project("br_rf_cnpj_staging.simples") }} as t
where safe_cast(opcao_mei as string) != "opcao_mei"
