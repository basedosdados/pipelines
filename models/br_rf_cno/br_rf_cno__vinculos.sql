{{
    config(
        alias="vinculos",
        schema="br_rf_cno",
        materialized="incremental",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}


select
    safe_cast(data as date) data_extracao,
    safe_cast(data_registro as date) data_registro,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(id_cno as string) id_cno,
    safe_cast(id_responsavel as string) id_responsavel,
    safe_cast(
        ltrim(qualificacao_contribuinte, '0') as string
    ) qualificacao_contribuinte,
from {{ set_datalake_project("br_rf_cno_staging.vinculos") }} as t

{% if is_incremental() %}
    where safe_cast(data as date) > (select max(data_extracao) from {{ this }})
{% endif %}
