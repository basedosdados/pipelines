{{
    config(
        alias="pessoa_fisica",
        schema="br_cvm_administradores_carteira",
        materialized="incremental",
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day",
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(data_registro), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(data_registro), MONTH) <= 6)',
        ],
    )
}}
with
    tabela as (
        select
            safe_cast(nome as string) nome,
            safe_cast(data_registro as date) data_registro,
            safe_cast(data_cancelamento as date) data_cancelamento,
            safe_cast(motivo_cancelamento as string) motivo_cancelamento,
            safe_cast(situacao as string) situacao,
            safe_cast(data_inicio_situacao as date) data_inicio_situacao,
            safe_cast(categoria_registro as string) categoria_registro
        from
            {{
                set_datalake_project(
                    "br_cvm_administradores_carteira_staging.pessoa_fisica"
                )
            }} t
    )
select *
from tabela
{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    -- (uses > to include records whose timestamp occurred since the last run of this
    -- model)
    where data_registro > (select max(data_registro) from {{ this }})

{% endif %}
