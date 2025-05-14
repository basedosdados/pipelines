{{
    config(
        schema="br_ms_cnes",
        alias="equipamento",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}


with
    raw_cnes_equipamento as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.equipamento") }}
        where cnes is not null
    ),
    unique_raw_cnes_equipamento as (
        -- 2. distinct nas linhas
        select distinct * from raw_cnes_equipamento
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio de 7 dígitos
        select *
        from unique_raw_cnes_equipamento
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on unique_raw_cnes_equipamento.codufmun = mun.id_municipio_6
    )
select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(id_municipio as string) as id_municipio,
    safe_cast(cnes as string) as id_estabelecimento_cnes,
    ltrim(safe_cast(codequip as string), '0') as id_equipamento,
    safe_cast(tipequip as string) as tipo_equipamento,
    safe_cast(qt_exist as string) as quantidade_equipamentos,
    safe_cast(qt_uso as string) as quantidade_equipamentos_ativos,
    safe_cast(ind_sus as int64) as indicador_equipamento_disponivel_sus,
    safe_cast(ind_nsus as int64) as indicador_equipamento_indisponivel_sus
from cnes_add_muni

{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
