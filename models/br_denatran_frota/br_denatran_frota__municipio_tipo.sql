{{
    config(
        alias="municipio_tipo",
        schema="br_denatran_frota",
        materialization="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 2003,
                "end": 2024,
                "interval": 1,
            },
        },
        cluster_by=["mes"],
    )
}}

with
    tipo_municipio as (
        select
            ano,
            mes,
            sigla_uf,
            id_municipio,
            case
                when tipo_veiculo = 'AUTOMÓVEL'
                then 'AUTOMOVEL'
                when tipo_veiculo = 'CAMINHÃO'
                then 'CAMINHAO'
                when tipo_veiculo = 'CAMINHÃO TRATOR'
                then 'CAMINHAO TRATOR'
                when tipo_veiculo = 'CHASSI PLATAFAFORMA'
                then 'CHASSI PLATAFORMA'
                when tipo_veiculo = 'CHASSI PLATAF'
                then 'CHASSI PLATAFORMA'
                when tipo_veiculo = 'MICRO-ÔNIBUS'
                then 'MICRO-ONIBUS'
                when tipo_veiculo = 'MICROÔNIBUS'
                then 'MICRO-ONIBUS'
                when tipo_veiculo = 'ÔNIBUS'
                then 'ONIBUS'
                when tipo_veiculo = 'UTILITÁRIO'
                then 'UTILITARIO'
                when tipo_veiculo = 'nan'
                then ''
                when tipo_veiculo = 'TRATOR ESTEI'
                then 'TRATOR ESTEIRA'
                else tipo_veiculo
            end as tipo_veiculo2,
            quantidade
        from {{ set_datalake_project("br_denatran_frota_staging.municipio_tipo") }}
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(lower(tipo_veiculo2) as string) tipo_veiculo,
    safe_cast(quantidade as int64) quantidade
from tipo_municipio
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
