{{ config(alias="uf_tipo", schema="br_denatran_frota", materialization="table") }}


with
    uf_tipo as (
        select
            ano,
            mes,
            sigla_uf,
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
                when tipo_veiculo = 'caminhaotrator'
                then 'caminhao trator'
                when tipo_veiculo = 'chassiplataforma'
                then 'chassi plataforma'
                when tipo_veiculo = 'moto cicleta'
                then 'motocicleta'
                when tipo_veiculo = 'moto  cicleta'
                then 'motocicleta'
                when tipo_veiculo = 'MICRO-ÔNIBUS'
                then 'MICRO-ONIBUS'
                when tipo_veiculo = 'microonibus'
                then 'micro-onibus'
                when tipo_veiculo = 'sidecar'
                then 'side-car'
                when tipo_veiculo = 'semireboque'
                then 'semi-reboque'
                when tipo_veiculo = 'tratoresteira'
                then 'trator esteira'
                when tipo_veiculo = 'tratorrodas'
                then 'trator rodas'
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

        from {{ set_datalake_project("br_denatran_frota_staging.uf_tipo") }}
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(lower(tipo_veiculo2) as string) tipo_veiculo,
    safe_cast(quantidade as int64) quantidade
from uf_tipo
