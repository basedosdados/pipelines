{{
    config(
        materialized="table",
        alias="iptu",
        schema="br_sp_saopaulo_geosampa_iptu",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2024, "interval": 1},
        },
    )
}}


select distinct
    safe_cast(ano as int64) ano,
    safe_cast(data_cadastramento as date) data_cadastramento,
    safe_cast(numero_notificacao as string) numero_notificacao,
    safe_cast(numero_contribuinte as string) numero_contribuinte,
    safe_cast(ano_inicio_vida_contribuinte as int64) ano_inicio_vida_contribuinte,
    safe_cast(mes_inicio_vida_contribuinte as int64) mes_inicio_vida_contribuinte,
    safe_cast(logradouro as string) logradouro,
    safe_cast(numero_imovel as int64) numero_imovel,
    safe_cast(numero_condominio as string) numero_condominio,
    safe_cast(complemento as string) complemento,
    safe_cast(bairro as string) bairro,
    case when cep in ('nan', '0') then null else cep end as cep,
    safe_cast(ano_construcao_corrigida as int64) ano_construcao_corrigida,
    safe_cast(fator_obsolescencia as float64) fator_obsolescencia,
    initcap(referencia_imovel) as referencia_imovel,
    initcap(finalidade_imovel) as finalidade_imovel,
    initcap(tipo_construcao) as tipo_construcao,
    initcap(tipo_terreno) as tipo_terreno,
    safe_cast(fracao_ideal as float64) fracao_ideal,
    safe_cast(area_terreno as int64) area_terreno,
    safe_cast(area_construida as int64) area_construida,
    safe_cast(area_ocupada as int64) area_ocupada,
    safe_cast(quantidade_pavimento as int64) quantidade_pavimento,
    safe_cast(quantidade_esquina_imovel as string) quantidade_esquina_imovel,
    safe_cast(testada_imovel as float64) testada_imovel,
    safe_cast(valor_terreno as int64) valor_terreno,
    safe_cast(valor_construcao as int64) valor_construcao,
from {{ set_datalake_project("br_sp_saopaulo_geosampa_iptu_staging.iptu") }} as t
