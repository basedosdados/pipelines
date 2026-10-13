{{
    config(
        schema="br_ibama_fiscalizacao",
        alias="area_embargada",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1970, "end": 2031, "interval": 1},
        },
    )
}}


with
    staging as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(id_embargo as string) id_embargo,
            safe_cast(numero_tad as string) numero_tad,
            safe_cast(serie_tad as string) serie_tad,
            safe_cast(data_embargo as date) data_embargo,
            safe_cast(id_auto as string) id_auto,
            safe_cast(numero_auto_infracao as string) numero_auto_infracao,
            safe_cast(area_ha as float64) area_ha,
            safe_cast(tipo_area as string) tipo_area,
            safe_cast(nome_imovel as string) nome_imovel,
            safe_cast(tipo_pessoa_embargado as string) tipo_pessoa_embargado,
            safe_cast(cpf_cnpj_embargado as string) cpf_cnpj_embargado,
            safe_cast(nome_embargado as string) nome_embargado,
            safe_cast(latitude as float64) latitude,
            safe_cast(longitude as float64) longitude,
            safe_cast(indicador_desembargado as string) indicador_desembargado,
            safe_cast(data_desembargo as date) data_desembargo,
            safe_cast(data_ultima_alteracao as date) data_ultima_alteracao,
            safe_cast(data_extracao as date) data_extracao
        from
            {{ set_datalake_project("br_ibama_fiscalizacao_staging.area_embargada") }}
            as t
    ),
    municipio as (
        select id_municipio, geometria from `basedosdados.br_geobr_mapas.municipio`
    )
select
    s.ano,
    s.sigla_uf,
    s.id_municipio,
    s.id_embargo,
    s.numero_tad,
    s.serie_tad,
    s.data_embargo,
    s.id_auto,
    s.numero_auto_infracao,
    s.area_ha,
    s.tipo_area,
    s.nome_imovel,
    s.tipo_pessoa_embargado,
    s.cpf_cnpj_embargado,
    s.nome_embargado,
    s.latitude,
    s.longitude,
    case
        when s.latitude is null or s.longitude is null
        then null
        when safe.st_geogpoint(s.longitude, s.latitude) is null
        then 'nao'
        when m.geometria is null
        then null
        when st_contains(m.geometria, safe.st_geogpoint(s.longitude, s.latitude))
        then 'sim'
        else 'nao'
    end indicador_coordenada_valida,
    s.indicador_desembargado,
    s.data_desembargo,
    s.data_ultima_alteracao,
    s.data_extracao
from staging as s
left join municipio as m on s.id_municipio = m.id_municipio
