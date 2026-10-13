{{
    config(
        schema="br_ibama_fiscalizacao",
        alias="auto_infracao",
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
            safe_cast(id_auto as string) id_auto,
            safe_cast(numero_auto_infracao as string) numero_auto_infracao,
            safe_cast(serie_auto_infracao as string) serie_auto_infracao,
            safe_cast(data_auto as date) data_auto,
            safe_cast(tipo_auto as string) tipo_auto,
            safe_cast(tipo_infracao as string) tipo_infracao,
            safe_cast(gravidade as string) gravidade,
            safe_cast(valor_multa as float64) valor_multa,
            safe_cast(moeda as string) moeda,
            safe_cast(situacao_debito as string) situacao_debito,
            safe_cast(situacao_auto as string) situacao_auto,
            safe_cast(indicador_cancelado as string) indicador_cancelado,
            safe_cast(tipo_pessoa_infrator as string) tipo_pessoa_infrator,
            safe_cast(cpf_cnpj_infrator as string) cpf_cnpj_infrator,
            safe_cast(nome_infrator as string) nome_infrator,
            safe_cast(latitude as float64) latitude,
            safe_cast(longitude as float64) longitude,
            safe_cast(data_ultima_alteracao as date) data_ultima_alteracao,
            safe_cast(data_extracao as date) data_extracao
        from
            {{ set_datalake_project("br_ibama_fiscalizacao_staging.auto_infracao") }}
            as t
    ),
    -- Municipality boundaries used to test whether the published point actually
    -- falls inside the municipality the record declares. SAFE.ST_GEOGPOINT keeps
    -- the 28 records whose latitude is outside [-90, 90] from failing the build.
    municipio as (
        select id_municipio, geometria from `basedosdados.br_geobr_mapas.municipio`
    )
select
    s.ano,
    s.sigla_uf,
    s.id_municipio,
    s.id_auto,
    s.numero_auto_infracao,
    s.serie_auto_infracao,
    s.data_auto,
    s.tipo_auto,
    s.tipo_infracao,
    s.gravidade,
    s.valor_multa,
    s.moeda,
    s.situacao_debito,
    s.situacao_auto,
    s.indicador_cancelado,
    s.tipo_pessoa_infrator,
    s.cpf_cnpj_infrator,
    s.nome_infrator,
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
    s.data_ultima_alteracao,
    s.data_extracao
from staging as s
left join municipio as m on s.id_municipio = m.id_municipio
