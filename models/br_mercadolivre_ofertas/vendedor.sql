{{
    config(
        materialized="table",
        partition_by={
            "field": "data_consulta",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    main as (
        select
            lpad(id_vendor, 12, '0') as id_vendedor,
            dia,
            nome,
            safe_cast(experiencia as int64) experiencia,
            reputacao,
            case
                when classificacao = 'None' then null else classificacao
            end as classificacao,
            id_municipio,
        from {{ set_datalake_project("br_mercadolivre_ofertas_staging.vendedor") }}
    ),
    predata as (
        select
            lpad(id_vendor, 12, '0') as id_vendedor,
            struct(
                json_extract_scalar(opinioes, '$.Bom') as bom,
                json_extract_scalar(opinioes, '$.Regular') as regular,
                json_extract_scalar(opinioes, '$.Ruim') as ruim
            ) as opinioes
        from {{ set_datalake_project("br_mercadolivre_ofertas_staging.vendedor") }}
    ),
    tabela_ordenada as (
        select
            dia as data_consulta,
            id_municipio,
            main.id_vendedor,
            nome as vendedor,
            classificacao,
            reputacao,
            experiencia as anos_experiencia,
            safe_cast(predata.opinioes.bom as int64) as avaliacao_bom,
            safe_cast(predata.opinioes.regular as int64) as avaliacao_regular,
            safe_cast(predata.opinioes.regular as int64) as avaliacao_ruim
        from main
        left join predata on main.id_vendedor = predata.id_vendedor
    ),

    tabela_deduplicada as (
        select
            parse_date('%Y-%m-%d', data_consulta) as data_consulta,
            id_municipio,
            id_vendedor,
            vendedor,
            classificacao,
            reputacao,
            anos_experiencia,
            array_agg(avaliacao_bom)[offset(0)] as avaliacao_bom,
            array_agg(avaliacao_regular)[offset(0)] as avaliacao_regular,
            array_agg(avaliacao_ruim)[offset(0)] as avaliacao_ruim
        from tabela_ordenada
        group by
            data_consulta,
            id_vendedor,
            vendedor,
            anos_experiencia,
            reputacao,
            classificacao,
            id_municipio
        having count(*) > 1
    ),
    tabela_unicos as (
        select
            parse_date('%Y-%m-%d', data_consulta) as data_consulta,
            id_municipio,
            id_vendedor,
            vendedor,
            classificacao,
            reputacao,
            anos_experiencia,
            array_agg(avaliacao_bom)[offset(0)] as avaliacao_bom,
            array_agg(avaliacao_regular)[offset(0)] as avaliacao_regular,
            array_agg(avaliacao_ruim)[offset(0)] as avaliacao_ruim
        from tabela_ordenada
        group by
            data_consulta,
            id_vendedor,
            vendedor,
            anos_experiencia,
            reputacao,
            classificacao,
            id_municipio
        having count(*) = 1
    )
select *
from tabela_unicos
union all
select *
from tabela_deduplicada
