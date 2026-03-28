{{
    config(
        alias="especies_ameacadas",
        schema="mundo_cites_comercio",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(apendice_cities as string) apendice_cites,
    safe_cast(nome_cientifico as string) nome_cientifico,
    case
        when classe_taxonomica = 'nan'
        then null
        else safe_cast(classe_taxonomica as string)
    end as classe_taxonomica,
    case
        when ordem_taxonomica = 'nan'
        then null
        else safe_cast(ordem_taxonomica as string)
    end as ordem_taxonomica,
    case
        when familia_taxonomica = 'nan'
        then null
        else safe_cast(familia_taxonomica as string)
    end as familia_taxonomica,
    case
        when genero_taxonomico = 'nan'
        then null
        else safe_cast(genero_taxonomico as string)
    end as genero_taxonomico,
    case
        when pais_importador = 'nan' then null else safe_cast(pais_importador as string)
    end as pais_importador,
    case
        when pais_exportador = 'nan' then null else safe_cast(pais_exportador as string)
    end as pais_exportador,
    case
        when pais_origem = 'nan' then null else safe_cast(pais_origem as string)
    end as pais_origem,
    safe_cast(quantidade_importada_reportada as float64) quantidade_importada_reportada,
    safe_cast(quantidade_exportada_reportada as float64) quantidade_exportada_reportada,
    safe_cast(termo as string) termo,
    case
        when unidade_de_medida = 'nan'
        then null
        else safe_cast(unidade_de_medida as string)
    end as unidade_de_medida,
    case
        when finalidade = 'nan' then null else safe_cast(finalidade as string)
    end as finalidade,
    case when fonte = 'nan' then null else safe_cast(fonte as string) end as fonte,
from {{ set_datalake_project("mundo_cites_comercio_staging.especies_ameacadas") }} as t
