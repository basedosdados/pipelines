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
    safe_cast(classe_taxonomica as string) classe_taxonomica,
    safe_cast(ordem_taxonomica as string) ordem_taxonomica,
    safe_cast(familia_taxonomica as string) familia_taxonomica,
    safe_cast(genero_taxonomico as string) genero_taxonomico,
    safe_cast(pais_importador as string) pais_importador,
    safe_cast(pais_exportador as string) pais_exportador,
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
