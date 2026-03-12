{{
    config(
        alias="microdados_recurso_publico_gleba",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}

with
    raw_data as (
        select
            id_referencia_bacen,
            numero_ordem,
            indice_gleba,
            geometria as geometria_original
        from
            {{
                set_datalake_project(
                    "br_bcb_sicor_staging.microdados_recurso_publico_gleba"
                )
            }}
    ),

    cleaned_wkt as (
        select
            id_referencia_bacen,
            numero_ordem,
            indice_gleba,
            geometria_original,
            -- 1. Remove altitude e limpa o texto
            regexp_replace(
                regexp_replace(
                    geometria_original,
                    r'([-+]?\d+\.?\d*)\s+([-+]?\d+\.?\d*)\s+[-+]?\d+\.?\d*',
                    r'\1 \2'
                ),
                r'(?i) Z ',
                ' '
            ) as stripped_wkt
        from raw_data
    ),

    normalized_wkt as (
        select
            *,
            -- 2. Força sinais negativos para alinhar com o BBox do Brasil
            regexp_replace(
                stripped_wkt, r'([ (\,])(\d+\.?\d*)', r'\1-\2'
            ) as fixed_negatives
        from cleaned_wkt
    ),

    geography_cast as (
        select
            *,
            -- 3. Converte para GEOGRAPHY. O SAFE evita erro de sintaxe, retornando
            -- NULL.
            safe.st_geogfromtext(fixed_negatives, make_valid => true) as geog_temp
        from normalized_wkt
    )

select
    id_referencia_bacen,
    numero_ordem,
    indice_gleba,
    geometria_original,

    -- 4. Validação do centroíde dos polígonos utilizando bbox
    case
        when
            geog_temp is not null
            and not st_isempty(geog_temp)
            and st_x(st_centroid(geog_temp)) between -74 and -34
            and st_y(st_centroid(geog_temp)) between -34 and 6
        then geog_temp
        else null
    end as geometria

from geography_cast
