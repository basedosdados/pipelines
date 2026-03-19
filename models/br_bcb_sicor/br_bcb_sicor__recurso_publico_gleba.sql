{{
    config(
        alias="recurso_publico_gleba",
        schema="br_bcb_sicor",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
    )
}}


with
    raw_data as (
        select
            id_referencia_bacen,
            numero_ordem,
            indice_gleba,
            geometria as geometria_original
        from {{ set_datalake_project("br_bcb_sicor_staging.recurso_publico_gleba") }}
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
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    t.id_referencia_bacen,
    t.numero_ordem,
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

from
    geography_cast as t
    {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
{% if is_incremental() %}
    where
        -- a pipeline é settada para atualizar sempre um arquivo de ano; logo, precisa
        -- de um insert_overwrite que sobrescreva o ano atual com os dados mais
        -- recentes;
        cast(ano_emissao as int64) = (select max(ano_emissao) from {{ this }})
{% endif %}
