{{
    config(
        alias="proagro_rcp_gleba",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}


-- A limpeza de WKT é idêntica à de `br_bcb_sicor__recurso_publico_gleba`: a
-- fonte publica os mesmos defeitos nos dois arquivos (dimensão Z, sinais
-- positivos em coordenadas do hemisfério Sul/Oeste, topologias inválidas).
-- Diferenças em relação àquele modelo: as glebas do RCP começam em 2015, não em
-- 2013, e a fonte as divulga em dois arquivos plurianuais
-- (`SICOR_RCP_GLEBAS_2015_2020` e `SICOR_RCP_GLEBAS_2021`) em vez de um por
-- ano, então a materialização é da tabela inteira e não incremental.
with
    raw_data as (
        select
            id_referencia_bacen,
            numero_ordem,
            indice_gleba,
            geometria as geometria_original
        from {{ set_datalake_project("br_bcb_sicor_staging.proagro_rcp_gleba") }}
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
