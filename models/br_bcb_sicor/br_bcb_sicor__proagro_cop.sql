{{
    config(
        alias="proagro_cop",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2031, "interval": 1},
        },
        pre_hook="             BEGIN                 DROP ALL ROW ACCESS POLICIES ON {{ this }};             EXCEPTION WHEN ERROR THEN                 SELECT 1;              END;         ",
    )
}}


-- `ltrim(..., '0')` reproduz o tratamento dos códigos em
-- `br_bcb_sicor__operacao`, para que as mesmas colunas sejam comparáveis entre
-- as duas tabelas e casem com o modelo `dicionario`. Aqui o único código
-- afetado é `id_tipo_solo`, cujo valor 0 ("Não se aplica") vira string vazia —
-- ver a seção "O código zero" no README do pipeline.
select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(ltrim(id_evento, '0') as string) id_evento,
    safe_cast(ltrim(id_status, '0') as string) id_status,
    safe_cast(ltrim(id_tipo_ciclo_cultivar, '0') as string) id_tipo_ciclo_cultivar,
    safe_cast(ltrim(id_tipo_solo, '0') as string) id_tipo_solo,
    safe.parse_date("%d/%m/%Y", data_comunicacao) data_comunicacao,
    {{ parse_data_agronomica_sicor("data_inicio_plantio") }} data_inicio_plantio,
    {{ parse_data_agronomica_sicor("data_fim_plantio") }} data_fim_plantio,
    {{ parse_data_agronomica_sicor("data_inicio_colheita") }} data_inicio_colheita,
    {{ parse_data_agronomica_sicor("data_fim_colheita") }} data_fim_colheita
from
    {{ set_datalake_project("br_bcb_sicor_staging.proagro_cop") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
