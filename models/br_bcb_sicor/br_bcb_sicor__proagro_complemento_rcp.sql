{{
    config(
        alias="proagro_complemento_rcp",
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


-- Ao contrário de `proagro_complemento_cop`, aqui `tipo_cpf_cnpj_periciadora`
-- só traz comprimentos válidos (11 ou 14), então a separação é completa e não
-- existe coluna de CNPJ básico.
select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(
        case
            when length(tipo_cpf_cnpj_periciadora) = 11 then tipo_cpf_cnpj_periciadora
        end as string
    ) cpf_periciadora,
    safe_cast(
        case
            when length(tipo_cpf_cnpj_periciadora) = 14 then tipo_cpf_cnpj_periciadora
        end as string
    ) cnpj_periciadora,
    safe_cast(cpf_perito as string) cpf_perito
from
    {{ set_datalake_project("br_bcb_sicor_staging.proagro_complemento_rcp") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
