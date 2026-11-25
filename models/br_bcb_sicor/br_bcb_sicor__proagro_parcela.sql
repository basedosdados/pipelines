{{
    config(
        alias="proagro_parcela",
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


select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(ltrim(id_natureza_parcela, '0') as string) id_natureza_parcela,
    safe_cast(ltrim(id_instancia, '0') as string) id_instancia,
    safe_cast(ltrim(id_status, '0') as string) id_status,
    safe_cast(parse_date("%d/%m/%Y", data_base) as date) data_base,
    safe_cast(parse_date("%d/%m/%Y", data_remessa) as date) data_remessa,
    safe_cast(parse_date("%d/%m/%Y", data_pagamento) as date) data_pagamento,
    safe_cast(parse_date("%d/%m/%Y", data_atualizacao) as date) data_atualizacao,
    safe_cast(valor_base as float64) valor_base,
    safe_cast(valor_atual as float64) valor_atual,
    safe_cast(valor_pago as float64) valor_pago,
    safe_cast(valor_imposto as float64) valor_imposto
from
    {{ set_datalake_project("br_bcb_sicor_staging.proagro_parcela") }}
    as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
