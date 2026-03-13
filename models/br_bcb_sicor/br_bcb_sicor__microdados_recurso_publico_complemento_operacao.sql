{{
    config(
        alias="microdados_recurso_publico_complemento_operacao",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
    )
}}

select distinct
    safe_cast(mo.ano_emissao as int64) ano_emissao,
    safe_cast(mo.mes_emissao as int64) mes_emissao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(id_referencia_bacen_efetivo as string) id_referencia_bacen_efetivo,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(
        concat(mo.cnpj_basico_instituicao_financeira, id_agencia) as string
    ) cnpj_agencia,
    safe_cast(numero_cedula as string) as numero_cedula
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_recurso_publico_complemento_operacao"
        )
    }} t
left join
    (
        select distinct
            id_referencia_bacen,
            numero_ordem,
            ano_emissao,
            mes_emissao,
            cnpj_basico_instituicao_financeira
        from {{ ref("br_bcb_sicor__microdados_operacao") }}
    ) as mo using (id_referencia_bacen, numero_ordem)
