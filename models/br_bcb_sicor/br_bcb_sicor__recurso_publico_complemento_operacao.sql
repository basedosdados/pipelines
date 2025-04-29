{{
    config(
        alias="recurso_publico_complemento_operacao",
        schema="br_bcb_sicor",
        materialized="table",
        partition_by={"field": "id_municipio", "data_type": "string"},
    )
}}

select
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_referencia_bacen_efetivo as string) id_referencia_bacen_efetivo,
    safe_cast(id_agencia as string) id_agencia
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.recurso_publico_complemento_operacao"
        )
    }} t
