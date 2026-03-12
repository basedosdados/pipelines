{{
    config(
        alias="microdados_operacoes_desclassificadas",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}
select
    safe_cast(
        parse_datetime("%m/%d/%Y %H:%M:%S", trim(data_desclassificacao)) as date
    ) data_desclassificacao,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(id_motivo_desclassificacao as string) id_motivo_desclassificacao,
    safe_cast(valor_desclassificado as float64) valor_desclassificado,
    safe_cast(initcap(tipo_desclassificacao) as string) tipo_desclassificacao,
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_operacoes_desclassificadas"
        )
    }} as t
