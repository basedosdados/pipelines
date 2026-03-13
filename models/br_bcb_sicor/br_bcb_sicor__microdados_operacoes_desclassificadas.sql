{{
    config(
        alias="microdados_operacoes_desclassificadas",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}
select
    safe_cast(ano_emissao as int64) ano_emissao,
    safe_cast(mes_emissao as int64) mes_emissao,
    safe_cast(
        parse_datetime("%m/%d/%Y %H:%M:%S", trim(data_desclassificacao)) as date
    ) data_desclassificacao,
    safe_cast(t.id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(t.numero_ordem as string) numero_ordem,
    safe_cast(id_motivo_desclassificacao as string) id_motivo_desclassificacao,
    safe_cast(valor_desclassificado as float64) valor_desclassificado,
    safe_cast(initcap(tipo_desclassificacao) as string) tipo_desclassificacao,
from
    {{
        set_datalake_project(
            "br_bcb_sicor_staging.microdados_operacoes_desclassificadas"
        )
    }} as t {{ add_ano_mes_operacao_data(["id_referencia_bacen", "numero_ordem"]) }}
