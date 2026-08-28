{{
    config(
        schema="br_mgi_compras_publicas",
        alias="licitacao_pregao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero as string) numero,
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(nome_uasg as string) nome_uasg,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(numero_portaria as string) numero_portaria,
    safe_cast(situacao_pregao as string) situacao_pregao,
    safe_cast(tipo_pregao as string) tipo_pregao,
    safe_cast(tipo_pregao_compra as string) tipo_pregao_compra,
    safe_cast(objeto as string) objeto,
    safe_cast(valor_estimado_total as float64) valor_estimado_total,
    safe_cast(valor_homologado_total as float64) valor_homologado_total,
    safe_cast(indicador_pertence_14133 as boolean) indicador_pertence_14133,
    safe_cast(data_portaria as date) data_portaria,
    safe_cast(data_edital as date) data_edital,
    safe_cast(data_inicio_proposta as datetime) data_inicio_proposta,
    safe_cast(data_fim_proposta as datetime) data_fim_proposta,
    safe_cast(data_encerramento as datetime) data_encerramento,
    safe_cast(data_resultado as datetime) data_resultado,
    safe_cast(data_alteracao as datetime) data_alteracao
from {{ set_datalake_project("br_mgi_compras_publicas_staging.licitacao_pregao") }} as t
qualify row_number() over (partition by id_compra order by data_alteracao desc) = 1
