{{
    config(
        schema="br_mgi_compras_publicas",
        alias="contrato_item",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(numero_controle_pncp_contrato as string) numero_controle_pncp_contrato,
    safe_cast(numero_contrato as string) numero_contrato,
    safe_cast(numero_item as string) numero_item,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_unidade_gestora as string) codigo_unidade_gestora,
    safe_cast(nome_unidade_gestora as string) nome_unidade_gestora,
    safe_cast(codigo_unidade_gestora_origem as string) codigo_unidade_gestora_origem,
    safe_cast(nome_unidade_gestora_origem as string) nome_unidade_gestora_origem,
    safe_cast(
        codigo_unidade_realizadora_compra as string
    ) codigo_unidade_realizadora_compra,
    safe_cast(
        nome_unidade_realizadora_compra as string
    ) nome_unidade_realizadora_compra,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_controle_pncp_compra as string) numero_controle_pncp_compra,
    safe_cast(numero_compra as string) numero_compra,
    safe_cast(codigo_modalidade_compra as string) codigo_modalidade_compra,
    safe_cast(modalidade_compra as string) modalidade_compra,
    safe_cast(id_fornecedor as string) id_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(processo as string) processo,
    safe_cast(esfera as string) esfera,
    safe_cast(poder as string) poder,
    safe_cast(tipo_item as string) tipo_item,
    safe_cast(codigo_item as string) codigo_item,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(quantidade_item as float64) quantidade_item,
    safe_cast(valor_unitario_item as float64) valor_unitario_item,
    safe_cast(valor_total_item as float64) valor_total_item,
    safe_cast(valor_global as float64) valor_global,
    safe_cast(indicador_contrato_excluido as boolean) indicador_contrato_excluido,
    safe_cast(indicador_item_excluido as boolean) indicador_item_excluido,
    safe_cast(data_vigencia_inicial as date) data_vigencia_inicial,
    safe_cast(data_vigencia_final as date) data_vigencia_final,
    safe_cast(data_hora_inclusao as datetime) data_hora_inclusao,
    safe_cast(data_hora_exclusao_item as datetime) data_hora_exclusao_item,
    safe_cast(data_hora_exclusao_contrato as datetime) data_hora_exclusao_contrato
from {{ set_datalake_project("br_mgi_compras_publicas_staging.contrato_item") }} as t
qualify
    row_number() over (
        partition by codigo_orgao, codigo_unidade_gestora, numero_contrato, numero_item
        order by data_hora_inclusao desc
    )
    = 1
