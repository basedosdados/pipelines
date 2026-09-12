{{
    config(
        schema="br_mgi_compras_publicas",
        alias="contrato",
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
    safe_cast(receita_despesa as string) receita_despesa,
    safe_cast(codigo_tipo as string) codigo_tipo,
    safe_cast(tipo as string) tipo,
    safe_cast(codigo_categoria as string) codigo_categoria,
    safe_cast(categoria as string) categoria,
    safe_cast(codigo_subcategoria as string) codigo_subcategoria,
    safe_cast(subcategoria as string) subcategoria,
    safe_cast(objeto as string) objeto,
    safe_cast(informacoes_complementares as string) informacoes_complementares,
    safe_cast(unidades_requisitantes as string) unidades_requisitantes,
    safe_cast(valor_global as float64) valor_global,
    safe_cast(numero_parcelas as int64) numero_parcelas,
    safe_cast(valor_parcela as float64) valor_parcela,
    safe_cast(valor_acumulado as float64) valor_acumulado,
    safe_cast(total_despesas_acessorias as float64) total_despesas_acessorias,
    safe_cast(indicador_contrato_excluido as boolean) indicador_contrato_excluido,
    safe_cast(data_vigencia_inicial as date) data_vigencia_inicial,
    safe_cast(data_vigencia_final as date) data_vigencia_final,
    safe_cast(data_hora_inclusao as datetime) data_hora_inclusao,
    safe_cast(data_hora_exclusao as datetime) data_hora_exclusao
from {{ set_datalake_project("br_mgi_compras_publicas_staging.contrato") }} as t
qualify
    row_number() over (
        partition by
            ano,
            numero_controle_pncp_contrato,
            numero_contrato,
            codigo_orgao,
            nome_orgao,
            codigo_unidade_gestora,
            nome_unidade_gestora,
            codigo_unidade_gestora_origem,
            nome_unidade_gestora_origem,
            codigo_unidade_realizadora_compra,
            nome_unidade_realizadora_compra,
            id_compra,
            numero_controle_pncp_compra,
            numero_compra,
            codigo_modalidade_compra,
            modalidade_compra,
            id_fornecedor,
            nome_fornecedor,
            processo,
            receita_despesa,
            codigo_tipo,
            tipo,
            codigo_categoria,
            categoria,
            codigo_subcategoria,
            subcategoria,
            objeto,
            informacoes_complementares,
            unidades_requisitantes,
            cast(valor_global as string),
            numero_parcelas,
            cast(valor_parcela as string),
            cast(valor_acumulado as string),
            cast(total_despesas_acessorias as string),
            indicador_contrato_excluido,
            data_vigencia_inicial,
            data_vigencia_final
        order by data_hora_inclusao desc
    )
    = 1
