{{
    config(
        schema="br_mgi_compras_publicas",
        alias="ata_registro_preco_item",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2023, "end": 2032, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(numero_controle_pncp_ata as string) numero_controle_pncp_ata,
    safe_cast(numero_item as string) numero_item,
    safe_cast(classificacao_fornecedor as string) classificacao_fornecedor,
    safe_cast(numero_ata_registro_preco as string) numero_ata_registro_preco,
    safe_cast(numero_controle_pncp_compra as string) numero_controle_pncp_compra,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_compra as string) numero_compra,
    safe_cast(ano_compra as int64) ano_compra,
    safe_cast(codigo_unidade_gerenciadora as string) codigo_unidade_gerenciadora,
    safe_cast(nome_unidade_gerenciadora as string) nome_unidade_gerenciadora,
    safe_cast(codigo_modalidade_compra as string) codigo_modalidade_compra,
    safe_cast(modalidade_compra as string) modalidade_compra,
    safe_cast(codigo_item as string) codigo_item,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(tipo_item as string) tipo_item,
    safe_cast(codigo_pdm as string) codigo_pdm,
    safe_cast(nome_pdm as string) nome_pdm,
    safe_cast(id_fornecedor as string) id_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(situacao_sicaf as string) situacao_sicaf,
    safe_cast(quantidade_homologada_item as float64) quantidade_homologada_item,
    safe_cast(quantidade_homologada_vencedor as float64) quantidade_homologada_vencedor,
    safe_cast(quantidade_empenhada as float64) quantidade_empenhada,
    safe_cast(maximo_adesao as float64) maximo_adesao,
    safe_cast(valor_unitario as float64) valor_unitario,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(percentual_maior_desconto as float64) percentual_maior_desconto,
    safe_cast(indicador_item_excluido as boolean) indicador_item_excluido,
    safe_cast(data_assinatura as date) data_assinatura,
    safe_cast(data_vigencia_inicial as date) data_vigencia_inicial,
    safe_cast(data_vigencia_final as date) data_vigencia_final,
    safe_cast(data_hora_inclusao as datetime) data_hora_inclusao,
    safe_cast(data_hora_atualizacao as datetime) data_hora_atualizacao,
    safe_cast(data_hora_exclusao as datetime) data_hora_exclusao
from
    {{
        set_datalake_project(
            "br_mgi_compras_publicas_staging.ata_registro_preco_item"
        )
    }} as t
qualify
    row_number() over (
        partition by
            ano,
            numero_controle_pncp_ata,
            numero_item,
            classificacao_fornecedor,
            numero_ata_registro_preco,
            numero_controle_pncp_compra,
            id_compra,
            numero_compra,
            ano_compra,
            codigo_unidade_gerenciadora,
            nome_unidade_gerenciadora,
            codigo_modalidade_compra,
            modalidade_compra,
            codigo_item,
            descricao_item,
            tipo_item,
            codigo_pdm,
            nome_pdm,
            id_fornecedor,
            nome_fornecedor,
            situacao_sicaf,
            cast(quantidade_homologada_item as string),
            cast(quantidade_homologada_vencedor as string),
            cast(quantidade_empenhada as string),
            cast(maximo_adesao as string),
            cast(valor_unitario as string),
            cast(valor_total as string),
            cast(percentual_maior_desconto as string),
            indicador_item_excluido,
            data_assinatura,
            data_vigencia_inicial,
            data_vigencia_final,
            data_hora_exclusao
        order by data_hora_atualizacao desc
    )
    = 1
