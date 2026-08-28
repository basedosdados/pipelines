{{
    config(
        schema="br_mgi_compras_publicas",
        alias="ata_registro_preco",
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
    safe_cast(numero_ata_registro_preco as string) numero_ata_registro_preco,
    safe_cast(numero_controle_pncp_compra as string) numero_controle_pncp_compra,
    safe_cast(id_compra as string) id_compra,
    safe_cast(numero_compra as string) numero_compra,
    safe_cast(ano_compra as int64) ano_compra,
    safe_cast(codigo_unidade_gerenciadora as string) codigo_unidade_gerenciadora,
    safe_cast(nome_unidade_gerenciadora as string) nome_unidade_gerenciadora,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(codigo_modalidade_compra as string) codigo_modalidade_compra,
    safe_cast(modalidade_compra as string) modalidade_compra,
    safe_cast(status_ata as string) status_ata,
    safe_cast(objeto as string) objeto,
    safe_cast(quantidade_itens as int64) quantidade_itens,
    safe_cast(valor_total as float64) valor_total,
    safe_cast(link_ata_pncp as string) link_ata_pncp,
    safe_cast(link_compra_pncp as string) link_compra_pncp,
    safe_cast(indicador_ata_excluida as boolean) indicador_ata_excluida,
    safe_cast(data_assinatura as date) data_assinatura,
    safe_cast(data_vigencia_inicial as date) data_vigencia_inicial,
    safe_cast(data_vigencia_final as date) data_vigencia_final,
    safe_cast(data_hora_inclusao as datetime) data_hora_inclusao,
    safe_cast(data_hora_atualizacao as datetime) data_hora_atualizacao,
    safe_cast(data_hora_exclusao as datetime) data_hora_exclusao
from
    {{ set_datalake_project("br_mgi_compras_publicas_staging.ata_registro_preco") }}
    as t
qualify
    row_number() over (
        partition by numero_controle_pncp_ata order by data_hora_atualizacao desc
    )
    = 1
