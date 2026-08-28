{{
    config(
        schema="br_mgi_compras_publicas",
        alias="licitacao_item",
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
    safe_cast(id_compra_item as string) id_compra_item,
    safe_cast(numero_licitacao as string) numero_licitacao,
    safe_cast(numero_item_licitacao as string) numero_item_licitacao,
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(nome_uasg as string) nome_uasg,
    safe_cast(numero_aviso as string) numero_aviso,
    safe_cast(codigo_modalidade as string) codigo_modalidade,
    safe_cast(modalidade as string) modalidade,
    safe_cast(codigo_item_material as string) codigo_item_material,
    safe_cast(nome_material as string) nome_material,
    safe_cast(codigo_item_servico as string) codigo_item_servico,
    safe_cast(nome_servico as string) nome_servico,
    safe_cast(descricao_item as string) descricao_item,
    safe_cast(unidade as string) unidade,
    safe_cast(quantidade as float64) quantidade,
    safe_cast(valor_estimado as float64) valor_estimado,
    safe_cast(criterio_julgamento as string) criterio_julgamento,
    safe_cast(beneficio as string) beneficio,
    safe_cast(cnpj_fornecedor as string) cnpj_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(cpf_vencedor as string) cpf_vencedor,
    safe_cast(nome_vencedor_pessoa_fisica as string) nome_vencedor_pessoa_fisica,
    safe_cast(indicador_decreto_7174 as string) indicador_decreto_7174,
    safe_cast(indicador_sustentavel as string) indicador_sustentavel,
    safe_cast(data_alteracao as datetime) data_alteracao
from {{ set_datalake_project("br_mgi_compras_publicas_staging.licitacao_item") }} as t
qualify row_number() over (partition by id_compra_item order by data_alteracao desc) = 1
