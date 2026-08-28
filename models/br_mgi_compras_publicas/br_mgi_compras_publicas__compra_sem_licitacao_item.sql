{{
    config(
        schema="br_mgi_compras_publicas",
        alias="compra_sem_licitacao_item",
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
    safe_cast(numero_item_material as string) numero_item_material,
    safe_cast(codigo_uasg as string) codigo_uasg,
    safe_cast(codigo_orgao as string) codigo_orgao,
    safe_cast(numero_aviso as string) numero_aviso,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(codigo_modalidade as string) codigo_modalidade,
    safe_cast(quantidade_total_item as int64) quantidade_total_item,
    safe_cast(objeto_licitacao as string) objeto_licitacao,
    safe_cast(fundamento_legal as string) fundamento_legal,
    safe_cast(justificativa as string) justificativa,
    safe_cast(
        nome_responsavel_declaracao_dispensa as string
    ) nome_responsavel_declaracao_dispensa,
    safe_cast(
        cargo_responsavel_declaracao_dispensa as string
    ) cargo_responsavel_declaracao_dispensa,
    safe_cast(nome_responsavel_ratificacao as string) nome_responsavel_ratificacao,
    safe_cast(cargo_responsavel_ratificacao as string) cargo_responsavel_ratificacao,
    safe_cast(modalidade as string) modalidade,
    safe_cast(numero_inciso as string) numero_inciso,
    safe_cast(tipo_item as string) tipo_item,
    safe_cast(codigo_conjunto_materiais as string) codigo_conjunto_materiais,
    safe_cast(nome_conjunto_materiais as string) nome_conjunto_materiais,
    safe_cast(codigo_servico as string) codigo_servico,
    safe_cast(nome_servico as string) nome_servico,
    safe_cast(descricao_detalhada as string) descricao_detalhada,
    safe_cast(marca_material as string) marca_material,
    safe_cast(fabricante as string) fabricante,
    safe_cast(unidade_medida as string) unidade_medida,
    safe_cast(quantidade_material as float64) quantidade_material,
    safe_cast(valor_estimado as float64) valor_estimado,
    safe_cast(valor_estimado_item as float64) valor_estimado_item,
    safe_cast(tipo_fornecedor_vencedor as string) tipo_fornecedor_vencedor,
    safe_cast(nome_fornecedor_vencedor as string) nome_fornecedor_vencedor,
    safe_cast(cnpj_vencedor as string) cnpj_vencedor,
    safe_cast(cpf_vencedor as string) cpf_vencedor,
    safe_cast(
        cpf_responsavel_declaracao_dispensa as string
    ) cpf_responsavel_declaracao_dispensa,
    safe_cast(cpf_responsavel_ratificacao as string) cpf_responsavel_ratificacao,
    safe_cast(cpf_responsavel_publicacao as string) cpf_responsavel_publicacao,
    safe_cast(data_publicacao as date) data_publicacao,
    safe_cast(data_alteracao as datetime) data_alteracao
from
    {{
        set_datalake_project(
            "br_mgi_compras_publicas_staging.compra_sem_licitacao_item"
        )
    }} as t
qualify
    row_number() over (
        partition by
            ano,
            id_compra,
            id_compra_item,
            numero_item_material,
            codigo_uasg,
            codigo_orgao,
            numero_aviso,
            numero_processo,
            codigo_modalidade,
            quantidade_total_item,
            objeto_licitacao,
            fundamento_legal,
            justificativa,
            nome_responsavel_declaracao_dispensa,
            cargo_responsavel_declaracao_dispensa,
            nome_responsavel_ratificacao,
            cargo_responsavel_ratificacao,
            modalidade,
            numero_inciso,
            tipo_item,
            codigo_conjunto_materiais,
            nome_conjunto_materiais,
            codigo_servico,
            nome_servico,
            descricao_detalhada,
            marca_material,
            fabricante,
            unidade_medida,
            quantidade_material,
            valor_estimado,
            valor_estimado_item,
            tipo_fornecedor_vencedor,
            nome_fornecedor_vencedor,
            cnpj_vencedor,
            cpf_vencedor,
            cpf_responsavel_declaracao_dispensa,
            cpf_responsavel_ratificacao,
            cpf_responsavel_publicacao,
            data_publicacao,
            data_alteracao
        order by data_alteracao desc
    )
    = 1
