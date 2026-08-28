{{
    config(
        schema="br_mgi_compras_publicas",
        alias="contratacao_item_resultado",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2021, "end": 2031, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_compra as string) id_compra,
    safe_cast(id_compra_item as string) id_compra_item,
    safe_cast(sequencial_resultado as string) sequencial_resultado,
    safe_cast(numero_controle_pncp as string) numero_controle_pncp,
    safe_cast(id_contratacao_pncp as string) id_contratacao_pncp,
    safe_cast(numero_item_pncp as string) numero_item_pncp,
    safe_cast(cnpj_orgao as string) cnpj_orgao,
    safe_cast(codigo_unidade as string) codigo_unidade,
    safe_cast(id_fornecedor as string) id_fornecedor,
    safe_cast(nome_fornecedor as string) nome_fornecedor,
    safe_cast(tipo_pessoa as string) tipo_pessoa,
    safe_cast(codigo_pais as string) codigo_pais,
    safe_cast(id_porte_fornecedor as string) id_porte_fornecedor,
    safe_cast(porte_fornecedor as string) porte_fornecedor,
    safe_cast(id_natureza_juridica as string) id_natureza_juridica,
    safe_cast(natureza_juridica as string) natureza_juridica,
    safe_cast(ordem_classificacao_srp as string) ordem_classificacao_srp,
    safe_cast(id_situacao_resultado as string) id_situacao_resultado,
    safe_cast(situacao_resultado as string) situacao_resultado,
    safe_cast(motivo_cancelamento as string) motivo_cancelamento,
    safe_cast(quantidade_homologada as float64) quantidade_homologada,
    safe_cast(valor_unitario_homologado as float64) valor_unitario_homologado,
    safe_cast(valor_total_homologado as float64) valor_total_homologado,
    safe_cast(percentual_desconto as float64) percentual_desconto,
    safe_cast(
        id_amparo_legal_margem_preferencia as string
    ) id_amparo_legal_margem_preferencia,
    safe_cast(
        amparo_legal_margem_preferencia as string
    ) amparo_legal_margem_preferencia,
    safe_cast(
        id_amparo_legal_criterio_desempate as string
    ) id_amparo_legal_criterio_desempate,
    safe_cast(
        amparo_legal_criterio_desempate as string
    ) amparo_legal_criterio_desempate,
    safe_cast(id_moeda_estrangeira as string) id_moeda_estrangeira,
    safe_cast(
        valor_nominal_moeda_estrangeira as float64
    ) valor_nominal_moeda_estrangeira,
    safe_cast(
        data_cotacao_moeda_estrangeira as datetime
    ) data_cotacao_moeda_estrangeira,
    safe_cast(
        timezone_cotacao_moeda_estrangeira as string
    ) timezone_cotacao_moeda_estrangeira,
    safe_cast(id_pais_origem_produto_servico as string) id_pais_origem_produto_servico,
    safe_cast(indicador_subcontratacao as boolean) indicador_subcontratacao,
    safe_cast(
        indicador_aplicacao_margem_preferencia as boolean
    ) indicador_aplicacao_margem_preferencia,
    safe_cast(
        indicador_aplicacao_beneficio_meepp as boolean
    ) indicador_aplicacao_beneficio_meepp,
    safe_cast(
        indicador_aplicacao_criterio_desempate as boolean
    ) indicador_aplicacao_criterio_desempate,
    safe_cast(data_resultado_pncp as datetime) data_resultado_pncp,
    safe_cast(data_inclusao_pncp as datetime) data_inclusao_pncp,
    safe_cast(data_atualizacao_pncp as datetime) data_atualizacao_pncp,
    safe_cast(data_cancelamento_pncp as datetime) data_cancelamento_pncp
from
    {{
        set_datalake_project(
            "br_mgi_compras_publicas_staging.contratacao_item_resultado"
        )
    }} as t
qualify
    row_number() over (
        partition by id_compra_item, sequencial_resultado
        order by data_atualizacao_pncp desc
    )
    = 1
