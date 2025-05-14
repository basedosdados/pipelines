{{ config(alias="licitacao_item", schema="br_camara_dados_abertos") }}
select
    safe_cast(ano as int64) ano_licitacao,
    safe_cast(idlicitacao as string) id_licitacao,
    safe_cast(numitem as string) id_item,
    safe_cast(replace(numsubitem, '.0', '') as string) id_sub_item,
    safe_cast(descricao as string) descricao,
    safe_cast(especificacao as string) especificacao,
    safe_cast(unidade as string) unidade,
    safe_cast(qtdlicitada as int64) quantidade_licitada,
    safe_cast(replace(vlrunitarioestimado, ".0", "") as int64) valor_unitario_estimado,
    safe_cast(replace(qtdcontratada, ".0", "") as int64) quantidade_contratada,
    safe_cast(
        replace(vlrunitariocontratado, ".0", "") as int64
    ) valor_unitario_contratado,
    safe_cast(replace(vlrtotalcontratado, ".0", "") as int64) valor_total_contratado,
    safe_cast(fornecedorcpfcnpj as string) cpf_cnpj_fornecedor,
    safe_cast(fornecedornome as string) nome_fornecedor,
    safe_cast(numcontrato as string) id_contrato,
    safe_cast(replace(anocontrato, ".0", "") as int64) ano_contrato,
    safe_cast(tipocontrato as string) tipo_contrato,
    safe_cast(situacaoitem as string) situacao_item,
    safe_cast(observacoes as string) observacao,
    safe_cast(naturezadespesa as string) natureza_despesa,
    safe_cast(programatrabalho as string) programa_trabalho,
from {{ set_datalake_project("br_camara_dados_abertos_staging.licitacao_item") }} as t
