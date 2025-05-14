{{ config(alias="licitacao_proposta", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(ano as int64) ano,
    safe_cast(idlicitacao as string) id_licitacao,
    safe_cast(descricao as string) descricao,
    safe_cast(unidadeslicitadas as int64) quantidade_unidade_licitacao,
    safe_cast(vlrestimado as int64) valor_estimado,
    safe_cast(numproposta as string) id_proposta,
    safe_cast(replace(unidadesproposta, ".0", "") as int64) unidade_proposta,
    safe_cast(vlrproposta as int64) valor_proposta,
    safe_cast(marcaproposta as string) marca_proposta,
    safe_cast(fornecedorcpfcnpj as string) cpf_cnpj_fornecedor,
    safe_cast(fornecedorsituacao as string) fornecedor_situacao,
    safe_cast(dataproposta as date) data_proposta,
    safe_cast(diasvalidadeproposta as int64) validade_proposta,
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.licitacao_proposta") }}
    as t
