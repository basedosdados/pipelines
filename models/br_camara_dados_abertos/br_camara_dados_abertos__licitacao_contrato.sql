{{ config(alias="licitacao_contrato", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(ano as int64) ano_licitacao,
    safe_cast(idlicitacao as string) id_licitacao,
    safe_cast(anocontrato as int64) ano_contrato,
    safe_cast(numcontrato as string) id_contrato,
    safe_cast(tipocontrato as string) tipo_contrato,
    safe_cast(situacaocontrato as string) situacao_contrato,
    safe_cast(objeto as string) descricao,
    safe_cast(dataassinatura as date) data_assinatura,
    safe_cast(datapublicacao as date) data_publicacao,
    safe_cast(datainiciovigenciaoriginal as date) data_inicio_contrato,
    safe_cast(datafimvigenciaoriginal as date) data_fim_contrato,
    safe_cast(datafimultimavigencia as date) data_fim_ultima_vigencia,
    safe_cast(fornecedornome as string) nome_fornecedor,
    safe_cast(fornecedorendereco as string) endereco_fornecedor,
    safe_cast(fornecedorcidade as string) cidade_fornecedor,
    safe_cast(fornecedorsiglauf as string) sigla_uf_fornecedor,
    safe_cast(fornecedorcpfcnpj as string) cpf_cnpj_fornecedor,
    safe_cast(vlroriginal as int64) valor_original,
    safe_cast(vlrtotal as int64) valor_total,
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.licitacao_contrato") }}
    as t
