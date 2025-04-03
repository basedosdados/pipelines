{{ config(alias="licitacao", schema="br_camara_dados_abertos") }}
select
    safe_cast(ano as int64) ano_licitacao,
    safe_cast(idlicitacao as string) id_licitacao,
    safe_cast(anoprocesso as int64) ano_processo,
    safe_cast(numprocesso as string) id_processo,
    safe_cast(objeto as string) objeto,
    safe_cast(modalidade as string) modalidade,
    safe_cast(tipo as string) tipo,
    safe_cast(situacao as string) situacao,
    safe_cast(dataabertura as date) data_abertura,
    safe_cast(datapublicacao as date) data_publicacao,
    safe_cast(dataautorizacao as date) data_autorizacao,
    safe_cast(numitens as int64) quantidade_item,
    safe_cast(numunidades as int64) quantidade_unidade,
    safe_cast(numpropostas as int64) quantidade_proposta,
    safe_cast(numcontratos as int64) quantidade_contrato,
    safe_cast(vlrestimado as int64) valor_estimado,
    safe_cast(vlrcontratado as int64) valor_contratado,
    safe_cast(vlrpago as int64) valor_pago,
from {{ project_path("br_camara_dados_abertos_staging.licitacao") }} as t
