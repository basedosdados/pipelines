{{
    config(
        alias="processo",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1946, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_processo as string) id_processo,
    safe_cast(codigo_materia as string) codigo_materia,
    safe_cast(identificacao as string) identificacao,
    safe_cast(sigla as string) sigla,
    safe_cast(numero as string) numero,
    safe_cast(autoria as string) autoria,
    safe_cast(ementa as string) ementa,
    safe_cast(objetivo as string) objetivo,
    safe_cast(tipo_documento as string) tipo_documento,
    safe_cast(tipo_conteudo as string) tipo_conteudo,
    safe_cast(situacao_atual as string) situacao_atual,
    safe_cast(sigla_tipo_deliberacao as string) sigla_tipo_deliberacao,
    safe_cast(ente_identificador as string) ente_identificador,
    safe_cast(casa_identificadora as string) casa_identificadora,
    safe_cast(norma_gerada as string) norma_gerada,
    safe_cast(apelido as string) apelido,
    safe_cast(tramitando as string) tramitando,
    safe_cast(data_apresentacao as date) data_apresentacao,
    safe_cast(data_deliberacao as date) data_deliberacao,
    safe_cast(data_situacao_atual as date) data_situacao_atual,
    safe_cast(data_ultima_atualizacao as datetime) data_ultima_atualizacao,
    safe_cast(ultima_informacao_atualizada as string) ultima_informacao_atualizada,
    safe_cast(url_documento as string) url_documento,
from {{ set_datalake_project("br_senado_dados_abertos_staging.processo") }} as t
