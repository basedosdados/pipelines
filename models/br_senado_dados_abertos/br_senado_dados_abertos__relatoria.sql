{{
    config(
        alias="relatoria",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_relatoria as string) id_relatoria,
    safe_cast(id_processo as string) id_processo,
    safe_cast(codigo_materia as string) codigo_materia,
    safe_cast(identificacao_processo as string) identificacao_processo,
    safe_cast(id_senador as string) id_senador,
    safe_cast(sigla_casa_relator as string) sigla_casa_relator,
    safe_cast(id_colegiado as string) id_colegiado,
    safe_cast(sigla_colegiado as string) sigla_colegiado,
    safe_cast(nome_colegiado as string) nome_colegiado,
    safe_cast(id_tipo_colegiado as string) id_tipo_colegiado,
    safe_cast(tipo_relator as string) tipo_relator,
    safe_cast(id_tipo_relator as string) id_tipo_relator,
    safe_cast(numero_autuacao as string) numero_autuacao,
    safe_cast(tipo_encerramento as string) tipo_encerramento,
    safe_cast(data_designacao as date) data_designacao,
    safe_cast(data_destituicao as date) data_destituicao,
    safe_cast(tramitando as string) tramitando,
from {{ set_datalake_project("br_senado_dados_abertos_staging.relatoria") }} as t
