{{
    config(
        alias="discurso",
        schema="br_senado_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2031, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_pronunciamento as string) id_pronunciamento,
    safe_cast(id_sessao as string) id_sessao,
    safe_cast(data_sessao as date) data_sessao,
    safe_cast(sigla_casa as string) sigla_casa,
    safe_cast(sigla_tipo_sessao as string) sigla_tipo_sessao,
    safe_cast(numero_sessao as string) numero_sessao,
    safe_cast(id_senador as string) id_senador,
    safe_cast(tipo_autor as string) tipo_autor,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sigla_tipo_uso_palavra as string) sigla_tipo_uso_palavra,
    safe_cast(descricao_tipo_uso_palavra as string) descricao_tipo_uso_palavra,
    safe_cast(resumo as string) resumo,
    safe_cast(indexacao as string) indexacao,
    safe_cast(url_texto as string) url_texto,
from {{ set_datalake_project("br_senado_dados_abertos_staging.discurso") }} as t
