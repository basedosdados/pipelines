{{
    config(
        schema="br_tse_filiacao_partidaria",
        alias="microdados_antigos",
        materialized="table",
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(zona as int64) zona,
    safe_cast(secao as int64) secao,
    safe_cast(titulo_eleitoral as string) titulo_eleitoral,
    safe_cast(nome as string) nome,
    ({{ validate_date_range("data_filiacao", "1980-01-01") }}) as data_filiacao,
    safe_cast(situacao_registro as string) situacao_registro,
    safe_cast(tipo_registro as string) tipo_registro,
    {{ validate_date_range("data_processamento", "1980-01-01") }} as data_processamento,
    {{ validate_date_range("data_desfiliacao", "1980-01-01") }} as data_desfiliacao,
    {{ validate_date_range("data_cancelamento", "1980-01-01") }} as data_cancelamento,
    {{ validate_date_range("data_regularizacao", "1980-01-01") }} as data_regularizacao,
    safe_cast(motivo_cancelamento as string) motivo_cancelamento,
from
    {{ set_datalake_project("br_tse_filiacao_partidaria_staging.microdados_antigos") }}
    as t
