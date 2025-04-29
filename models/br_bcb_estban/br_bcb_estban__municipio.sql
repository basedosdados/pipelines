{{
    config(
        alias="municipio",
        schema="br_bcb_estban",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1987, "end": 2024, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnpj_basico as string) cnpj_basico,
    safe_cast(instituicao as string) instituicao,
    safe_cast(agencias_esperadas as int64) agencias_esperadas,
    safe_cast(agencias_processadas as int64) agencias_processadas,
    safe_cast(id_verbete as string) id_verbete,
    safe_cast(valor as float64) valor
from {{ set_datalake_project("br_bcb_estban_staging.municipio") }} as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
