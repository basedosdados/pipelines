{{
    config(
        alias="microdados",
        schema="br_anatel_banda_larga_fixa",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2023, "interval": 1},
        },
        cluster_by=["id_municipio", "mes"],
        labels={"project_id": "basedosdados"},
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6 OR                                   DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 0)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 6                                  OR  DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 0)',
        ],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnpj as string) cnpj,
    safe_cast(empresa as string) empresa,
    safe_cast(porte_empresa as string) porte_empresa,
    safe_cast(tecnologia as string) tecnologia,
    safe_cast(transmissao as string) transmissao,
    safe_cast(velocidade as string) velocidade,
    safe_cast(produto as string) produto,
    safe_cast(acessos as int64) acessos
from {{ set_datalake_project("br_anatel_banda_larga_fixa_staging.microdados") }} as t
