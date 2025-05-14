{{
    config(
        schema="br_mp_pep",
        materialized="table",
        alias="cargos_funcoes",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2019, "end": 2023, "interval": 1},
        },
        cluster_by="mes",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (True)',
        ],
    )
}}


select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(funcao as string) as funcao,
    safe_cast(natureza_juridica as string) as natureza_juridica,
    safe_cast(orgao_superior as string) as orgao_superior,
    safe_cast(escolaridade_servidor as string) as escolaridade_servidor,
    safe_cast(orgao as string) as orgao,
    safe_cast(regiao as string) as regiao,
    safe_cast(sexo as string) as sexo,
    safe_cast(nivel_funcao as string) as nivel_funcao,
    safe_cast(subnivel_funcao as string) as subnivel_funcao,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(faixa_etaria as string) as faixa_etaria,
    safe_cast(raca_cor as string) as raca_cor,
    safe_cast(cce_e_fce as int64) as cce_e_fce,
    safe_cast(das_e_correlatas as int64) as das_e_correlatas
from {{ set_datalake_project("br_mp_pep_staging.cargos_funcoes") }}
