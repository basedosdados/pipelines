{{
    config(
        alias="microdados",
        schema="br_anatel_banda_larga_fixa",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2031, "interval": 1},
        },
        cluster_by=["id_municipio", "mes"],
        labels={"project_id": "basedosdados"},
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
