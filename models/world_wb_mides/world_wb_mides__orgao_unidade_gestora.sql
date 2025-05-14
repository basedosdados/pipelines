{{
    config(
        alias="orgao_unidade_gestora",
        schema="world_wb_mides",
        materialized="table",
        cluster_by=["sigla_uf"],
        labels={"project_id": "basedosdados", "tema": "economia"},
    )
}}
select
    safe_cast(ano as string) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(nome_unidade_gestora as string) nome_unidade_gestora,
    safe_cast(esfera as string) esfera
from
    {{ set_datalake_project("world_wb_mides_staging.orgao_unidade_gestora") }} as t

    -- contém 10 UFs: CE, DF, MG, PB, PE, PR, RJ, RS, SC, SP
    
