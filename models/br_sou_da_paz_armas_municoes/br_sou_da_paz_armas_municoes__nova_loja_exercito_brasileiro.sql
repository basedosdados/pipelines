{{
    config(
        alias="nova_loja_exercito_brasileiro",
        schema="br_sou_da_paz_armas_municoes",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(periodo as string) periodo,
    safe_cast(consolidado as bool) consolidado,
    safe_cast(id_regiao_militar as string) id_regiao_militar,
    safe_cast(area_regiao_militar as string) area_regiao_militar,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(unidade as string) unidade,
    safe_cast(quantidade as int64) quantidade,
from
    {{
        set_datalake_project(
            "br_sou_da_paz_armas_municoes_staging.nova_loja_exercito_brasileiro"
        )
    }} as t
