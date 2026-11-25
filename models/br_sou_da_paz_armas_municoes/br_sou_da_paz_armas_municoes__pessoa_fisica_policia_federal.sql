{{
    config(
        alias="pessoa_fisica_policia_federal",
        schema="br_sou_da_paz_armas_municoes",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(periodo as string) periodo,
    safe_cast(consolidado as bool) consolidado,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(unidade as string) unidade,
    safe_cast(quantidade as int64) quantidade,
from
    {{
        set_datalake_project(
            "br_sou_da_paz_armas_municoes_staging.pessoa_fisica_policia_federal"
        )
    }} as t
