{{
    config(
        alias="nova_arma_registro_cac",
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
    safe_cast(categoria_informada as string) categoria_informada,
    safe_cast(categoria_principal as string) categoria_principal,
    safe_cast(macrocategoria as string) macrocategoria,
from
    {{
        set_datalake_project(
            "br_sou_da_paz_armas_municoes_staging.nova_arma_registro_cac"
        )
    }} as t
