{{
    config(
        alias="cid_10",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(subcategoria as string) subcategoria,
    safe_cast(descricao_subcategoria as string) descricao_subcategoria,
    safe_cast(categoria as string) categoria,
    safe_cast(descricao_categoria as string) descricao_categoria,
    safe_cast(capitulo as string) capitulo,
    safe_cast(descricao_capitulo as string) descricao_capitulo,
    safe_cast(causa_violencia as int64) causa_violencia,
    safe_cast(causa_overdose as int64) causa_overdose,
    safe_cast(cid_datasus as int64) cid_datasus,
from {{ set_datalake_project("br_bd_diretorios_brasil_staging.cid_10") }} as t
