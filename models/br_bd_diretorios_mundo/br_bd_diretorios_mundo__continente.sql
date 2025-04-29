{{ config(alias="continente", schema="br_bd_diretorios_mundo") }}

select
    safe_cast(sigla as string) sigla,
    safe_cast(nome_pt as string) nome_pt,
    safe_cast(nome_en as string) nome_en
from {{ set_datalake_project("br_bd_diretorios_mundo_staging.continente") }} as t
