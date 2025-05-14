{{ config(alias="municipio_bioma", schema="br_inpe_prodes", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(bioma as string) bioma,
    safe_cast(area as float64) area_total,
    safe_cast(desmatamento as float64) desmatado,
    safe_cast(floresta as float64) vegetacao_natural,
    safe_cast(nao_floresta as float64) nao_vegetacao_natural,
    safe_cast(hidrografia as float64) hidrografia
from {{ set_datalake_project("br_inpe_prodes_staging.municipio_bioma") }} as t
