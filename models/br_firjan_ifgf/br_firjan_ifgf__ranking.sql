{{ config(alias="ranking", schema="br_firjan_ifgf", labels={"tema": "economia"}) }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(indice_firjan_gestao_fiscal as float64) indice_firjan_gestao_fiscal,
    safe_cast(ranking_estadual as int64) ranking_estadual,
    safe_cast(ranking_nacional as int64) ranking_nacional,
from {{ set_datalake_project("br_firjan_ifgf_staging.ranking") }} as t
