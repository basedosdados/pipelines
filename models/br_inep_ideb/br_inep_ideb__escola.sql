{{ config(alias="escola", schema="br_inep_ideb", materialized="table") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_escola as string) id_escola,
    safe_cast(rede as string) rede,
    safe_cast(ensino as string) ensino,
    safe_cast(anos_escolares as string) anos_escolares,
    safe_cast(taxa_aprovacao as float64) taxa_aprovacao,
    safe_cast(indicador_rendimento as float64) indicador_rendimento,
    safe_cast(nota_saeb_matematica as float64) nota_saeb_matematica,
    safe_cast(nota_saeb_lingua_portuguesa as float64) nota_saeb_lingua_portuguesa,
    safe_cast(nota_saeb_media_padronizada as float64) nota_saeb_media_padronizada,
    safe_cast(ideb as float64) ideb,
    safe_cast(projecao as float64) projecao,
from {{ set_datalake_project("br_inep_ideb_staging.escola") }} as t
