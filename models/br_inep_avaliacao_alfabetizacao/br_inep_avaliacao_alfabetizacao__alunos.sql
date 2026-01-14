{{
    config(
        alias="alunos",
        schema="br_inep_avaliacao_alfabetizacao",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_escola as string) id_escola,
    safe_cast(id_aluno as string) id_aluno,
    safe_cast(caderno as string) as caderno,
    safe_cast(serie as string) serie,
    safe_cast(rede as string) rede,
    safe_cast(presenca as string) presenca,
    safe_cast(preenchimento_caderno as string) preenchimento_caderno,
    safe_cast(alfabetizado as string) alfabetizado,
    safe_cast(proficiencia as float64) proficiencia,
    safe_cast(peso_aluno as float64) peso_aluno,
from {{ set_datalake_project("br_inep_avaliacao_alfabetizacao_staging.alunos") }} as t
