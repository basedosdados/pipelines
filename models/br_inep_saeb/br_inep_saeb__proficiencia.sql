{{
    config(
        alias="proficiencia",
        schema="br_inep_saeb",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2023, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "educacao"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_escola as string) id_escola,
    safe_cast(rede as string) rede,
    safe_cast(localizacao as string) localizacao,
    safe_cast(serie as int64) serie,
    safe_cast(turno as string) turno,
    safe_cast(disciplina as string) disciplina,
    safe_cast(id_turma as string) id_turma,
    safe_cast(id_aluno as string) id_aluno,
    safe_cast(sexo as string) sexo,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(estrato as int64) estrato,
    safe_cast(amostra as int64) amostra,
    safe_cast(peso_aluno as float64) peso_aluno,
    safe_cast(proficiencia as float64) proficiencia,
    safe_cast(erro_padrao as float64) erro_padrao,
    safe_cast(proficiencia_saeb as float64) proficiencia_saeb,
    safe_cast(erro_padrao_saeb as float64) erro_padrao_saeb
from {{ set_datalake_project("br_inep_saeb_staging.proficiencia") }} as t
