{{
    config(
        alias="setor_censitario_2010",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

with
    tb as (
        select
            safe_cast(id_setor_censitario as string) id_setor_censitario,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
            safe_cast(id_distrito as string) id_distrito,
            safe_cast(id_subdistrito as string) id_subdistrito,
            safe_cast(nome_subdistrito as string) nome_subdistrito,
            safe_cast(id_bairro as string) id_bairro,
            safe_cast(nome_bairro as string) nome_bairro,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(situacao_setor as string) situacao_setor,
            safe_cast(tipo_setor as string) tipo_setor
        from `basedosdados.br_bd_diretorios_brasil.setor_censitario_2010` as t
    )

select
    a.id_setor_censitario,
    a.id_municipio,
    b.nome as nome_municipio,
    a.id_regiao_metropolitana,
    b.nome_regiao_metropolitana as nome_regiao_metropolitana,
    a.id_distrito,
    c.nome as nome_distrito,
    a.id_subdistrito,
    a.nome_subdistrito,
    a.id_bairro,
    a.nome_bairro,
    a.sigla_uf,
    a.situacao_setor,
    a.tipo_setor,
from tb as a
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as b
    on a.id_municipio = b.id_municipio
left join
    (select * from `basedosdados.br_bd_diretorios_brasil.distrito_2010`) as c
    on a.id_distrito = c.id_distrito
