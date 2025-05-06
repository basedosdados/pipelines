{{
    config(
        alias="historico_inscritos",
        schema="world_oceanos_mapeamento",
        materialized="table",
    )
}}


with
    nacionalidade_mais_frequente as (
        select *
        from
            (
                select
                    nome_autor as nome_autor_final,
                    nacionalidade_autor as nacionalidade_autor,
                    count(1) as n_aparicoes,
                    row_number() over (
                        partition by nome_autor order by count(1) desc
                    ) as rn
                from
                    `basedosdados-staging.world_oceanos_mapeamento_staging.historico_inscritos`
                where nacionalidade_autor is not null
                group by nome_autor, nacionalidade_autor
            )
        where rn = 1

    ),

    pais_mais_frequente as (
        select *
        from
            (
                select
                    nome_autor as nome_autor_final,
                    pais_residencia_autor as nome_pais_autor,
                    count(1) as n_aparicoes,
                    row_number() over (
                        partition by nome_autor order by count(1) desc
                    ) as rn_pais
                from
                    `basedosdados-staging.world_oceanos_mapeamento_staging.historico_inscritos`
                where pais_residencia_autor is not null
                group by nome_autor, nome_pais_autor
            )
        where rn_pais = 1
    ),

    pais_editora_mais_frequente as (
        select *
        from
            (
                select
                    nome_editora as nome_editora_final_3,
                    sede_editora as pais_origem_editora,
                    sigla_pais_iso2,
                    count(1) as n_aparicoes,
                    row_number() over (
                        partition by nome_editora order by count(1) desc
                    ) as rn_pais_editora
                from
                    `basedosdados-staging.world_oceanos_mapeamento_staging.historico_inscritos`
                    as inscritos
                left join
                    `basedosdados.br_bd_diretorios_mundo.pais` as pais
                    on inscritos.sede_editora = pais.nome
                where sede_editora is not null
                group by nome_editora_final_3, pais_origem_editora, sigla_pais_iso2
            )
        where rn_pais_editora = 1

    )

select
    safe_cast(ano as int64) ano,
    safe_cast(titulo_livro as string) titulo_livro,
    safe_cast(genero_livro as string) genero_livro,
    safe_cast(pais_primeira_edicao as string) pais_primeira_edicao,
    safe_cast(inscritos.nome_autor as string) nome_autor,
    safe_cast(
        nacionalidade_mais_frequente.nacionalidade_autor as string
    ) nacionaldade_autor,
    safe_cast(genero_autor as string) genero_autor,
    safe_cast(faixa_etaria_autor as string) faixa_etaria_autor,
    safe_cast(pais_mais_frequente.nome_pais_autor as string) nome_pais_autor,
    safe_cast(inscritos.nome_editora as string) nome_editora,
    safe_cast(sede_editora as string) sede_editora,
    safe_cast(site_editora as string) site_editora,
    safe_cast(pais_editora.sigla_pais_iso2 as string) sigla_pais_iso2,
    safe_cast(indicador_vencedor as string) indicador_vencedor,
    safe_cast(indicador_finalista as string) indicador_finalista,
    safe_cast(indicador_semifinalista as string) indicador_semifinalista,
from
    `basedosdados-staging.world_oceanos_mapeamento_staging.historico_inscritos` inscritos
left join
    nacionalidade_mais_frequente
    on nacionalidade_mais_frequente.nome_autor_final = inscritos.nome_autor
left join
    pais_mais_frequente on pais_mais_frequente.nome_autor_final = inscritos.nome_autor
left join
    pais_editora_mais_frequente as pais_editora
    on pais_editora.nome_editora_final_3 = inscritos.nome_editora
order by ano desc
