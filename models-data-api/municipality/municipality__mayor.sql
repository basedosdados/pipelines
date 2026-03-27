{{
    config(
        alias="mayor",
        schema="municipality",
        materialized="table",
    )
}}
with
    prefeitos as (
        select
            c.id_municipio,
            c.titulo_eleitoral,
            c.nome,
            c.sigla_partido,
            c.genero,
            date_diff(current_date(), date(c.data_nascimento), year) as idade,
            c.raca,
            r.ano,
            row_number() over (partition by c.id_municipio order by r.ano desc) as rn
        from basedosdados.br_tse_eleicoes.resultados_candidato_municipio as r
        inner join
            basedosdados.br_tse_eleicoes.candidatos as c
            on r.titulo_eleitoral_candidato = c.titulo_eleitoral
            and r.ano = c.ano
        where
            c.cargo = 'prefeito'
            and r.resultado = 'eleito'
            and c.id_municipio is not null
    )

select id_municipio, titulo_eleitoral, nome, sigla_partido, genero, idade, raca,
from prefeitos
where rn = 1
order by id_municipio asc
