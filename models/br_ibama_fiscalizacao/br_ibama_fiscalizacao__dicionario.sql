{{
    config(
        schema="br_ibama_fiscalizacao",
        alias="dicionario",
        materialized="table",
    )
}}

-- Derived from the fact models, never from a cleaning run's output, so the
-- dictionary cannot describe a different population than the tables it documents.
-- The codes come from the data; the labels are the fixed Ibama meanings below.
-- A code the source starts emitting without a label here lands with a null
-- `valor`, which the not_null test on that column turns into a build failure.
with
    codigos as (
        select distinct
            'auto_infracao' id_tabela,
            'indicador_cancelado' nome_coluna,
            indicador_cancelado chave
        from {{ ref("br_ibama_fiscalizacao__auto_infracao") }}
        where indicador_cancelado is not null

        union all
        select distinct 'auto_infracao', 'tipo_pessoa_infrator', tipo_pessoa_infrator
        from {{ ref("br_ibama_fiscalizacao__auto_infracao") }}
        where tipo_pessoa_infrator is not null

        union all
        select distinct 'area_embargada', 'tipo_pessoa_embargado', tipo_pessoa_embargado
        from {{ ref("br_ibama_fiscalizacao__area_embargada") }}
        where tipo_pessoa_embargado is not null

        union all
        select distinct
            'area_embargada', 'indicador_desembargado', indicador_desembargado
        from {{ ref("br_ibama_fiscalizacao__area_embargada") }}
        where indicador_desembargado is not null
    ),
    rotulos as (
        select *
        from
            unnest(
                [
                    struct(
                        'indicador_cancelado' as nome_coluna,
                        'S' as chave,
                        'Sim' as valor
                    ),
                    struct('indicador_cancelado', 'N', 'Não'),
                    struct('indicador_desembargado', 'S', 'Sim'),
                    struct('indicador_desembargado', 'N', 'Não'),
                    struct('tipo_pessoa_infrator', 'PF', 'Pessoa física'),
                    struct('tipo_pessoa_infrator', 'PJ', 'Pessoa jurídica'),
                    struct('tipo_pessoa_embargado', 'PF', 'Pessoa física'),
                    struct('tipo_pessoa_embargado', 'PJ', 'Pessoa jurídica')
                ]
            )
    )
select
    safe_cast(c.id_tabela as string) id_tabela,
    safe_cast(c.nome_coluna as string) nome_coluna,
    safe_cast(c.chave as string) chave,
    safe_cast(null as string) cobertura_temporal,
    safe_cast(r.valor as string) valor
from codigos as c
left join rotulos as r on c.nome_coluna = r.nome_coluna and c.chave = r.chave
qualify
    row_number() over (partition by id_tabela, nome_coluna, chave order by valor) = 1
