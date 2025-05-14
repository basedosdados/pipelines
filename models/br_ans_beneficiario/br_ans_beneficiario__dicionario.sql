{{ config(alias="dicionario", schema="br_ans_beneficiario") }}

with
    dicionario as (
        -- Consulta para a segunda coluna.
        select
            row_number() over (order by faixa_etaria) as chave,
            faixa_etaria as valor,
            'faixa_etaria' as nome_coluna
        from {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by faixa_etaria

        union all

        -- Consulta para a terceira coluna
        select
            row_number() over (order by contratacao_beneficiario) as chave,
            contratacao_beneficiario as valor,
            'contratacao_beneficiario' as nome_coluna
        from {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by contratacao_beneficiario

        union all

        -- Consulta para a quarta coluna.
        select
            row_number() over (order by segmentacao_beneficiario) as chave,
            segmentacao_beneficiario as valor,
            'segmentacao_beneficiario' as nome_coluna
        from {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by segmentacao_beneficiario

        union all

        -- Consulta para a quarta coluna
        select
            row_number() over (order by abrangencia_beneficiario) as chave,
            abrangencia_beneficiario as valor,
            'abrangencia_beneficiario' as nome_coluna
        from {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by abrangencia_beneficiario

        union all

        -- Consulta para a quinta coluna
        select
            row_number() over (order by cobertura_assistencia_beneficiario) as chave,
            cobertura_assistencia_beneficiario as valor,
            'cobertura_assistencia_beneficiario' as nome_coluna
            {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by cobertura_assistencia_beneficiario

        union all

        -- Consulta para a sexta coluna
        select
            row_number() over (order by tipo_vinculo) as chave,
            tipo_vinculo as valor,
            'tipo_vinculo' as nome_coluna
        from {{ set_datalake_project("br_ans_beneficiario.informacao_consolidada") }}
        group by tipo_vinculo
    ),
    fact as (
        select cobertura_temporal, id_tabela, coluna, valor
        from {{ set_datalake_project("br_ans_beneficiario.dicionario") }}
    )
select
    fact.id_tabela,
    dicionario.nome_coluna,
    dicionario.chave,
    fact.cobertura_temporal,
    dicionario.valor
from dicionario
join fact on dicionario.nome_coluna = fact.coluna and dicionario.valor = fact.valor
