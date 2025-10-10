{{
    config(
        alias="uf", schema="br_inep_avaliacao_alfabetizacao", materialized="table"
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(serie as string) serie,
    safe_cast(rede as string) rede,
    safe_cast(taxa_alfabetizacao as float64) taxa_alfabetizacao,
    safe_cast(media_portugues as float64) media_portugues,
    safe_cast(pc_aluno_nivel_0_lp as float64) proporcao_aluno_nivel_0,
    safe_cast(pc_aluno_nivel_1_lp as float64) proporcao_aluno_nivel_1,
    safe_cast(pc_aluno_nivel_2_lp as float64) proporcao_aluno_nivel_2,
    safe_cast(pc_aluno_nivel_3_lp as float64) proporcao_aluno_nivel_3,
    safe_cast(pc_aluno_nivel_4_lp as float64) proporcao_aluno_nivel_4,
    safe_cast(pc_aluno_nivel_5_lp as float64) proporcao_aluno_nivel_5,
    safe_cast(pc_aluno_nivel_6_lp as float64) proporcao_aluno_nivel_6,
    safe_cast(pc_aluno_nivel_7_lp as float64) proporcao_aluno_nivel_7,
    safe_cast(pc_aluno_nivel_8_lp as float64) proporcao_aluno_nivel_8,
from {{ set_datalake_project("br_inep_avaliacao_alfabetizacao_staging.uf") }} as t
