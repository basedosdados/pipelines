-- Distrito Federal (DF) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenho_df as (
        select
            (safe_cast(exercicio as int64)) as ano,
            (safe_cast(extract(month from date(lancamento)) as int64)) as mes,
            safe_cast(lancamento as date) as data,
            'DF' as sigla_uf,
            '5300108' as id_municipio,
            safe_cast(codigo_ug as string) as orgao,
            safe_cast(codigo_gestao as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(null as string) as id_licitacao,
            case
                when codigo_licitacao = '1'
                then '11'
                when codigo_licitacao = '2'
                then '1'
                when codigo_licitacao = '3'
                then '2'
                when codigo_licitacao = '4'
                then '3'
                when codigo_licitacao = '5'
                then '8'
                when codigo_licitacao = '6'
                then '10'
                when codigo_licitacao = '7'
                then '99'
                when codigo_licitacao = '8'
                then '32'
                when codigo_licitacao = '9'
                then '4'
                when codigo_licitacao = '10'
                then '32'
                when codigo_licitacao = '11'
                then '31'
                when codigo_licitacao = '12'
                then ''
                when codigo_licitacao = '13'
                then '5'
                when codigo_licitacao = '14'
                then '6'
                when codigo_licitacao = '15'
                then '5'
                when codigo_licitacao = '16'
                then '5'
                when codigo_licitacao = '17'
                then '6'
                when codigo_licitacao = '18'
                then '3'
                when codigo_licitacao = '19'
                then '32'
                when codigo_licitacao = '20'
                then '31'
                when codigo_licitacao = '21'
                then '31'
                when codigo_licitacao = '22'
                then '32'
                when codigo_licitacao = '23'
                then '12'
                when codigo_licitacao = '25'
                then '98'
                when codigo_licitacao = 'INEXIGÍVEL'
                then '10'
            end as modalidade_licitacao,
            safe_cast(
                concat(
                    right(nota_empenho, length(nota_empenho) - 6),
                    ' ',
                    codigo_ug,
                    ' ',
                    codigo_gestao,
                    ' ',
                    '5300108',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nota_empenho as string) as numero,
            safe_cast(descricao as string) as descricao,
            safe_cast(left(modalidade_empenho, 1) as string) as modalidade,
            safe_cast(cast(codigo_funcao as int64) as string) as funcao,
            safe_cast(codigo_subfuncao as string) as subfuncao,
            safe_cast(codigo_programa as string) as programa,
            safe_cast(codigo_acao as string) as acao,
            safe_cast(codigo_natureza as string) as elemento_despesa,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(replace (valor_final, ',', '.') as float64), 2
            ) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_df") }}

    )
select *
from empenho_df
