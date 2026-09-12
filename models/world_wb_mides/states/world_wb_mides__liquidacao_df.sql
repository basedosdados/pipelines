-- Distrito Federal (DF) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_df as (
        select
            (safe_cast(exercicio as int64)) as ano,
            (safe_cast(extract(month from date(emissao)) as int64)) as mes,
            safe_cast(emissao as date) as data,
            'DF' as sigla_uf,
            '5300108' as id_municipio,
            safe_cast(codigo_ug as string) as orgao,
            safe_cast(codigo_gestao as string) as id_unidade_gestora,
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
            safe_cast(nota_empenho as string) as numero_empenho,
            case
                when length(nota_lancamento) = 11
                then
                    safe_cast(
                        concat(
                            right(nota_lancamento, length(nota_lancamento) - 6),
                            ' ',
                            codigo_ug,
                            ' ',
                            codigo_gestao,
                            ' ',
                            '5300108',
                            ' ',
                            (right(exercicio, 2))
                        ) as string
                    )
            end as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(nota_lancamento as string) as numero,
            safe_cast(credor as string) as nome_responsavel,
            safe_cast(cnpj_cpf_credor as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(replace(valor, ',', '.') as float64), 2) as valor_inicial,
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_df") }}

    )
select *
from liquidacao_df
