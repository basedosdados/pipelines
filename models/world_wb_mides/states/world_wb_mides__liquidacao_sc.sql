-- Santa Catarina (SC) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidado_sc as (
        select
            safe_cast(ano_emp as int64) as ano,
            safe_cast(substring(trim(data_empenho), -7, 2) as int64) as mes,
            safe_cast(null as date) as data,
            'SC' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(null as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    num_empenho,
                    ' ',
                    codigo_orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    (right(cast(ano_emp as string), 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(num_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_liquidacao as float64), 2) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}

    ),
    frequencia_sc as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from liquidado_sc
        group by 1
        order by 2 desc

    ),
    liquidacao_sc as (
        select
            l.ano,
            l.mes,
            l.data,
            l.sigla_uf,
            l.id_municipio,
            l.orgao,
            l.id_unidade_gestora,
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
                    else l.id_empenho_bd
                end
            ) as id_empenho_bd,
            l.id_empenho,
            l.numero_empenho,
            l.id_liquidacao_bd,
            l.id_liquidacao,
            l.numero,
            l.nome_responsavel,
            l.documento_responsavel,
            l.indicador_restos_pagar,
            l.valor_inicial,
            l.valor_anulacao,
            l.valor_ajuste,
            l.valor_final
        from liquidado_sc l
        left join frequencia_sc f on l.id_empenho_bd = f.id_empenho_bd

    )
select *
from liquidacao_sc
