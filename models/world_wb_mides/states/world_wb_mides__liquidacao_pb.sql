-- Paraíba (PB) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_pb as (
        select
            safe_cast(dt_ano as int64) as ano,
            (safe_cast(substring(dt_liquidacao, -7, 2) as int64)) as mes,
            safe_cast(
                concat(
                    substring(dt_liquidacao, -4),
                    '-',
                    substring(dt_liquidacao, -7, 2),
                    '-',
                    substring(dt_liquidacao, 1, 2)
                ) as date
            ) as data,
            'PB' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(null as string) as orgao,
            safe_cast(l.cd_ugestora as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    nu_empenho,
                    ' ',
                    l.cd_ugestora,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(dt_ano, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nu_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(nu_liquidacao as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(vl_liquidacao as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(vl_liquidacao as float64), 2) as valor_final,
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_pb") }} l
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pb") }} m
            on l.cd_ugestora = safe_cast(m.id_unidade_gestora as string)

    )
select *
from liquidacao_pb
