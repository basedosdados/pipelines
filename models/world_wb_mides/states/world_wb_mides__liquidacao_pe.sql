-- Pernambuco (PE) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_pe as (
        select
            safe_cast(l.anoreferencia as int64) as ano,
            (safe_cast(extract(month from date(data)) as int64)) as mes,
            safe_cast(extract(date from timestamp(data)) as date) as data,
            'PE' as sigla_uf,
            safe_cast(codigoibge as string) as id_municipio,
            safe_cast(null as string) orgao,
            safe_cast(id_unidadegestora as string) as id_unidade_gestora,
            safe_cast(null as string) as id_empenho_bd,
            safe_cast(trim(idempenho) as string) as id_empenho,
            safe_cast(l.numeroempenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(valor as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor as float64), 2) as valor_final,
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_pe") }} l
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_pe") }} m
            on l.id_unidade_gestora = safe_cast(m.id_unidadegestora as string)

    )
select *
from liquidacao_pe
