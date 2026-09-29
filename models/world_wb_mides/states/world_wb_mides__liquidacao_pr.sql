-- Paraná (PR) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_pr as (
        select
            safe_cast(nranoliquidacao as int64) as ano,
            (safe_cast(extract(month from date(dtliquidacao)) as int64)) as mes,
            safe_cast(extract(date from timestamp(dtliquidacao)) as date) as data,
            'PR' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(cdorgao as string) as orgao,
            safe_cast(cdunidade as string) as id_unidade_gestora,
            safe_cast(
                concat(l.idempenho, ' ', m.id_municipio) as string
            ) as id_empenho_bd,
            safe_cast(l.idempenho as string) as id_empenho,
            safe_cast(nrempenho as string) as numero_empenho,
            safe_cast(
                concat(l.idliquidacao, ' ', m.id_municipio) as string
            ) as id_liquidacao_bd,
            safe_cast(idliquidacao as string) as id_liquidacao,
            safe_cast(nrliquidacao as string) as numero,
            safe_cast(nmliquidante as string) as nome_responsavel,
            safe_cast(nrdocliquidante as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(vlliquidacaobruto as float64), 2) as valor_inicial,
            round(safe_cast(vlliquidacaoestornado as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(vlliquidacaoliquido as float64), 2) as valor_final,
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_pr") }} l
        left join
            basedosdados.br_bd_diretorios_brasil.municipio m on cdibge = id_municipio_6
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pr") }} e
            on l.idempenho = e.idempenho

    )
select *
from liquidacao_pr
