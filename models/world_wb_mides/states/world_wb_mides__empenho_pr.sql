-- Paraná (PR) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenho_pr as (
        select
            safe_cast(nranoempenho as int64) as ano,
            (safe_cast(extract(month from date(dtempenho)) as int64)) as mes,
            safe_cast(extract(date from timestamp(dtempenho)) as date) as data,
            'PR' as sigla_uf,
            safe_cast(m.id_municipio as string) as id_municipio,
            safe_cast(trim(cdorgao, '0') as string) as orgao,
            safe_cast(cdunidade as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(null as string) as id_licitacao,
            safe_cast(null as string) as modalidade_licitacao,
            safe_cast(
                concat(idempenho, ' ', m.id_municipio) as string
            ) as id_empenho_bd,
            safe_cast(idempenho as string) as id_empenho,
            safe_cast(nrempenho as string) as numero,
            safe_cast(lower(dshistorico) as string) as descricao,
            safe_cast(left(dstipoempenho, 1) as string) as modalidade,
            safe_cast(safe_cast(cdfuncao as int64) as string) as funcao,
            safe_cast(safe_cast(cdsubfuncao as int64) as string) as subfuncao,
            safe_cast(safe_cast(cdprograma as int64) as string) as programa,
            safe_cast(safe_cast(cdprojetoatividade as int64) as string) as acao,
            safe_cast(
                concat(
                    cdcategoriaeconomica, cdgruponatureza, cdmodalidade, cdelemento
                ) as string
            ) as elemento_despesa,
            round(safe_cast(vlempenho as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(safe_cast(vlestornoempenho as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(vlempenho as float64)
                - ifnull(safe_cast(vlestornoempenho as float64), 0),
                2
            ) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pr") }} e
        left join
            basedosdados.br_bd_diretorios_brasil.municipio m
            on e.cdibge = m.id_municipio_6

    )
select *
from empenho_pr
