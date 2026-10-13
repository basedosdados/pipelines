-- Santa Catarina (SC) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenhado_sc as (
        select
            safe_cast(ano_emp as int64) as ano,
            safe_cast(substring(trim(data_empenho), -7, 2) as int64) as mes,
            safe_cast(
                concat(
                    substring(trim(data_empenho), -4),
                    '-',
                    substring(trim(data_empenho), -7, 2),
                    '-',
                    substring(trim(data_empenho), 1, 2)
                ) as date
            ) as data,
            'SC' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(null as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(
                case
                    when
                        (split(nr_licitacao_contrato_convenio, ' / ')[offset(0)])
                        != "Sem Licitação"
                        and (split(nr_licitacao_contrato_convenio, ' / ')[offset(0)])
                        != "Sem licitação"
                        and (split(nr_licitacao_contrato_convenio, ' / ')[offset(0)])
                        != "Sem Licitacao"
                        and (split(nr_licitacao_contrato_convenio, ' / ')[offset(0)])
                        != "SEM LICITACAO"
                    then (split(nr_licitacao_contrato_convenio, ' / ')[offset(0)])
                    else ''
                end as string
            ) as id_licitacao,
            safe_cast(null as string) as modalidade_licitacao,
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
            safe_cast(num_empenho as string) as numero,
            safe_cast(lower(descricao_historico_empenho) as string) as descricao,
            safe_cast(null as string) as modalidade,
            safe_cast(cast(left(funcao, 2) as int64) as string) as funcao,
            safe_cast(cast(left(subfuncao, 3) as int64) as string) as subfuncao,
            safe_cast(null as string) as programa,
            safe_cast(null as string) as acao,
            safe_cast(elemento_despesa as string) as elemento_despesa,
            round(safe_cast(valor_empenho as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_empenho as float64), 2) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}

    ),
    frequencia_sc as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from empenhado_sc
        group by 1
        order by 2 desc

    ),
    empenho_sc as (
        select
            e.ano,
            e.mes,
            e.data,
            e.sigla_uf,
            e.id_municipio,
            e.orgao,
            e.id_unidade_gestora,
            e.id_licitacao_bd,
            e.id_licitacao,
            e.modalidade_licitacao,
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
                    else e.id_empenho_bd
                end
            ) as id_empenho_bd,
            e.id_empenho,
            e.numero,
            e.descricao,
            e.modalidade,
            e.funcao,
            e.subfuncao,
            e.programa,
            e.acao,
            e.elemento_despesa,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_reforco,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            e.valor_final as valor_final
        from empenhado_sc e
        left join frequencia_sc f on e.id_empenho_bd = f.id_empenho_bd

    )
select *
from empenho_sc
