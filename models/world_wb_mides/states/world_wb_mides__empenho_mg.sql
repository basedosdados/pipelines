-- Minas Gerais (MG) contribution to world_wb_mides.empenho.
-- Split out of the monolithic empenho model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    empenhado_mg as (
        select
            safe_cast(ano as int64) as ano,
            safe_cast(mes as int64) as mes,
            safe_cast(data as date) as data,
            'MG' as sigla_uf,
            safe_cast(id_municipio as string) as id_municipio,
            safe_cast(trim(orgao) as string) as orgao,
            safe_cast(id_unidade_gestora as string) as id_unidade_gestora,
            safe_cast(null as string) as id_licitacao_bd,
            safe_cast(id_licitacao as string) as id_licitacao,
            safe_cast(null as string) as modalidade_licitacao,
            safe_cast(
                concat(
                    id_empenho, ' ', orgao, ' ', id_municipio, ' ', (right(ano, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(id_empenho as string) as id_empenho,
            safe_cast(numero_empenho as string) as numero,
            safe_cast(lower(descricao) as string) as descricao,
            safe_cast(substring(dsc_modalidade, 5, 1) as string) as modalidade,
            safe_cast(cast(left(dsc_funcao, 2) as int64) as string) as funcao,
            safe_cast(cast(left(dsc_subfuncao, 3) as int64) as string) as subfuncao,
            safe_cast(cast(left(dsc_programa, 4) as int64) as string) as programa,
            safe_cast(cast(left(dsc_acao, 4) as int64) as string) as acao,
            safe_cast(
                replace(left(elemento_despesa, 12), '.', '') as string
            ) as elemento_despesa,
            round(safe_cast(valor_empenho_original as float64), 2) as valor_inicial,
            round(
                safe_cast(ifnull(safe_cast(valor_reforco as float64), 0) as float64), 2
            ) as valor_reforco,
            round(
                safe_cast(ifnull(safe_cast(valor_anulacao as float64), 0) as float64), 2
            ) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(valor_empenho_original as float64)
                + safe_cast(ifnull(safe_cast(valor_reforco as float64), 0) as float64)
                - safe_cast(ifnull(safe_cast(valor_anulacao as float64), 0) as float64),
                2
            ) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}

    ),
    dlic as (
        select
            id_empenho_bd,
            case when (count(distinct id_licitacao)) > 1 then 1 else 0 end as dlic
        from empenhado_mg
        group by 1

    ),
    empenho_mg as (
        select distinct
            e.ano,
            e.mes,
            e.data,
            e.sigla_uf,
            e.id_municipio,
            e.orgao,
            e.id_unidade_gestora,
            e.id_licitacao_bd,
            case
                when dlic = 1 then (safe_cast(null as string)) else e.id_licitacao
            end as id_licitacao,
            e.modalidade_licitacao,
            e.id_empenho_bd,
            e.id_empenho,
            e.numero,
            e.descricao,
            e.modalidade,
            e.funcao,
            e.subfuncao,
            e.programa,
            e.acao,
            e.elemento_despesa,
            e.valor_inicial,
            e.valor_reforco,
            e.valor_anulacao,
            e.valor_ajuste,
            e.valor_final
        from empenhado_mg e
        left join dlic l on l.id_empenho_bd = e.id_empenho_bd

    )
select *
from empenho_mg
