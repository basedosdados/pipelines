-- Rio Grande do Sul (RS) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidado_rs as (
        select
            min(ano_recebimento) as ano_recebimento,
            safe_cast(ano_operacao as int64) as ano,
            safe_cast(extract(month from date(dt_operacao)) as int64) as mes,
            safe_cast(
                concat(
                    substring(dt_operacao, 1, 4),
                    '-',
                    substring(dt_operacao, 6, 2),
                    '-',
                    substring(dt_operacao, 9, 2)
                ) as date
            ) as data,
            'RS' as sigla_uf,
            safe_cast(a.id_municipio as string) as id_municipio,
            safe_cast(c.cd_orgao as string) as orgao,
            safe_cast(cd_orgao_orcamentario as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(
                concat(
                    nr_liquidacao,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(nr_liquidacao as string) as numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(vl_liquidacao as float64) as valor_inicial
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'L' and (safe_cast(vl_liquidacao as float64) >= 0)
        group by 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18

    ),
    estorno_rs as (
        select
            safe_cast(
                concat(
                    nr_empenho,
                    ' ',
                    c.cd_orgao,
                    ' ',
                    m.id_municipio,
                    ' ',
                    (right(ano_empenho, 2))
                ) as string
            ) as id_empenho_bd,
            -1 * sum(safe_cast(vl_liquidacao as float64)) as valor_anulacao
        from {{ set_datalake_project("world_wb_mides_staging.raw_despesa_rs") }} as c
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_orgao_rs") }} as a
            on c.cd_orgao = a.cd_orgao
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` m
            on m.id_municipio = a.id_municipio
        where tipo_operacao = 'L' and (safe_cast(vl_liquidacao as float64) < 0)
        group by 1

    ),
    frequencia_rs as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from liquidado_rs
        group by 1

    ),
    liquidacao1_rs as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            l.id_empenho_bd,
            id_empenho,
            numero_empenho,
            id_liquidacao_bd,
            id_liquidacao,
            numero,
            nome_responsavel,
            documento_responsavel,
            indicador_restos_pagar,
            sum(valor_inicial) as valor_inicial,
            sum(valor_anulacao / frequencia_id) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            sum(
                valor_inicial - ifnull((valor_anulacao / frequencia_id), 0)
            ) as valor_final
        from liquidado_rs l
        left join estorno_rs e on l.id_empenho_bd = e.id_empenho_bd
        left join frequencia_rs f on l.id_empenho_bd = f.id_empenho_bd
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

    ),
    data_rs as (
        select
            id_liquidacao_bd,
            case when (count(distinct data)) > 1 then 1 else 0 end as ddata
        from liquidacao1_rs
        group by 1

    ),
    liquidacao_rs as (
        select
            ano,
            mes,
            data,
            sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            id_empenho_bd,
            id_empenho,
            numero_empenho,
            case
                when ddata = 1 then (safe_cast(null as string)) else l.id_liquidacao_bd
            end as id_liquidacao_bd,
            id_liquidacao,
            numero,
            nome_responsavel,
            documento_responsavel,
            indicador_restos_pagar,
            round(valor_inicial, 2),
            round(ifnull(valor_anulacao, 0), 2),
            valor_ajuste,
            round(valor_final, 2)
        from liquidacao1_rs l
        left join data_rs d on l.id_liquidacao_bd = d.id_liquidacao_bd

    )
select *
from liquidacao_rs
