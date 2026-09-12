-- Minas Gerais (MG) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_mg as (
        select
            safe_cast(ano as int64) as ano,
            safe_cast(mes as int64) as mes,
            safe_cast(data as date) as data,
            'MG' as sigla_uf,
            safe_cast(l.id_municipio as string) as id_municipio,
            safe_cast(l.orgao as string) as orgao,
            safe_cast(l.id_unidade_gestora as string) as id_unidade_gestora,
            safe_cast(
                (
                    case
                        when id_empenho != '-1'
                        then
                            concat(
                                id_empenho,
                                ' ',
                                l.orgao,
                                ' ',
                                l.id_municipio,
                                ' ',
                                (right(ano, 2))
                            )
                        when id_empenho = '-1'
                        then
                            concat(
                                id_empenho_origem,
                                ' ',
                                r.orgao,
                                ' ',
                                r.id_municipio,
                                ' ',
                                (right(num_ano_emp_origem, 2))
                            )
                    end
                ) as string
            ) as id_empenho_bd,
            safe_cast(
                (
                    case
                        when id_empenho = '-1'
                        then replace (id_empenho, '-1', id_empenho_origem)
                    end
                ) as string
            ) as id_empenho,
            safe_cast(numero_empenho as string) as numero_empenho,
            safe_cast(
                concat(
                    id_liquidacao,
                    ' ',
                    l.orgao,
                    ' ',
                    l.id_municipio,
                    ' ',
                    (right(ano, 2))
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(id_liquidacao as string) as id_liquidacao,
            safe_cast(numero_liquidacao as string) as numero,
            safe_cast(nome_responsavel as string) as nome_responsavel,
            safe_cast(documento_responsavel as string) as documento_responsavel,
            safe_cast(
                (case when l.id_rsp != '-1' then 1 else 0 end) as bool
            ) as indicador_restos_pagar,
            round(safe_cast(valor_liquidacao_original as float64), 2) as valor_inicial,
            round(safe_cast(valor_anulado as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(valor_liquidacao_original as float64)
                - ifnull(safe_cast(valor_anulado as float64), 0),
                2
            ) as valor_final
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_mg") }} as l
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_rsp_mg") }} as r
            on l.id_rsp = r.id_rsp

    )
select *
from liquidacao_mg
