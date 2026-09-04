-- Minas Gerais (MG) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pagamento_mg as (
        select distinct
            safe_cast(p.ano as int64) as ano,
            safe_cast(p.mes as int64) as mes,
            safe_cast(p.data as date) as data,
            safe_cast(p.sigla_uf as string) as sigla_uf,
            safe_cast(p.id_municipio as string) as id_municipio,
            safe_cast(p.orgao as string) as orgao,
            safe_cast(p.id_unidade_gestora as string) as id_unidade_gestora,
            safe_cast(
                case
                    when id_empenho != '-1'
                    then
                        concat(
                            id_empenho,
                            ' ',
                            p.orgao,
                            ' ',
                            p.id_municipio,
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
                end as string
            ) as id_empenho_bd,
            safe_cast(
                case
                    when p.id_empenho = '-1'
                    then replace (p.id_empenho, '-1', id_empenho_origem)
                end as string
            ) as id_empenho,
            safe_cast(p.numero_empenho as string) as numero_empenho,
            safe_cast(
                case
                    when p.id_liquidacao != '-1'
                    then
                        concat(
                            p.id_liquidacao,
                            ' ',
                            p.orgao,
                            ' ',
                            p.id_municipio,
                            ' ',
                            (right(p.ano, 2))
                        )
                    when p.id_liquidacao = '-1'
                    then
                        concat(
                            ' ', r.orgao, ' ', r.id_municipio, ' ', (right(p.ano, 2))
                        )
                end as string
            ) as id_liquidacao_bd,
            safe_cast(
                case
                    when p.id_empenho = '-1' then replace (p.id_liquidacao, '-1', '')
                end as string
            ) as id_liquidacao,
            safe_cast(p.numero_liquidacao as string) as numero_liquidacao,
            safe_cast(
                concat(
                    id_pagamento,
                    ' ',
                    p.orgao,
                    ' ',
                    p.id_municipio,
                    ' ',
                    (right(p.ano, 2))
                ) as string
            ) as id_pagamento_bd,
            safe_cast(id_pagamento as string) as id_pagamento,
            safe_cast(p.numero_pagamento as string) as numero,
            safe_cast(nome_credor as string) as nome_credor,
            safe_cast(
                replace(replace (documento_credor, '.', ''), '-', '') as string
            ) as documento_credor,
            safe_cast(
                case when p.id_rsp != '-1' then 1 else 0 end as bool
            ) as indicador_restos_pagar,
            safe_cast(left(fonte, 3) as string) as fonte,
            round(safe_cast(valor_pagamento_original as float64), 2) as valor_inicial,
            round(ifnull(safe_cast(vlr_anu_fonte as float64), 0), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(valor_pagamento_original as float64)
                - ifnull(safe_cast(vlr_anu_fonte as float64), 0),
                2
            ) as valor_final,
            round(
                safe_cast(valor_pagamento_original as float64)
                - ifnull(safe_cast(vlr_anu_fonte as float64), 0)
                - ifnull(safe_cast(vlr_ret_fonte as float64), 0),
                2
            ) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_mg") }} as p
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_rsp_mg") }} as r
            on p.id_rsp = r.id_rsp

    )
select *
from pagamento_mg
