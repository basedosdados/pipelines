-- Santa Catarina (SC) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pago_sc as (
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
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(null as string) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(null as string) as numero,
            safe_cast(nome_credor as string) as nome_credor,
            safe_cast(cpf_cnpj as string) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(right(especificacao_fonte_recurso, 2) as string) as fonte,
            round(safe_cast(0 as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_pagamento as float64), 2) as valor_final,
            round(safe_cast(valor_pagamento as float64), 2) as valor_liquido_recebido
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_sc") }}

    ),
    frequencia_sc as (
        select id_empenho_bd, count(id_empenho_bd) as frequencia_id
        from pago_sc
        group by 1
        order by 2 desc

    ),
    pagamento_sc as (
        select
            p.ano,
            p.mes,
            p.data,
            p.sigla_uf,
            p.id_municipio,
            p.orgao,
            p.id_unidade_gestora,
            (
                case
                    when frequencia_id > 1
                    then (safe_cast(null as string))
                    else p.id_empenho_bd
                end
            ) as id_empenho_bd,
            p.id_empenho,
            p.numero_empenho,
            p.id_liquidacao_bd,
            p.id_liquidacao,
            p.numero_liquidacao,
            p.id_pagamento_bd,
            p.id_pagamento,
            p.numero,
            p.nome_credor,
            p.documento_credor,
            p.indicador_restos_pagar,
            p.fonte,
            p.valor_inicial,
            p.valor_anulacao,
            p.valor_ajuste,
            p.valor_final,
            p.valor_liquido_recebido
        from pago_sc p
        left join frequencia_sc f on p.id_empenho_bd = f.id_empenho_bd

    )
select *
from pagamento_sc
