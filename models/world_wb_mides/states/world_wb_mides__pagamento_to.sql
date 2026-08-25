-- Tocantins (TO) contribution to world_wb_mides.pagamento.
--
-- Source: world_wb_mides_staging.raw_pagamento_to, 2013-2022. Same movement
-- ledger shape as the other two TO sources: one row per movement, signed by
-- `sinal`, aggregated here to the line key
-- (municipio, orgao, nr_pagamento, nr_liquidacao, nr_empenho, rubrica).
--
-- TCE-TO's pagamento file carries no creditor, so nome_credor / documento_credor
-- are joined from the empenho on (municipio, nr_empenho) -- supplier identity is
-- one of the things MiDES exists to expose. The join is deliberately withheld
-- where an empenho names more than one creditor across its lines, so a payment
-- is never attributed to the wrong supplier.
--
-- indicador_restos_pagar is null for the reason given in the empenho model.
-- valor_liquido_recebido is null: TCE-TO publishes the gross payment only, with
-- no net-of-deductions figure.
with
    credor_to as (
        select
            safe_cast(municipio as string) as id_municipio,
            safe_cast(nr_empenho as string) as numero_empenho,
            case
                when count(distinct idcredor) = 1
                then any_value(safe_cast(nome_credor as string))
            end as nome_credor,
            case
                when count(distinct idcredor) = 1
                then any_value(safe_cast(idcredor as string))
            end as documento_credor
        from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_to") }}
        group by 1, 2
    ),
    movimento_to as (
        select
            safe_cast(exercicio as int64) as ano,
            parse_date('%d/%m/%Y', trim(data)) as data,
            safe_cast(municipio as string) as id_municipio,
            safe_cast(trim(orgao) as string) as orgao,
            safe_cast(
                trim(split(unidade_gestora, ' - ')[safe_offset(0)]) as string
            ) as id_unidade_gestora,
            safe_cast(nr_empenho as string) as numero_empenho,
            safe_cast(nr_liquidacao as string) as numero_liquidacao,
            safe_cast(nr_pagamento as string) as numero,
            safe_cast(trim(rubrica) as string) as rubrica,
            safe_cast(
                trim(split(rec_vinculado, ' - ')[safe_offset(0)]) as string
            ) as fonte,
            trim(sinal) as sinal,
            safe_cast(valor as float64) as valor
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_to") }}
    ),
    linha_to as (
        select
            ano,
            id_municipio,
            orgao,
            numero,
            numero_liquidacao,
            numero_empenho,
            rubrica,
            min(data) as data,
            any_value(id_unidade_gestora) as id_unidade_gestora,
            any_value(fonte) as fonte,
            round(sum(if(sinal = '+', valor, 0)), 2) as valor_inicial,
            round(sum(if(sinal = '-', valor, 0)), 2) as valor_anulacao
        from movimento_to
        group by
            ano, id_municipio, orgao, numero, numero_liquidacao, numero_empenho, rubrica
    ),
    pago_to as (
        select
            l.ano,
            extract(month from l.data) as mes,
            l.data,
            'TO' as sigla_uf,
            l.id_municipio,
            l.orgao,
            l.id_unidade_gestora,
            safe_cast(
                concat(
                    l.numero_empenho,
                    ' ',
                    l.orgao,
                    ' ',
                    l.id_municipio,
                    ' ',
                    right(cast(l.ano as string), 2)
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            l.numero_empenho,
            safe_cast(
                concat(
                    l.numero_liquidacao,
                    ' ',
                    l.orgao,
                    ' ',
                    l.id_municipio,
                    ' ',
                    right(cast(l.ano as string), 2)
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            l.numero_liquidacao,
            safe_cast(
                concat(
                    l.numero,
                    ' ',
                    l.orgao,
                    ' ',
                    l.id_municipio,
                    ' ',
                    right(cast(l.ano as string), 2)
                ) as string
            ) as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            l.numero,
            c.nome_credor,
            c.documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            l.fonte,
            l.valor_inicial,
            l.valor_anulacao
        from linha_to l
        left join
            credor_to c
            on l.id_municipio = c.id_municipio
            and l.numero_empenho = c.numero_empenho
    ),
    frequencia_to as (
        select id_pagamento_bd, count(id_pagamento_bd) as frequencia_id
        from pago_to
        group by 1
    ),
    pagamento_to as (
        select
            p.ano,
            p.mes,
            p.data,
            p.sigla_uf,
            p.id_municipio,
            p.orgao,
            p.id_unidade_gestora,
            p.id_empenho_bd,
            p.id_empenho,
            p.numero_empenho,
            p.id_liquidacao_bd,
            p.id_liquidacao,
            p.numero_liquidacao,
            (
                case
                    when f.frequencia_id > 1
                    then safe_cast(null as string)
                    else p.id_pagamento_bd
                end
            ) as id_pagamento_bd,
            p.id_pagamento,
            p.numero,
            p.nome_credor,
            p.documento_credor,
            p.indicador_restos_pagar,
            p.fonte,
            round(p.valor_inicial, 2) as valor_inicial,
            round(p.valor_anulacao, 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(p.valor_inicial - p.valor_anulacao, 2) as valor_final,
            safe_cast(null as float64) as valor_liquido_recebido
        from pago_to p
        left join frequencia_to f on p.id_pagamento_bd = f.id_pagamento_bd
    )
select *
from pagamento_to
