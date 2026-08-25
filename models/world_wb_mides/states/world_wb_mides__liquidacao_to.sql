-- Tocantins (TO) contribution to world_wb_mides.liquidacao.
--
-- Source: world_wb_mides_staging.raw_liquidacao_to, 2013-2022. Same movement
-- ledger shape as the TO empenho source: one row per movement, signed by
-- `sinal`, aggregated here to the line key
-- (municipio, orgao, nr_liquidacao, nr_empenho, rubrica).
--
-- nome_responsavel / documento_responsavel are null on purpose. TCE-TO does
-- publish `credor` / `nome_credor` on this table, but that is the supplier
-- being paid, not the official who certified the liquidacao; putting a creditor
-- in a responsavel column would misstate what it means. The creditor is carried
-- on the empenho and on pagamento instead.
--
-- indicador_restos_pagar is null for the reason given in the empenho model:
-- nr_empenho always carries the same year as `exercicio`, so an empenho
-- inherited from a previous year cannot be identified in this source.
with
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
            safe_cast(nr_liquidacao as string) as numero,
            safe_cast(trim(rubrica) as string) as rubrica,
            trim(sinal) as sinal,
            safe_cast(valor as float64) as valor
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_to") }}
    ),
    linha_to as (
        select
            ano,
            id_municipio,
            orgao,
            numero,
            numero_empenho,
            rubrica,
            min(data) as data,
            any_value(id_unidade_gestora) as id_unidade_gestora,
            round(sum(if(sinal = '+', valor, 0)), 2) as valor_inicial,
            round(sum(if(sinal = '-', valor, 0)), 2) as valor_anulacao
        from movimento_to
        group by ano, id_municipio, orgao, numero, numero_empenho, rubrica
    ),
    liquidado_to as (
        select
            ano,
            extract(month from data) as mes,
            data,
            'TO' as sigla_uf,
            id_municipio,
            orgao,
            id_unidade_gestora,
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    right(cast(ano as string), 2)
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            numero_empenho,
            safe_cast(
                concat(
                    numero,
                    ' ',
                    orgao,
                    ' ',
                    id_municipio,
                    ' ',
                    right(cast(ano as string), 2)
                ) as string
            ) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            numero,
            safe_cast(null as string) as nome_responsavel,
            safe_cast(null as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            valor_inicial,
            valor_anulacao
        from linha_to
    ),
    frequencia_to as (
        select id_liquidacao_bd, count(id_liquidacao_bd) as frequencia_id
        from liquidado_to
        group by 1
    ),
    liquidacao_to as (
        select
            l.ano,
            l.mes,
            l.data,
            l.sigla_uf,
            l.id_municipio,
            l.orgao,
            l.id_unidade_gestora,
            l.id_empenho_bd,
            l.id_empenho,
            l.numero_empenho,
            (
                case
                    when f.frequencia_id > 1
                    then safe_cast(null as string)
                    else l.id_liquidacao_bd
                end
            ) as id_liquidacao_bd,
            l.id_liquidacao,
            l.numero,
            l.nome_responsavel,
            l.documento_responsavel,
            l.indicador_restos_pagar,
            round(l.valor_inicial, 2) as valor_inicial,
            round(l.valor_anulacao, 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(l.valor_inicial - l.valor_anulacao, 2) as valor_final
        from liquidado_to l
        left join frequencia_to f on l.id_liquidacao_bd = f.id_liquidacao_bd
    )
select *
from liquidacao_to
