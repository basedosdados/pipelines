-- Paraná (PR) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pagamento_pr as (
        select
            safe_cast(nranopagamento as int64) as ano,
            (safe_cast(extract(month from date(dtoperacao)) as int64)) as mes,
            safe_cast(extract(date from timestamp(dtoperacao)) as date) as data,
            sigla_uf,
            id_municipio,
            safe_cast(cdorgao as string) as orgao,
            safe_cast(cdunidade as string) as id_unidade_gestora,
            safe_cast(
                concat(p.idempenho, ' ', m.id_municipio) as string
            ) as id_empenho_bd,
            safe_cast(p.idempenho as string) as id_empenho,
            safe_cast(nrempenho as string) as numero_empenho,
            safe_cast(
                concat(p.idliquidacao, ' ', m.id_municipio) as string
            ) as id_liquidacao_bd,
            safe_cast(p.idliquidacao as string) as id_liquidacao,
            safe_cast(null as string) as numero_liquidacao,
            safe_cast(
                concat(p.idpagamento, ' ', m.id_municipio) as string
            ) as id_pagamento_bd,
            safe_cast(idpagamento as string) as id_pagamento,
            safe_cast(nrpagamento as string) as numero,
            safe_cast(nmcredor as string) as nome_credor,
            safe_cast(
                regexp_replace(nrdoccredor, '[^0-9]', '') as string
            ) as documento_credor,
            safe_cast(null as bool) as indicador_restos_pagar,
            safe_cast(cdfontereceita as string) as fonte,
            round(safe_cast(vloperacao as float64), 2) as valor_inicial,
            round(safe_cast(nranoliquidacao as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(p.cdibge as float64), 2) as valor_final,
            round(safe_cast(0 as float64), 2) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_pr") }} p
        left join
            {{ set_datalake_project("world_wb_mides_staging.raw_empenho_pr") }} e
            on p.idempenho = e.idempenho
        left join
            basedosdados.br_bd_diretorios_brasil.municipio m
            on e.cdibge = id_municipio_6

    )
select *
from pagamento_pr
