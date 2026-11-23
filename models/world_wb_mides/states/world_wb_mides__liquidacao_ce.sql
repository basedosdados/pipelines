-- Ceará (CE) contribution to world_wb_mides.liquidacao.
-- Split out of the monolithic liquidacao model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    liquidacao_ce as (
        select
            (safe_cast(extract(year from date(data_liquidacao)) as int64)) as ano,
            (safe_cast(extract(month from date(data_liquidacao)) as int64)) as mes,
            safe_cast(extract(date from timestamp(data_liquidacao)) as date) as data,
            'CE' as sigla_uf,
            safe_cast(geoibgeid as string) as id_municipio,
            safe_cast(codigo_orgao as string) as orgao,
            safe_cast(codigo_unidade as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    numero_empenho,
                    ' ',
                    trim(codigo_orgao),
                    ' ',
                    trim(codigo_unidade),
                    ' ',
                    geoibgeid,
                    ' ',
                    (substring(data_emissao_empenho, 6, 2)),
                    ' ',
                    (substring(data_emissao_empenho, 3, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(numero_empenho as string) as numero_empenho,
            safe_cast(null as string) as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(null as string) as numero,
            safe_cast(nome_responsavel_liquidacao as string) as nome_responsavel,
            safe_cast(cpf_responsavel_liquidacao_ as string) as documento_responsavel,
            safe_cast(null as bool) as indicador_restos_pagar,
            round(safe_cast(valor_liquidado as float64), 2) as valor_inicial,
            round(safe_cast(0 as float64), 2) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(safe_cast(valor_liquidado as float64), 2) as valor_final,
        from {{ set_datalake_project("world_wb_mides_staging.raw_liquidacao_ce") }} l
        left join
            {{ set_datalake_project("world_wb_mides_staging.aux_municipio_ce") }} m
            on l.codigo_municipio = m.codigo_municipio

    )
select *
from liquidacao_ce
