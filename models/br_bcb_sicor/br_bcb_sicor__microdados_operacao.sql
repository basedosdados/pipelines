{{
    config(
        alias="operacao",
        schema="br_bcb_sicor",
        materialized="incremental",
        partition_by={
            "field": "ano_emissao",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2026, "interval": 1},
        },
        cluster_by=["sigla_uf", "ano_safra_emissao"],
    )
}}

-- pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
with
    sicor as (
        select
            *,
            safe_cast(
                extract(year from parse_date("%d/%m/%Y", data_emissao)) as int64
            ) as _ano_emissao,
            safe_cast(
                extract(month from parse_date("%d/%m/%Y", data_emissao)) as int64
            ) as _mes_emissao,
            safe_cast(
                extract(year from parse_date("%d/%m/%Y", data_vencimento)) as int64
            ) as _ano_vencimento,
            safe_cast(
                extract(month from parse_date("%d/%m/%Y", data_vencimento)) as int64
            ) as _mes_vencimento,
        from {{ set_datalake_project("br_bcb_sicor_staging.operacao") }}
    )

select
    safe_cast(_ano_emissao as int64) as ano_emissao,
    safe_cast(_mes_emissao as int64) as mes_emissao,
    case
        when safe_cast(_mes_emissao as int64) in (1, 2, 3, 4, 5, 6)
        then
            concat(
                safe_cast(safe_cast(_ano_emissao - 1 as int64) as string),
                '/',
                safe_cast(safe_cast(_ano_emissao as int64) as string)
            )
        else
            concat(
                safe_cast(safe_cast(_ano_emissao as int64) as string),
                '/',
                safe_cast(safe_cast(_ano_emissao + 1 as int64) as string)
            )
    end as ano_safra_emissao,
    safe_cast(parse_date("%d/%m/%Y", data_emissao) as date) data_emissao,
    safe_cast(_ano_vencimento as int64) as ano_vencimento,
    safe_cast(_mes_vencimento as int64) as mes_vencimento,
    case
        when safe_cast(_mes_vencimento as int64) in (1, 2, 3, 4, 5, 6)
        then
            concat(
                safe_cast(safe_cast(_ano_vencimento - 1 as int64) as string),
                '/',
                safe_cast(safe_cast(_ano_vencimento as int64) as string)
            )
        else
            concat(
                safe_cast(safe_cast(_ano_vencimento as int64) as string),
                '/',
                safe_cast(safe_cast(_ano_vencimento + 1 as int64) as string)
            )
    end as ano_safra_vencimento,
    safe_cast(parse_date("%d/%m/%Y", data_vencimento) as date) data_vencimento,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_referencia_bacen as string) id_referencia_bacen,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(ltrim(id_categoria_emitente, '0') as string) id_categoria_emitente,
    safe_cast(id_empreendimento as string) id_empreendimento,
    safe_cast(ltrim(id_fonte_recurso, '0') as string) id_fonte_recurso,
    safe_cast(id_instrumento_credito as string) id_instrumento_credito,
    safe_cast(ltrim(id_programa, '0') as string) id_programa,
    safe_cast(ltrim(id_subprograma, '0') as string) id_subprograma,
    safe_cast(
        id_referencia_bacen_investimento as string
    ) id_referencia_bacen_investimento,
    safe_cast(id_tipo_agricultura as string) id_tipo_agricultura,
    safe_cast(ltrim(id_tipo_cultivo, '0') as string) id_tipo_cultivo,
    safe_cast(ltrim(id_tipo_solo, '0') as string) id_tipo_solo,
    safe_cast(ltrim(id_tipo_ciclo_cultivar, '0') as string) id_tipo_ciclo_cultivar,
    safe_cast(
        ltrim(id_tipo_encargo_financeiro, '0') as string
    ) id_tipo_encargo_financeiro,
    safe_cast(ltrim(id_tipo_grao_semente, '0') as string) id_tipo_grao_semente,
    safe_cast(
        ltrim(id_tipo_integracao_consorcio, '0') as string
    ) id_tipo_integracao_consorcio,
    safe_cast(ltrim(id_tipo_irrigacao, '0') as string) id_tipo_irrigacao,
    safe_cast(ltrim(id_fase_ciclo_producao, '0') as string) id_fase_ciclo_producao,
    safe_cast(ltrim(id_tipo_seguro, '0') as string) id_tipo_seguro,
    safe_cast(
        id_contrato_sistema_tesouro_nacional as string
    ) id_contrato_sistema_tesouro_nacional,
    safe_cast(cnpj_agente_investimento as string) cnpj_basico_agente_investimento,
    safe_cast(
        cnpj_basico_instituicao_financeira as string
    ) cnpj_basico_instituicao_financeira,
    safe_cast(cnpj_cadastrante as string) cnpj_basico_cadastrante,
    -- converte datas preenchidas erradas para nulos. Existem cerca de 35 casos nas
    -- colunas abaixo com anos que não fazem sentido eg. 5026; 8218
    case
        when extract(year from parse_date("%d/%m/%Y", data_fim_colheita)) > 2100
        then null
        else safe_cast(parse_date("%d/%m/%Y", data_fim_colheita) as date)
    end as data_fim_colheita,
    case
        when extract(year from parse_date("%d/%m/%Y", data_fim_plantio)) > 2100
        then null
        else safe_cast(parse_date("%d/%m/%Y", data_fim_plantio) as date)
    end as data_fim_plantio,
    case
        when extract(year from parse_date("%d/%m/%Y", data_inicio_colheita)) > 2100
        then null
        else safe_cast(parse_date("%d/%m/%Y", data_inicio_colheita) as date)
    end as data_inicio_colheita,
    case
        when extract(year from parse_date("%d/%m/%Y", data_inicio_plantio)) > 2100
        then null
        else safe_cast(parse_date("%d/%m/%Y", data_inicio_plantio) as date)
    end as data_inicio_plantio,
    safe_cast(area_financiada as float64) area_financiada,
    safe_cast(area_informada as float64) area_informada,
    safe_cast(valor_aliquota_proagro as float64) valor_aliquota_proagro,
    safe_cast(valor_parcela_credito as float64) valor_parcela_credito,
    safe_cast(valor_prestacao_investimento as float64) valor_prestacao_investimento,
    safe_cast(valor_recurso_proprio as float64) valor_recurso_proprio,
    safe_cast(valor_receita_bruta_esperada as float64) valor_receita_bruta_esperada,
    safe_cast(valor_recurso_proprio_srv as float64) valor_recurso_proprio_srv,
    safe_cast(
        valor_quantidade_itens_financiados as float64
    ) valor_quantidade_itens_financiados,
    safe_cast(valor_produtividade_obtida as float64) valor_produtividade_obtida,
    safe_cast(valor_previsao_producao as float64) valor_previsao_producao,
    safe_cast(taxa_juro as float64) taxa_juro,
    safe_cast(percentual_bonus_car as float64) percentual_bonus_car,
    safe_cast(
        taxa_juro_encargo_financeiro_posfixado as float64
    ) taxa_juro_encargo_financeiro_posfixado,
    safe_cast(
        valor_percentual_custo_efetivo_total as float64
    ) valor_percentual_custo_efetivo_total,
    safe_cast(
        valor_percentual_risco_fundo_constitucional as float64
    ) valor_percentual_risco_fundo_constitucional,
    safe_cast(valor_percentual_risco_stn as float64) valor_percentual_risco_stn
from sicor
{% if is_incremental() %}
    where
        date(cast(_ano_emissao as int64), cast(_mes_emissao as int64), 1) > (
            select max(date(cast(ano_emissao as int64), cast(mes_emissao as int64), 1))
            from {{ this }}
        )
{% endif %}
