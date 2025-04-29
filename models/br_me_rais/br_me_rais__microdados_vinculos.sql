{{
    config(
        alias="microdados_vinculos",
        schema="br_me_rais",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2023, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(regexp_replace(id_municipio, r'\.0$', '') as string) id_municipio,
    safe_cast(tipo_vinculo as string) tipo_vinculo,
    safe_cast(vinculo_ativo_3112 as string) vinculo_ativo_3112,
    safe_cast(tipo_admissao as string) tipo_admissao,
    safe_cast(mes_admissao as int64) mes_admissao,
    safe_cast(mes_desligamento as int64) mes_desligamento,
    safe_cast(motivo_desligamento as string) motivo_desligamento,
    safe_cast(causa_desligamento_1 as string) causa_desligamento_1,
    safe_cast(causa_desligamento_2 as string) causa_desligamento_2,
    safe_cast(causa_desligamento_3 as string) causa_desligamento_3,
    safe_cast(faixa_tempo_emprego as string) faixa_tempo_emprego,
    safe_cast(faixa_horas_contratadas as string) faixa_horas_contratadas,
    round(safe_cast(tempo_emprego as float64), 2) tempo_emprego,
    safe_cast(quantidade_horas_contratadas as int64) quantidade_horas_contratadas,
    safe_cast(id_municipio_trabalho as string) id_municipio_trabalho,
    safe_cast(quantidade_dias_afastamento as int64) quantidade_dias_afastamento,
    safe_cast(indicador_cei_vinculado as string) indicador_cei_vinculado,
    safe_cast(indicador_trabalho_parcial as string) indicador_trabalho_parcial,
    safe_cast(
        indicador_trabalho_intermitente as string
    ) indicador_trabalho_intermitente,
    safe_cast(faixa_remuneracao_media_sm as string) faixa_remuneracao_media_sm,
    round(
        safe_cast(valor_remuneracao_media_sm as float64), 2
    ) valor_remuneracao_media_sm,
    safe_cast(valor_remuneracao_media as float64) valor_remuneracao_media,
    safe_cast(faixa_remuneracao_dezembro_sm as string) faixa_remuneracao_dezembro_sm,
    round(
        safe_cast(valor_remuneracao_dezembro_sm as float64), 2
    ) valor_remuneracao_dezembro_sm,
    safe_cast(valor_remuneracao_janeiro as float64) valor_remuneracao_janeiro,
    safe_cast(valor_remuneracao_fevereiro as float64) valor_remuneracao_fevereiro,
    safe_cast(valor_remuneracao_marco as float64) valor_remuneracao_marco,
    safe_cast(valor_remuneracao_abril as float64) valor_remuneracao_abril,
    safe_cast(valor_remuneracao_maio as float64) valor_remuneracao_maio,
    safe_cast(valor_remuneracao_junho as float64) valor_remuneracao_junho,
    safe_cast(valor_remuneracao_julho as float64) valor_remuneracao_julho,
    safe_cast(valor_remuneracao_agosto as float64) valor_remuneracao_agosto,
    safe_cast(valor_remuneracao_setembro as float64) valor_remuneracao_setembro,
    safe_cast(valor_remuneracao_outubro as float64) valor_remuneracao_outubro,
    safe_cast(valor_remuneracao_novembro as float64) valor_remuneracao_novembro,
    safe_cast(valor_remuneracao_dezembro as float64) valor_remuneracao_dezembro,
    cast(
        cast(regexp_replace(tipo_salario, r'^0+', '') as int64) as string
    ) as tipo_salario,
    safe_cast(valor_salario_contratual as float64) valor_salario_contratual,
    safe_cast(subatividade_ibge as string) subatividade_ibge,
    cast(
        cast(regexp_replace(subsetor_ibge, r'^0+', '') as int64) as string
    ) as subsetor_ibge,
    safe_cast(cbo_1994 as string) cbo_1994,
    safe_cast(cbo_2002 as string) cbo_2002,
    safe_cast(cnae_1 as string) cnae_1,
    safe_cast(cnae_2 as string) cnae_2,
    safe_cast(cnae_2_subclasse as string) cnae_2_subclasse,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(idade as int64) idade,
    safe_cast(grau_instrucao_1985_2005 as string) grau_instrucao_1985_2005,
    safe_cast(grau_instrucao_apos_2005 as string) grau_instrucao_apos_2005,
    safe_cast(nacionalidade as string) nacionalidade,
    safe_cast(sexo as string) sexo,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(indicador_portador_deficiencia as string) indicador_portador_deficiencia,
    safe_cast(tipo_deficiencia as string) tipo_deficiencia,
    safe_cast(ano_chegada_brasil as int64) ano_chegada_brasil,
    safe_cast(tamanho_estabelecimento as string) tamanho_estabelecimento,
    case
        when tipo_estabelecimento in ('Não', 'Não Indentificado')
        then 'Não identificado'
        when tipo_estabelecimento = 'CEI/CNO'
        then 'CEI'
        else safe_cast(regexp_replace(tipo_estabelecimento, r'^0+', '') as string)
    end as tipo_estabelecimento,
    safe_cast(natureza_juridica as string) natureza_juridica,
    safe_cast(indicador_simples as string) indicador_simples,
    trim(safe_cast(regexp_replace(bairros_sp, r'^0+', '') as string)) as bairros_sp,
    trim(safe_cast(regexp_replace(distritos_sp, r'^0+', '') as string)) as distritos_sp,
    trim(
        safe_cast(regexp_replace(bairros_fortaleza, r'^0+', '') as string)
    ) as bairros_fortaleza,
    trim(
        nullif(safe_cast(regexp_replace(bairros_rj, r'^0+', '') as string), '')
    ) as bairros_rj,
    trim(
        safe_cast(regexp_replace(regioes_administrativas_df, r'^0+', '') as string)
    ) as regioes_administrativas_df
from {{ set_datalake_project("br_me_rais_staging.microdados_vinculos") }}
{% if is_incremental() %} where safe_cast(ano as int64) > 2022 {% endif %}
