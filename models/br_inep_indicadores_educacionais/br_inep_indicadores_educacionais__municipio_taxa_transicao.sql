{{
    config(
        alias="municipio_taxa_transicao",
        materialized="table",
        schema="br_inep_indicadores_educacionais",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2021, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(localizacao as string) localizacao,
    safe_cast(rede as string) rede,
    safe_cast(taxa_evasao_ef as float64) taxa_evasao_ef,
    safe_cast(taxa_evasao_ef_1_ano as float64) taxa_evasao_ef_1_ano,
    safe_cast(taxa_evasao_ef_2_ano as float64) taxa_evasao_ef_2_ano,
    safe_cast(taxa_evasao_ef_3_ano as float64) taxa_evasao_ef_3_ano,
    safe_cast(taxa_evasao_ef_4_ano as float64) taxa_evasao_ef_4_ano,
    safe_cast(taxa_evasao_ef_5_ano as float64) taxa_evasao_ef_5_ano,
    safe_cast(taxa_evasao_ef_6_ano as float64) taxa_evasao_ef_6_ano,
    safe_cast(taxa_evasao_ef_7_ano as float64) taxa_evasao_ef_7_ano,
    safe_cast(taxa_evasao_ef_8_ano as float64) taxa_evasao_ef_8_ano,
    safe_cast(taxa_evasao_ef_9_ano as float64) taxa_evasao_ef_9_ano,
    safe_cast(taxa_evasao_ef_anos_finais as float64) taxa_evasao_ef_anos_finais,
    safe_cast(taxa_evasao_ef_anos_iniciais as float64) taxa_evasao_ef_anos_iniciais,
    safe_cast(taxa_evasao_em as float64) taxa_evasao_em,
    safe_cast(taxa_evasao_em_1_ano as float64) taxa_evasao_em_1_ano,
    safe_cast(taxa_evasao_em_2_ano as float64) taxa_evasao_em_2_ano,
    safe_cast(taxa_evasao_em_3_ano as float64) taxa_evasao_em_3_ano,
    safe_cast(taxa_migracao_eja_ef as float64) taxa_migracao_eja_ef,
    safe_cast(taxa_migracao_eja_ef_1_ano as float64) taxa_migracao_eja_ef_1_ano,
    safe_cast(taxa_migracao_eja_ef_2_ano as float64) taxa_migracao_eja_ef_2_ano,
    safe_cast(taxa_migracao_eja_ef_3_ano as float64) taxa_migracao_eja_ef_3_ano,
    safe_cast(taxa_migracao_eja_ef_4_ano as float64) taxa_migracao_eja_ef_4_ano,
    safe_cast(taxa_migracao_eja_ef_5_ano as float64) taxa_migracao_eja_ef_5_ano,
    safe_cast(taxa_migracao_eja_ef_6_ano as float64) taxa_migracao_eja_ef_6_ano,
    safe_cast(taxa_migracao_eja_ef_7_ano as float64) taxa_migracao_eja_ef_7_ano,
    safe_cast(taxa_migracao_eja_ef_8_ano as float64) taxa_migracao_eja_ef_8_ano,
    safe_cast(taxa_migracao_eja_ef_9_ano as float64) taxa_migracao_eja_ef_9_ano,
    safe_cast(
        taxa_migracao_eja_ef_anos_finais as float64
    ) taxa_migracao_eja_ef_anos_finais,
    safe_cast(
        taxa_migracao_eja_ef_anos_iniciais as float64
    ) taxa_migracao_eja_ef_anos_iniciais,
    safe_cast(taxa_migracao_eja_em as float64) taxa_migracao_eja_em,
    safe_cast(taxa_migracao_eja_em_1_ano as float64) taxa_migracao_eja_em_1_ano,
    safe_cast(taxa_migracao_eja_em_2_ano as float64) taxa_migracao_eja_em_2_ano,
    safe_cast(taxa_migracao_eja_em_3_ano as float64) taxa_migracao_eja_em_3_ano,
    safe_cast(taxa_promocao_ef as float64) taxa_promocao_ef,
    safe_cast(taxa_promocao_ef_1_ano as float64) taxa_promocao_ef_1_ano,
    safe_cast(taxa_promocao_ef_2_ano as float64) taxa_promocao_ef_2_ano,
    safe_cast(taxa_promocao_ef_3_ano as float64) taxa_promocao_ef_3_ano,
    safe_cast(taxa_promocao_ef_4_ano as float64) taxa_promocao_ef_4_ano,
    safe_cast(taxa_promocao_ef_5_ano as float64) taxa_promocao_ef_5_ano,
    safe_cast(taxa_promocao_ef_6_ano as float64) taxa_promocao_ef_6_ano,
    safe_cast(taxa_promocao_ef_7_ano as float64) taxa_promocao_ef_7_ano,
    safe_cast(taxa_promocao_ef_8_ano as float64) taxa_promocao_ef_8_ano,
    safe_cast(taxa_promocao_ef_9_ano as float64) taxa_promocao_ef_9_ano,
    safe_cast(taxa_promocao_ef_anos_finais as float64) taxa_promocao_ef_anos_finais,
    safe_cast(taxa_promocao_ef_anos_iniciais as float64) taxa_promocao_ef_anos_iniciais,
    safe_cast(taxa_promocao_em as float64) taxa_promocao_em,
    safe_cast(taxa_promocao_em_1_ano as float64) taxa_promocao_em_1_ano,
    safe_cast(taxa_promocao_em_2_ano as float64) taxa_promocao_em_2_ano,
    safe_cast(taxa_promocao_em_3_ano as float64) taxa_promocao_em_3_ano,
    safe_cast(taxa_repetencia_ef as float64) taxa_repetencia_ef,
    safe_cast(taxa_repetencia_ef_1_ano as float64) taxa_repetencia_ef_1_ano,
    safe_cast(taxa_repetencia_ef_2_ano as float64) taxa_repetencia_ef_2_ano,
    safe_cast(taxa_repetencia_ef_3_ano as float64) taxa_repetencia_ef_3_ano,
    safe_cast(taxa_repetencia_ef_4_ano as float64) taxa_repetencia_ef_4_ano,
    safe_cast(taxa_repetencia_ef_5_ano as float64) taxa_repetencia_ef_5_ano,
    safe_cast(taxa_repetencia_ef_6_ano as float64) taxa_repetencia_ef_6_ano,
    safe_cast(taxa_repetencia_ef_7_ano as float64) taxa_repetencia_ef_7_ano,
    safe_cast(taxa_repetencia_ef_8_ano as float64) taxa_repetencia_ef_8_ano,
    safe_cast(taxa_repetencia_ef_9_ano as float64) taxa_repetencia_ef_9_ano,
    safe_cast(taxa_repetencia_ef_anos_finais as float64) taxa_repetencia_ef_anos_finais,
    safe_cast(
        taxa_repetencia_ef_anos_iniciais as float64
    ) taxa_repetencia_ef_anos_iniciais,
    safe_cast(taxa_repetencia_em as float64) taxa_repetencia_em,
    safe_cast(taxa_repetencia_em_1_ano as float64) taxa_repetencia_em_1_ano,
    safe_cast(taxa_repetencia_em_2_ano as float64) taxa_repetencia_em_2_ano,
    safe_cast(taxa_repetencia_em_3_ano as float64) taxa_repetencia_em_3_ano,
from
    {{
        set_datalake_project(
            "br_inep_indicadores_educacionais_staging.municipio_taxa_transicao"
        )
    }} as t
