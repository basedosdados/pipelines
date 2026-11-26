{{
    config(
        alias="fluxo_educacao_superior_uf",
        schema="br_inep_indicadores_educacionais",
        materialized="table",
        partition_by={
            "field": "ano_inicial",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2028, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano_inicial as int64) ano_inicial,
    safe_cast(ano_final as int64) ano_final,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(taxa_evasao_total as float64) taxa_evasao_total,
    safe_cast(taxa_evasao_feminino as float64) taxa_evasao_feminino,
    safe_cast(taxa_evasao_masculino as float64) taxa_evasao_masculino,
    safe_cast(taxa_evasao_ppi_sim as float64) taxa_evasao_ppi_sim,
    safe_cast(taxa_evasao_ppi_nao as float64) taxa_evasao_ppi_nao,
    safe_cast(taxa_evasao_ate_19_anos as float64) taxa_evasao_ate_19_anos,
    safe_cast(taxa_evasao_20_22_anos as float64) taxa_evasao_20_22_anos,
    safe_cast(taxa_evasao_23_24_anos as float64) taxa_evasao_23_24_anos,
    safe_cast(taxa_evasao_25_29_anos as float64) taxa_evasao_25_29_anos,
    safe_cast(taxa_evasao_30_39_anos as float64) taxa_evasao_30_39_anos,
    safe_cast(taxa_evasao_40_49_anos as float64) taxa_evasao_40_49_anos,
    safe_cast(taxa_evasao_50_anos_ou_mais as float64) taxa_evasao_50_anos_ou_mais,
    safe_cast(taxa_evasao_deficiencia_sim as float64) taxa_evasao_deficiencia_sim,
    safe_cast(taxa_evasao_deficiencia_nao as float64) taxa_evasao_deficiencia_nao,
    safe_cast(taxa_conclusao_total as float64) taxa_conclusao_total,
    safe_cast(taxa_conclusao_feminino as float64) taxa_conclusao_feminino,
    safe_cast(taxa_conclusao_masculino as float64) taxa_conclusao_masculino,
    safe_cast(taxa_conclusao_ppi_sim as float64) taxa_conclusao_ppi_sim,
    safe_cast(taxa_conclusao_ppi_nao as float64) taxa_conclusao_ppi_nao,
    safe_cast(taxa_conclusao_ate_19_anos as float64) taxa_conclusao_ate_19_anos,
    safe_cast(taxa_conclusao_20_22_anos as float64) taxa_conclusao_20_22_anos,
    safe_cast(taxa_conclusao_23_24_anos as float64) taxa_conclusao_23_24_anos,
    safe_cast(taxa_conclusao_25_29_anos as float64) taxa_conclusao_25_29_anos,
    safe_cast(taxa_conclusao_30_39_anos as float64) taxa_conclusao_30_39_anos,
    safe_cast(taxa_conclusao_40_49_anos as float64) taxa_conclusao_40_49_anos,
    safe_cast(taxa_conclusao_50_anos_ou_mais as float64) taxa_conclusao_50_anos_ou_mais,
    safe_cast(taxa_conclusao_deficiencia_sim as float64) taxa_conclusao_deficiencia_sim,
    safe_cast(taxa_conclusao_deficiencia_nao as float64) taxa_conclusao_deficiencia_nao,
    safe_cast(taxa_retencao_total as float64) taxa_retencao_total,
    safe_cast(taxa_retencao_feminino as float64) taxa_retencao_feminino,
    safe_cast(taxa_retencao_masculino as float64) taxa_retencao_masculino,
    safe_cast(taxa_retencao_ppi_sim as float64) taxa_retencao_ppi_sim,
    safe_cast(taxa_retencao_ppi_nao as float64) taxa_retencao_ppi_nao,
    safe_cast(taxa_retencao_ate_19_anos as float64) taxa_retencao_ate_19_anos,
    safe_cast(taxa_retencao_20_22_anos as float64) taxa_retencao_20_22_anos,
    safe_cast(taxa_retencao_23_24_anos as float64) taxa_retencao_23_24_anos,
    safe_cast(taxa_retencao_25_29_anos as float64) taxa_retencao_25_29_anos,
    safe_cast(taxa_retencao_30_39_anos as float64) taxa_retencao_30_39_anos,
    safe_cast(taxa_retencao_40_49_anos as float64) taxa_retencao_40_49_anos,
    safe_cast(taxa_retencao_50_anos_ou_mais as float64) taxa_retencao_50_anos_ou_mais,
    safe_cast(taxa_permanencia_total as float64) taxa_permanencia_total,
    safe_cast(taxa_permanencia_feminino as float64) taxa_permanencia_feminino,
    safe_cast(taxa_permanencia_masculino as float64) taxa_permanencia_masculino,
    safe_cast(taxa_permanencia_ppi_sim as float64) taxa_permanencia_ppi_sim,
    safe_cast(taxa_permanencia_ppi_nao as float64) taxa_permanencia_ppi_nao,
    safe_cast(taxa_permanencia_ate_19_anos as float64) taxa_permanencia_ate_19_anos,
    safe_cast(taxa_permanencia_20_22_anos as float64) taxa_permanencia_20_22_anos,
    safe_cast(taxa_permanencia_23_24_anos as float64) taxa_permanencia_23_24_anos,
    safe_cast(taxa_permanencia_25_29_anos as float64) taxa_permanencia_25_29_anos,
    safe_cast(taxa_permanencia_30_39_anos as float64) taxa_permanencia_30_39_anos,
    safe_cast(taxa_permanencia_40_49_anos as float64) taxa_permanencia_40_49_anos,
    safe_cast(
        taxa_permanencia_50_anos_ou_mais as float64
    ) taxa_permanencia_50_anos_ou_mais,
    safe_cast(
        taxa_permanencia_deficiencia_sim as float64
    ) taxa_permanencia_deficiencia_sim,
    safe_cast(
        taxa_permanencia_deficiencia_nao as float64
    ) taxa_permanencia_deficiencia_nao
from
    {{
        set_datalake_project(
            "br_inep_indicadores_educacionais_staging.fluxo_educacao_superior_uf"
        )
    }} as t
