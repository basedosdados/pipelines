{{
    config(
        schema="br_mj_sisdepen",
        alias="cobertura",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2030, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(semestre as int64) semestre,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(ciclo as string) ciclo,
    safe_cast(geracao_esquema as string) geracao_esquema,
    safe_cast(unidades_esperadas as int64) unidades_esperadas,
    safe_cast(unidades_reportando as int64) unidades_reportando,
    safe_cast(unidades_ausentes as int64) unidades_ausentes,
    safe_cast(taxa_presenca as float64) taxa_presenca,
    safe_cast(taxa_faixa_etaria_completa as float64) taxa_faixa_etaria_completa,
    safe_cast(taxa_faixa_etaria_parcial as float64) taxa_faixa_etaria_parcial,
    safe_cast(taxa_faixa_etaria_ausente as float64) taxa_faixa_etaria_ausente,
    safe_cast(taxa_raca_cor_completa as float64) taxa_raca_cor_completa,
    safe_cast(taxa_raca_cor_parcial as float64) taxa_raca_cor_parcial,
    safe_cast(taxa_raca_cor_ausente as float64) taxa_raca_cor_ausente,
    safe_cast(taxa_escolaridade_completa as float64) taxa_escolaridade_completa,
    safe_cast(taxa_escolaridade_parcial as float64) taxa_escolaridade_parcial,
    safe_cast(taxa_escolaridade_ausente as float64) taxa_escolaridade_ausente,
    safe_cast(taxa_capacidade_informada as float64) taxa_capacidade_informada,
    safe_cast(populacao_total as int64) populacao_total,
    safe_cast(capacidade_total as int64) capacidade_total
from {{ set_datalake_project("br_mj_sisdepen_staging.cobertura") }} as t
