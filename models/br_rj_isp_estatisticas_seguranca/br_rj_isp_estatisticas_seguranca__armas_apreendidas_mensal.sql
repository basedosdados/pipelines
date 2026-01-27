{{
    config(
        alias="armas_apreendidas_mensal",
        schema="br_rj_isp_estatisticas_seguranca",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_cisp as string) id_cisp,
    safe_cast(id_aisp as string) id_aisp,
    safe_cast(id_risp as string) id_risp,
    safe_cast(
        quantidade_arma_fabricacao_caseira as int64
    ) quantidade_arma_fabricacao_caseira,
    safe_cast(quantidade_carabina as int64) quantidade_carabina,
    safe_cast(quantidade_espingarda as int64) quantidade_espingarda,
    safe_cast(quantidade_fuzil as int64) quantidade_fuzil,
    safe_cast(quantidade_garrucha as int64) quantidade_garrucha,
    safe_cast(quantidade_garruchao as int64) quantidade_garruchao,
    safe_cast(quantidade_metralhadora as int64) quantidade_metralhadora,
    safe_cast(quantidade_outros as int64) quantidade_outros,
    safe_cast(quantidade_pistola as int64) quantidade_pistola,
    safe_cast(quantidade_revolver as int64) quantidade_revolver,
    safe_cast(quantidade_submetralhadora as int64) quantidade_submetralhadora,
    safe_cast(
        quantidade_artefato_explosivo_armadilha_explosiva as int64
    ) quantidade_artefato_explosivo_armadilha_explosiva,
    safe_cast(
        quantidade_artefato_explosivo_armadilha_incendiaria as int64
    ) quantidade_artefato_explosivo_armadilha_incendiaria,
    safe_cast(
        quantidade_artefato_explosivo_bomba_fabricacao_caseira as int64
    ) quantidade_artefato_explosivo_bomba_fabricacao_caseira,
    safe_cast(
        quantidade_artefato_explosivo_granada as int64
    ) quantidade_artefato_explosivo_granada,
    safe_cast(
        quantidade_artefato_explosivo_material_belico_explosivo as int64
    ) quantidade_artefato_explosivo_material_belico_explosivo,
    safe_cast(
        quantidade_artefato_explosivo_material_explosivo as int64
    ) quantidade_artefato_explosivo_material_explosivo,
    safe_cast(
        quantidade_artefato_explosivo_material_explosivo_caseiro as int64
    ) quantidade_artefato_explosivo_material_explosivo_caseiro,
    safe_cast(
        quantidade_artefato_explosivo_material_nao_identificado as int64
    ) quantidade_artefato_explosivo_material_nao_identificado,
    safe_cast(quantidade_simulacro_airsoft as int64) quantidade_simulacro_airsoft,
    safe_cast(
        quantidade_simulacro_arma_fabricacao_caseira as int64
    ) quantidade_simulacro_arma_fabricacao_caseira,
    safe_cast(quantidade_simulacro_carabina as int64) quantidade_simulacro_carabina,
    safe_cast(quantidade_simulacro_espingarda as int64) quantidade_simulacro_espingarda,
    safe_cast(quantidade_simulacro_fuzil as int64) quantidade_simulacro_fuzil,
    safe_cast(quantidade_simulacro_garrucha as int64) quantidade_simulacro_garrucha,
    safe_cast(quantidade_simulacro_garruchao as int64) quantidade_simulacro_garruchao,
    safe_cast(
        quantidade_simulacro_metralhadora as int64
    ) quantidade_simulacro_metralhadora,
    safe_cast(quantidade_simulacro_outros as int64) quantidade_simulacro_outros,
    safe_cast(quantidade_simulacro_paintball as int64) quantidade_simulacro_paintball,
    safe_cast(quantidade_simulacro_pistola as int64) quantidade_simulacro_pistola,
    safe_cast(quantidade_simulacro_revolver as int64) quantidade_simulacro_revolver,
    safe_cast(
        quantidade_simulacro_submetralhadora as int64
    ) quantidade_simulacro_submetralhadora,
    safe_cast(
        quantidade_artefato_explosivo_total as int64
    ) quantidade_artefato_explosivo_total,
    safe_cast(quantidade_simulacro_total as int64) quantidade_simulacro_total,
    safe_cast(quantidade_arma_fogo_total as int64) quantidade_arma_fogo_total,
    safe_cast(quantidade_arma_branca_total as int64) quantidade_arma_branca_total,
    safe_cast(quantidade_municao_total as int64) quantidade_municao_total,
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.armas_apreendidas_mensal"
        )
    }}
where ano is not null and mes is not null
