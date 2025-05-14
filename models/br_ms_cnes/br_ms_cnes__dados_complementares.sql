{{
    config(
        schema="br_ms_cnes",
        alias="dados_complementares",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
with
    raw_cnes_dados_complementares as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from {{ set_datalake_project("br_ms_cnes_staging.dados_complementares") }}
        where cnes is not null
    ),
    raw_cnes_dados_complementares_without_duplicates as (
        -- 2. distinct nas linhas
        select distinct * from raw_cnes_dados_complementares
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio e sigla_uf
        select *
        from raw_cnes_dados_complementares_without_duplicates
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_dados_complementares_without_duplicates.codufmun
            = mun.id_municipio_6
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    safe_cast(
        cns_adm as string
    ) cns_medico_responsavel_administrador_responsavel_tecnico,
    safe_cast(cns_oped as string) cns_medico_responsavel_oncologista_pediatrico,
    safe_cast(cns_conc as string) cns_medico_responsavel_cirurgia_oncologica,
    safe_cast(cns_oclin as string) cns_medico_responsavel_oncologista_clinico,
    safe_cast(cns_mrad as string) cns_medico_responsavel_radioterapeuta,
    safe_cast(cns_fnuc as string) cns_medico_responsavel_fisico_nuclear,
    safe_cast(cns_nefr as string) cns_medico_responsavel_nefrologista,
    safe_cast(cns_hmtr as string) cns_medico_responsavel_hemoterapeuta,
    safe_cast(cns_hmtl as string) cns_medico_responsavel_hematologista,
    safe_cast(cns_cres as string) cns_medico_capacitado_responsavel,
    safe_cast(cns_rtec as string) cns_responsavel_tecnico_sorologia,
    safe_cast(s_hbsagp as int64) quantidade_salas_hbsag_positivo,
    safe_cast(s_hbsagn as int64) quantidade_salas_hbsag_negativo,
    safe_cast(s_dpi as int64) quantidade_salas_dpi,
    safe_cast(s_dpac as int64) quantidade_salas_dpac,
    safe_cast(s_reagp as int64) quantidade_salas_reuso_hbsag_positivo,
    safe_cast(s_reagn as int64) quantidade_salas_reuso_hbsag_negativo,
    safe_cast(s_rehcv as int64) quantidade_salas_reuso_hcv_positivo,
    safe_cast(maq_prop as int64) quantidade_maquinas_proporcao,
    safe_cast(maq_outr as int64) quantidade_outras_maquinas,
    safe_cast(simul_rd as int64) quantidade_salas_simulacao_radioterapia,
    safe_cast(planj_rd as int64) quantidade_salas_planejamento_radioterapia,
    safe_cast(armaz_ft as int64) quantidade_salas_armazenamento_fontes_radioterapia,
    safe_cast(conf_mas as int64) quantidade_salas_confeccao_masc_radioterapia,
    safe_cast(sala_mol as int64) quantidade_salas_molde_radioterapia,
    safe_cast(blocoper as int64) quantidade_salas_bloco_personalizado_radioterapia,
    safe_cast(s_armaze as int64) quantidade_salas_armazenagem,
    safe_cast(s_prepar as int64) quantidade_salas_preparo,
    safe_cast(s_qcdura as int64) quantidade_salas_equipamentos_quimio_curta_duracao,
    safe_cast(s_qldura as int64) quantidade_salas_equipamentos_quimio_longa_duracao,
    safe_cast(s_cpflux as int64) quantidade_salas_equipamentos_capela_fluxo_laminar,
    safe_cast(s_simula as int64) quantidade_simuladores,
    safe_cast(s_acell6 as int64) quantidade_acelerador_linear_ate_6_mev,
    safe_cast(s_alseme as int64) quantidade_acelerador_linear_maior_6_mev_sem_eletrons,
    safe_cast(s_alcome as int64) quantidade_acelerador_linear_maior_6_mev_com_eletrons,
    safe_cast(ortv1050 as int64) quantidade_equipamentos_ortovoltagem_10_50_kv,
    safe_cast(orv50150 as int64) quantidade_equipamentos_ortovoltagem_50_150_kv,
    safe_cast(ov150500 as int64) quantidade_equipamentos_ortovoltagem_150_500_kv,
    safe_cast(un_cobal as int64) quantidade_unidade_cobalto,
    safe_cast(eqbrbaix as int64) quantidade_equipamentos_braquiterapia_baixa,
    safe_cast(eqbrmedi as int64) quantidade_equipamentos_braquiterapia_media,
    safe_cast(eqbralta as int64) quantidade_equipamentos_braquiterapia_alta,
    safe_cast(eq_marea as int64) quantidade_monitor_area,
    safe_cast(eq_mindi as int64) quantidade_monitor_individual,
    safe_cast(eqsispln as int64) quantidade_sistema_computacao_planejamento,
    safe_cast(eqdoscli as int64) quantidade_dosimetro_clinico,
    safe_cast(eqfonsel as int64) quantidade_fontes_seladas,
    safe_cast(s_recepc as int64) quantidade_salas_recepcao,
    safe_cast(s_trihmt as int64) quantidade_salas_triagem_hematologica,
    safe_cast(s_tricli as int64) quantidade_salas_triagem_clinica,
    safe_cast(s_coleta as int64) quantidade_salas_coleta,
    safe_cast(s_aferes as int64) quantidade_salas_aferese,
    safe_cast(s_preest as int64) quantidade_salas_pre_estoque,
    safe_cast(s_proces as int64) quantidade_salas_processamento,
    safe_cast(s_estoqu as int64) quantidade_salas_estoque,
    safe_cast(s_distri as int64) quantidade_salas_distribuicao,
    safe_cast(s_sorolo as int64) quantidade_salas_sorologia,
    safe_cast(s_imunoh as int64) quantidade_salas_imunohematologia,
    safe_cast(s_pretra as int64) quantidade_salas_pre_transfusionais,
    safe_cast(s_hemost as int64) quantidade_salas_hemostasia,
    safe_cast(s_contrq as int64) quantidade_salas_controle_qualidade,
    safe_cast(s_biomol as int64) quantidade_salas_biologia_molecular,
    safe_cast(s_imunfe as int64) quantidade_salas_imunofenotipagem,
    safe_cast(s_transf as int64) quantidade_salas_transfusao,
    safe_cast(s_sgdoad as int64) quantidade_salas_seguimento_doador,
    safe_cast(qt_cadre as int64) quantidade_cadeiras_reclinaveis,
    safe_cast(qt_cenre as int64) quantidade_centrifugas_refrigeradas,
    safe_cast(qt_refsa as int64) quantidade_refrigeradores_guarda_sangue,
    safe_cast(qt_conra as int64) quantidade_congeladores_rapidos,
    safe_cast(qt_extpl as int64) quantidade_extratores_automaticos_plasma,
    safe_cast(qt_fre18 as int64) quantidade_freezers_18_graus_celsius,
    safe_cast(qt_fre30 as int64) quantidade_freezers_30_graus_celsius,
    safe_cast(qt_agipl as int64) quantidade_agitadores_plaquetas,
    safe_cast(qt_selad as int64) quantidade_seladoras,
    safe_cast(qt_irrhe as int64) quantidade_irradiadores_hemocomponentes,
    safe_cast(qt_agltn as int64) quantidade_aglutinoscopio,
    safe_cast(qt_maqaf as int64) quantidade_maquinas_aferese,
    safe_cast(qt_refre as int64) quantidade_refrigeradores_reagentes,
    safe_cast(qt_refas as int64) quantidade_refrigeradores_amostras_sangue,
    safe_cast(qt_capfl as int64) quantidade_capelas_fluxo_laminar,
    safe_cast(hemotera as int64) indicador_existencia_requisito_hemoterapia,
    safe_cast(f_areia as int64) indicador_tratamento_agua_filtro_areia,
    safe_cast(f_carvao as int64) indicador_tratamento_agua_filtro_carvao,
    safe_cast(abrandad as int64) indicador_tratamento_agua_abrandador,
    safe_cast(deioniza as int64) indicador_tratamento_agua_deionizador,
    safe_cast(osmose_r as int64) indicador_tratamento_agua_maquina_osmose,
    safe_cast(out_trat as int64) indicador_tratamento_agua_outros_equipamentos,
    safe_cast(dialise as int64) indicador_existencia_requisito_dialise,
    safe_cast(quimradi as int64) indicador_existencia_requisito_quimio_radio
from cnes_add_muni as t

{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
