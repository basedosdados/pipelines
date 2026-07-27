{{ config(alias="recursos_humanos", schema="br_ibge_munic", materialized="table") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(adm_direta as int64) adm_direta,
    safe_cast(adm_direta_sem_instrucao as int64) adm_direta_sem_instrucao,
    safe_cast(adm_direta_ensino_fundamental as int64) adm_direta_ensino_fundamental,
    safe_cast(adm_direta_ensino_medio as int64) adm_direta_ensino_medio,
    safe_cast(adm_direta_ensino_superior as int64) adm_direta_ensino_superior,
    safe_cast(adm_direta_pos_graduacao as int64) adm_direta_pos_graduacao,
    safe_cast(adm_direta_nivel_auxiliar as int64) adm_direta_nivel_auxiliar,
    safe_cast(estatutario_adm_direta as int64) estatutario_adm_direta,
    safe_cast(
        estatutario_adm_direta_sem_instrucao as int64
    ) estatutario_adm_direta_sem_instrucao,
    safe_cast(
        estatutario_adm_direta_ensino_fundamental as int64
    ) estatutario_adm_direta_ensino_fundamental,
    safe_cast(
        estatutario_adm_direta_ensino_medio as int64
    ) estatutario_adm_direta_ensino_medio,
    safe_cast(
        estatutario_adm_direta_ensino_superior as int64
    ) estatutario_adm_direta_ensino_superior,
    safe_cast(
        estatutario_adm_direta_pos_graduacao as int64
    ) estatutario_adm_direta_pos_graduacao,
    safe_cast(clt_adm_direta as int64) clt_adm_direta,
    safe_cast(clt_adm_direta_sem_instrucao as int64) clt_adm_direta_sem_instrucao,
    safe_cast(
        clt_adm_direta_ensino_fundamental as int64
    ) clt_adm_direta_ensino_fundamental,
    safe_cast(clt_adm_direta_ensino_medio as int64) clt_adm_direta_ensino_medio,
    safe_cast(clt_adm_direta_ensino_superior as int64) clt_adm_direta_ensino_superior,
    safe_cast(clt_adm_direta_pos_graduacao as int64) clt_adm_direta_pos_graduacao,
    safe_cast(comissionado_adm_direta as int64) comissionado_adm_direta,
    safe_cast(
        comissionado_adm_direta_sem_instrucao as int64
    ) comissionado_adm_direta_sem_instrucao,
    safe_cast(
        comissionado_adm_direta_ensino_fundamental as int64
    ) comissionado_adm_direta_ensino_fundamental,
    safe_cast(
        comissionado_adm_direta_ensino_medio as int64
    ) comissionado_adm_direta_ensino_medio,
    safe_cast(
        comissionado_adm_direta_ensino_superior as int64
    ) comissionado_adm_direta_ensino_superior,
    safe_cast(
        comissionado_adm_direta_pos_graduacao as int64
    ) comissionado_adm_direta_pos_graduacao,
    safe_cast(estagio_adm_direta as int64) estagio_adm_direta,
    safe_cast(
        estagio_adm_direta_ensino_fundamental as int64
    ) estagio_adm_direta_ensino_fundamental,
    safe_cast(estagio_adm_direta_ensino_medio as int64) estagio_adm_direta_ensino_medio,
    safe_cast(
        estagio_adm_direta_ensino_superior as int64
    ) estagio_adm_direta_ensino_superior,
    safe_cast(
        estagio_adm_direta_pos_graduacao as int64
    ) estagio_adm_direta_pos_graduacao,
    safe_cast(
        sem_vinculo_permanente_adm_direta as int64
    ) sem_vinculo_permanente_adm_direta,
    safe_cast(
        sem_vinculo_permanente_adm_direta_sem_instrucao as int64
    ) sem_vinculo_permanente_adm_direta_sem_instrucao,
    safe_cast(
        sem_vinculo_permanente_adm_direta_ensino_fundamental as int64
    ) sem_vinculo_permanente_adm_direta_ensino_fundamental,
    safe_cast(
        sem_vinculo_permanente_adm_direta_ensino_medio as int64
    ) sem_vinculo_permanente_adm_direta_ensino_medio,
    safe_cast(
        sem_vinculo_permanente_adm_direta_ensino_superior as int64
    ) sem_vinculo_permanente_adm_direta_ensino_superior,
    safe_cast(
        sem_vinculo_permanente_adm_direta_pos_graduacao as int64
    ) sem_vinculo_permanente_adm_direta_pos_graduacao,
    safe_cast(outro_vinculo_adm_direta as int64) outro_vinculo_adm_direta,
    safe_cast(
        existencia_unidade_chefiada_mulher_adm_direta as int64
    ) existencia_unidade_chefiada_mulher_adm_direta,
    safe_cast(
        existencia_registro_deficiencia_adm_direta as int64
    ) existencia_registro_deficiencia_adm_direta,
    safe_cast(registro_deficiencia_adm_direta as int64) registro_deficiencia_adm_direta,
    safe_cast(existencia_adm_indireta as int64) existencia_adm_indireta,
    safe_cast(adm_indireta as int64) adm_indireta,
    safe_cast(adm_indireta_sem_instrucao as int64) adm_indireta_sem_instrucao,
    safe_cast(adm_indireta_ensino_fundamental as int64) adm_indireta_ensino_fundamental,
    safe_cast(adm_indireta_ensino_medio as int64) adm_indireta_ensino_medio,
    safe_cast(adm_indireta_ensino_superior as int64) adm_indireta_ensino_superior,
    safe_cast(adm_indireta_pos_graduacao as int64) adm_indireta_pos_graduacao,
    safe_cast(adm_indireta_nivel_auxiliar as int64) adm_indireta_nivel_auxiliar,
    safe_cast(estatutario_adm_indireta as int64) estatutario_adm_indireta,
    safe_cast(
        estatutario_adm_indireta_sem_instrucao as int64
    ) estatutario_adm_indireta_sem_instrucao,
    safe_cast(
        estatutario_adm_indireta_ensino_fundamental as int64
    ) estatutario_adm_indireta_ensino_fundamental,
    safe_cast(
        estatutario_adm_indireta_ensino_medio as int64
    ) estatutario_adm_indireta_ensino_medio,
    safe_cast(
        estatutario_adm_indireta_ensino_superior as int64
    ) estatutario_adm_indireta_ensino_superior,
    safe_cast(
        estatutario_adm_indireta_pos_graduacao as int64
    ) estatutario_adm_indireta_pos_graduacao,
    safe_cast(clt_adm_indireta as int64) clt_adm_indireta,
    safe_cast(clt_adm_indireta_sem_instrucao as int64) clt_adm_indireta_sem_instrucao,
    safe_cast(
        clt_adm_indireta_ensino_fundamental as int64
    ) clt_adm_indireta_ensino_fundamental,
    safe_cast(clt_adm_indireta_ensino_medio as int64) clt_adm_indireta_ensino_medio,
    safe_cast(
        clt_adm_indireta_ensino_superior as int64
    ) clt_adm_indireta_ensino_superior,
    safe_cast(clt_adm_indireta_pos_graduacao as int64) clt_adm_indireta_pos_graduacao,
    safe_cast(comissionado_adm_indireta as int64) comissionado_adm_indireta,
    safe_cast(
        comissionado_adm_indireta_sem_instrucao as int64
    ) comissionado_adm_indireta_sem_instrucao,
    safe_cast(
        comissionado_adm_indireta_ensino_fundamental as int64
    ) comissionado_adm_indireta_ensino_fundamental,
    safe_cast(
        comissionado_adm_indireta_ensino_medio as int64
    ) comissionado_adm_indireta_ensino_medio,
    safe_cast(
        comissionado_adm_indireta_ensino_superior as int64
    ) comissionado_adm_indireta_ensino_superior,
    safe_cast(
        comissionado_adm_indireta_pos_graduacao as int64
    ) comissionado_adm_indireta_pos_graduacao,
    safe_cast(estagio_adm_indireta as int64) estagio_adm_indireta,
    safe_cast(
        estagio_adm_indireta_ensino_fundamental as int64
    ) estagio_adm_indireta_ensino_fundamental,
    safe_cast(
        estagio_adm_indireta_ensino_medio as int64
    ) estagio_adm_indireta_ensino_medio,
    safe_cast(
        estagio_adm_indireta_ensino_superior as int64
    ) estagio_adm_indireta_ensino_superior,
    safe_cast(
        estagio_adm_indireta_pos_graduacao as int64
    ) estagio_adm_indireta_pos_graduacao,
    safe_cast(
        sem_vinculo_permanente_adm_indireta as int64
    ) sem_vinculo_permanente_adm_indireta,
    safe_cast(
        sem_vinculo_permanente_adm_indireta_sem_instrucao as int64
    ) sem_vinculo_permanente_adm_indireta_sem_instrucao,
    safe_cast(
        sem_vinculo_permanente_adm_indireta_ensino_fundamental as int64
    ) sem_vinculo_permanente_adm_indireta_ensino_fundamental,
    safe_cast(
        sem_vinculo_permanente_adm_indireta_ensino_medio as int64
    ) sem_vinculo_permanente_adm_indireta_ensino_medio,
    safe_cast(
        sem_vinculo_permanente_adm_indireta_ensino_superior as int64
    ) sem_vinculo_permanente_adm_indireta_ensino_superior,
    safe_cast(
        sem_vinculo_permanente_adm_indireta_pos_graduacao as int64
    ) sem_vinculo_permanente_adm_indireta_pos_graduacao,
    safe_cast(outro_vinculo_adm_indireta as int64) outro_vinculo_adm_indireta,
    safe_cast(fundacao_adm_indireta as int64) fundacao_adm_indireta,
    safe_cast(autarquia_adm_indireta as int64) autarquia_adm_indireta,
    safe_cast(empresa_publica_adm_indireta as int64) empresa_publica_adm_indireta,
    safe_cast(
        sociedade_economia_mista_adm_indireta as int64
    ) sociedade_economia_mista_adm_indireta,
    safe_cast(existencia_contratacao_24_meses as int64) existencia_contratacao_24_meses,
    safe_cast(
        existencia_realizacao_concurso_24_meses as int64
    ) existencia_realizacao_concurso_24_meses,
    safe_cast(
        existencia_reserva_concurso_deficientes as int64
    ) existencia_reserva_concurso_deficientes,
    safe_cast(
        existencia_reserva_concurso_negros as int64
    ) existencia_reserva_concurso_negros,
    safe_cast(
        existencia_reserva_concurso_quilombolas as int64
    ) existencia_reserva_concurso_quilombolas,
    safe_cast(
        existencia_reserva_concurso_indigenas as int64
    ) existencia_reserva_concurso_indigenas,
    safe_cast(
        existencia_reserva_concurso_ciganos as int64
    ) existencia_reserva_concurso_ciganos,
    safe_cast(
        existencia_reserva_concurso_nao_sabia_informar as int64
    ) existencia_reserva_concurso_nao_sabia_informar,
    safe_cast(
        existencia_reserva_concurso_nao_houve as int64
    ) existencia_reserva_concurso_nao_houve,
    safe_cast(
        existencia_priorizacao_cargo_mulher as int64
    ) existencia_priorizacao_cargo_mulher,
    safe_cast(
        existencia_fundo_municipal_previdencia as int64
    ) existencia_fundo_municipal_previdencia,
    safe_cast(aposentado as int64) aposentado,
    safe_cast(pesionista as int64) pesionista
from {{ set_datalake_project("br_ibge_munic_staging.recursos_humanos") }} as t
