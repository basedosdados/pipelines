{{ config(alias="evolucao_mensal_upp", schema="br_rj_isp_estatisticas_seguranca") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_upp as string) id_upp,
    safe_cast(nome as string) nome,
    safe_cast(quantidade_homicidio_doloso as int64) quantidade_homicidio_doloso,
    safe_cast(quantidade_latrocinio as int64) quantidade_latrocinio,
    safe_cast(quantidade_lesao_corporal_morte as int64) quantidade_lesao_corporal_morte,
    safe_cast(
        quantidade_homicidio_intervencao_policial as int64
    ) quantidade_homicidio_intervencao_policial,
    safe_cast(quantidade_tentativa_homicidio as int64) quantidade_tentativa_homicidio,
    safe_cast(
        quantidade_lesao_corporal_dolosa as int64
    ) quantidade_lesao_corporal_dolosa,
    safe_cast(quantidade_estupro as int64) quantidade_estupro,
    safe_cast(quantidade_homicidio_culposo as int64) quantidade_homicidio_culposo,
    safe_cast(
        quantidade_lesao_corporal_culposa as int64
    ) quantidade_lesao_corporal_culposa,
    safe_cast(quantidade_roubo_transeunte as int64) quantidade_roubo_transeunte,
    safe_cast(
        quantidade_roubo_corporal_coletivo as int64
    ) quantidade_roubo_corporal_coletivo,
    safe_cast(quantidade_roubo_veiculo as int64) quantidade_roubo_veiculo,
    safe_cast(quantidade_roubo_carga as int64) quantidade_roubo_carga,
    safe_cast(quantidade_roubo_comercio as int64) quantidade_roubo_comercio,
    safe_cast(quantidade_roubo_residencia as int64) quantidade_roubo_residencia,
    safe_cast(quantidade_roubo_banco as int64) quantidade_roubo_banco,
    safe_cast(
        quantidade_roubo_caixa_eletronico as int64
    ) quantidade_roubo_caixa_eletronico,
    safe_cast(quantidade_roubo_conducao_saque as int64) quantidade_roubo_conducao_saque,
    safe_cast(quantidade_total_roubos as int64) quantidade_total_roubos,
    safe_cast(quantidade_furto_veiculos as int64) quantidade_furto_veiculos,
    safe_cast(quantidade_total_furto as int64) quantidade_total_furto,
    safe_cast(quantidade_sequestro as int64) quantidade_sequestro,
    safe_cast(quantidade_extorsao as int64) quantidade_extorsao,
    safe_cast(quantidade_sequestro_relampago as int64) quantidade_sequestro_relampago,
    safe_cast(quantidade_estelionato as int64) quantidade_estelionato,
    safe_cast(quantidade_apreensao_droga as int64) quantidade_apreensao_droga,
    safe_cast(
        quantidade_registro_veiculo_recuperado as int64
    ) quantidade_registro_veiculo_recuperado,
    safe_cast(quantidade_ameaca as int64) quantidade_ameaca,
    safe_cast(quantidade_pessoa_desaparecida as int64) quantidade_pessoa_desaparecida,
    safe_cast(quantidade_encontro_cadaver as int64) quantidade_encontro_cadaver,
    safe_cast(quantidade_encontro_ossada as int64) quantidade_encontro_ossada,
    safe_cast(
        quantidade_policial_militar_morto_servico as int64
    ) quantidade_policial_militar_morto_servico,
    safe_cast(
        quantidade_policial_civil_morto_servico as int64
    ) quantidade_policial_civil_morto_servico,
    safe_cast(quantidade_registro_ocorrencia as int64) quantidade_registro_ocorrencia
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.evolucao_mensal_upp"
        )
    }} t
