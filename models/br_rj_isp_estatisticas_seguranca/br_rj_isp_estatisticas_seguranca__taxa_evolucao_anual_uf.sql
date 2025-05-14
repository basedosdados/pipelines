{{ config(alias="taxa_evolucao_anual_uf", schema="br_rj_isp_estatisticas_seguranca") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(taxa_homicidio_doloso as int64) taxa_homicidio_doloso,
    safe_cast(taxa_latrocinio as int64) taxa_latrocinio,
    safe_cast(taxa_lesao_corporal_morte as int64) taxa_lesao_corporal_morte,
    safe_cast(
        taxa_crimes_violentos_letais_intencionais as int64
    ) taxa_crimes_violentos_letais_intencionais,
    safe_cast(
        taxa_homicidio_intervencao_policial as int64
    ) taxa_homicidio_intervencao_policial,
    safe_cast(taxa_letalidade_violenta as int64) taxa_letalidade_violenta,
    safe_cast(taxa_tentativa_homicidio as int64) taxa_tentativa_homicidio,
    safe_cast(taxa_lesao_corporal_dolosa as int64) taxa_lesao_corporal_dolosa,
    safe_cast(taxa_estupro as int64) taxa_estupro,
    safe_cast(taxa_homicidio_culposo as int64) taxa_homicidio_culposo,
    safe_cast(taxa_lesao_corporal_culposa as int64) taxa_lesao_corporal_culposa,
    safe_cast(taxa_roubo_transeunte as int64) taxa_roubo_transeunte,
    safe_cast(taxa_roubo_celular as int64) taxa_roubo_celular,
    safe_cast(taxa_roubo_corporal_coletivo as int64) taxa_roubo_corporal_coletivo,
    safe_cast(taxa_roubo_rua as int64) taxa_roubo_rua,
    safe_cast(taxa_roubo_veiculo as int64) taxa_roubo_veiculo,
    safe_cast(taxa_roubo_carga as int64) taxa_roubo_carga,
    safe_cast(taxa_roubo_comercio as int64) taxa_roubo_comercio,
    safe_cast(taxa_roubo_residencia as int64) taxa_roubo_residencia,
    safe_cast(taxa_roubo_banco as int64) taxa_roubo_banco,
    safe_cast(taxa_roubo_caixa_eletronico as int64) taxa_roubo_caixa_eletronico,
    safe_cast(taxa_roubo_conducao_saque as int64) taxa_roubo_conducao_saque,
    safe_cast(taxa_roubo_apos_saque as int64) taxa_roubo_apos_saque,
    safe_cast(taxa_roubo_bicicleta as int64) taxa_roubo_bicicleta,
    safe_cast(taxa_outros_roubos as int64) taxa_outros_roubos,
    safe_cast(taxa_total_roubos as int64) taxa_total_roubos,
    safe_cast(taxa_furto_veiculos as int64) taxa_furto_veiculos,
    safe_cast(taxa_furto_transeunte as int64) taxa_furto_transeunte,
    safe_cast(taxa_furto_coletivo as int64) taxa_furto_coletivo,
    safe_cast(taxa_furto_celular as int64) taxa_furto_celular,
    safe_cast(taxa_furto_bicicleta as int64) taxa_furto_bicicleta,
    safe_cast(taxa_outros_furtos as int64) taxa_outros_furtos,
    safe_cast(taxa_total_furtos as int64) taxa_total_furtos,
    safe_cast(taxa_sequestro as int64) taxa_sequestro,
    safe_cast(taxa_extorsao as int64) taxa_extorsao,
    safe_cast(taxa_sequestro_relampago as int64) taxa_sequestro_relampago,
    safe_cast(taxa_estelionato as int64) taxa_estelionato,
    safe_cast(taxa_apreensao_drogas as int64) taxa_apreensao_drogas,
    safe_cast(taxa_registro_posse_drogas as int64) taxa_registro_posse_drogas,
    safe_cast(taxa_registro_trafico_drogas as int64) taxa_registro_trafico_drogas,
    safe_cast(
        taxa_registro_apreensao_drogas_sem_autor as int64
    ) taxa_registro_apreensao_drogas_sem_autor,
    safe_cast(
        taxa_registro_veiculo_recuperado as int64
    ) taxa_registro_veiculo_recuperado,
    safe_cast(taxa_apf as int64) taxa_apf,
    safe_cast(taxa_aaapai as int64) taxa_aaapai,
    safe_cast(taxa_cmp as int64) taxa_cmp,
    safe_cast(taxa_cmba as int64) taxa_cmba,
    safe_cast(taxa_ameaca as int64) taxa_ameaca,
    safe_cast(taxa_pessoas_desaparecidas as int64) taxa_pessoas_desaparecidas,
    safe_cast(taxa_encontro_cadaver as int64) taxa_encontro_cadaver,
    safe_cast(taxa_encontro_ossada as int64) taxa_encontro_ossada,
    safe_cast(
        taxa_policial_militar_morto_servico as int64
    ) taxa_policial_militar_morto_servico,
    safe_cast(
        taxa_policial_civil_morto_servico as int64
    ) taxa_policial_civil_morto_servico,
    safe_cast(taxa_registro_ocorrencia as int64) taxa_registro_ocorrencia,
    safe_cast(tipo_fase as string) tipo_fase
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.taxa_evolucao_anual_uf"
        )
    }} t
