{{
    config(
        alias="taxa_evolucao_mensal_municipio",
        schema="br_rj_isp_estatisticas_seguranca",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(regiao as string) regiao,
    safe_cast(taxa_homicidio_doloso as float64) taxa_homicidio_doloso,
    safe_cast(taxa_latrocinio as float64) taxa_latrocinio,
    safe_cast(taxa_lesao_corporal_morte as float64) taxa_lesao_corporal_morte,
    safe_cast(
        taxa_crimes_violentos_letais_intencionais as float64
    ) taxa_crimes_violentos_letais_intencionais,
    safe_cast(
        taxa_homicidio_intervencao_policial as float64
    ) taxa_homicidio_intervencao_policial,
    safe_cast(taxa_letalidade_violenta as float64) taxa_letalidade_violenta,
    safe_cast(taxa_tentativa_homicidio as float64) taxa_tentativa_homicidio,
    safe_cast(taxa_lesao_corporal_dolosa as float64) taxa_lesao_corporal_dolosa,
    safe_cast(taxa_estupro as float64) taxa_estupro,
    safe_cast(taxa_homicidio_culposo as float64) taxa_homicidio_culposo,
    safe_cast(taxa_lesao_corporal_culposa as float64) taxa_lesao_corporal_culposa,
    safe_cast(taxa_roubo_transeunte as float64) taxa_roubo_transeunte,
    safe_cast(taxa_roubo_celular as float64) taxa_roubo_celular,
    safe_cast(taxa_roubo_corporal_coletivo as float64) taxa_roubo_corporal_coletivo,
    safe_cast(taxa_roubo_rua as float64) taxa_roubo_rua,
    safe_cast(taxa_roubo_veiculo as float64) taxa_roubo_veiculo,
    safe_cast(taxa_roubo_carga as float64) taxa_roubo_carga,
    safe_cast(taxa_roubo_comercio as float64) taxa_roubo_comercio,
    safe_cast(taxa_roubo_residencia as float64) taxa_roubo_residencia,
    safe_cast(taxa_roubo_banco as float64) taxa_roubo_banco,
    safe_cast(taxa_roubo_caixa_eletronico as float64) taxa_roubo_caixa_eletronico,
    safe_cast(taxa_roubo_conducao_saque as float64) taxa_roubo_conducao_saque,
    safe_cast(taxa_roubo_apos_saque as float64) taxa_roubo_apos_saque,
    safe_cast(taxa_roubo_bicicleta as float64) taxa_roubo_bicicleta,
    safe_cast(taxa_outros_roubos as float64) taxa_outros_roubos,
    safe_cast(taxa_total_roubos as float64) taxa_total_roubos,
    safe_cast(taxa_furto_veiculos as float64) taxa_furto_veiculos,
    safe_cast(taxa_furto_transeunte as float64) taxa_furto_transeunte,
    safe_cast(taxa_furto_coletivo as float64) taxa_furto_coletivo,
    safe_cast(taxa_furto_celular as float64) taxa_furto_celular,
    safe_cast(taxa_furto_bicicleta as float64) taxa_furto_bicicleta,
    safe_cast(taxa_outros_furtos as float64) taxa_outros_furtos,
    safe_cast(taxa_total_furtos as float64) taxa_total_furtos,
    safe_cast(taxa_sequestro as float64) taxa_sequestro,
    safe_cast(taxa_extorsao as float64) taxa_extorsao,
    safe_cast(taxa_sequestro_relampago as float64) taxa_sequestro_relampago,
    safe_cast(taxa_estelionato as float64) taxa_estelionato,
    safe_cast(taxa_apreensao_drogas as float64) taxa_apreensao_drogas,
    safe_cast(taxa_registro_posse_drogas as float64) taxa_registro_posse_drogas,
    safe_cast(taxa_registro_trafico_drogas as float64) taxa_registro_trafico_drogas,
    safe_cast(
        taxa_registro_apreensao_drogas_sem_autor as float64
    ) taxa_registro_apreensao_drogas_sem_autor,
    safe_cast(
        taxa_registro_veiculo_recuperado as float64
    ) taxa_registro_veiculo_recuperado,
    safe_cast(taxa_apf as float64) taxa_apf,
    safe_cast(taxa_aaapai as float64) taxa_aaapai,
    safe_cast(taxa_cmp as float64) taxa_cmp,
    safe_cast(taxa_cmba as float64) taxa_cmba,
    safe_cast(taxa_ameaca as float64) taxa_ameaca,
    safe_cast(taxa_pessoas_desaparecidas as float64) taxa_pessoas_desaparecidas,
    safe_cast(taxa_encontro_cadaver as float64) taxa_encontro_cadaver,
    safe_cast(taxa_encontro_ossada as float64) taxa_encontro_ossada,
    safe_cast(
        taxa_policial_militar_morto_servico as float64
    ) taxa_policial_militar_morto_servico,
    safe_cast(
        taxa_policial_civil_morto_servico as float64
    ) taxa_policial_civil_morto_servico,
    safe_cast(taxa_registro_ocorrencia as int64) taxa_registro_ocorrencia,
    safe_cast(tipo_fase as string) tipo_fase
from
    {{
        set_datalake_project(
            "br_rj_isp_estatisticas_seguranca_staging.taxa_evolucao_mensal_municipio"
        )
    }} as t
