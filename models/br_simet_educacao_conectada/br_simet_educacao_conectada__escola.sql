{{
    config(
        alias="escola",
        schema="br_simet_educacao_conectada",
        materialized="table",
    )
}}

select
    safe_cast(ano_censo as int64) ano_censo,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_escola as string) id_escola,
    safe_cast(id_setor as string) id_setor,
    safe_cast(rede as string) rede,
    safe_cast(localizacao as string) localizacao,
    safe_cast(porte_escola as string) porte_escola,
    safe_cast(tipo_rede_local as string) tipo_rede_local,
    safe_cast(tipo_energia as string) tipo_energia,
    safe_cast(tipo_tecnologia as string) tipo_tecnologia,
    safe_cast(tipo_recurso_recebido as string) tipo_recurso_recebido,
    safe_cast(faixa_velocidade as string) faixa_velocidade,
    safe_cast(nome_empresa_provedora_1 as string) nome_empresa_provedora_1,
    safe_cast(nome_empresa_provedora_2 as string) nome_empresa_provedora_2,
    safe_cast(nome_simet_asn as string) nome_simet_asn,
    safe_cast(
        indicador_laboratorio_informatica as bool
    ) indicador_laboratorio_informatica,
    safe_cast(indicador_internet as bool) indicador_internet,
    safe_cast(indicador_internet_alunos as bool) indicador_internet_alunos,
    safe_cast(indicador_internet_aprendizagem as bool) indicador_internet_aprendizagem,
    safe_cast(indicador_satelite_mec as bool) indicador_satelite_mec,
    safe_cast(razao_desktop_aluno as float64) razao_desktop_aluno,
    safe_cast(razao_comp_portatil_aluno as float64) razao_comp_portatil_aluno,
    safe_cast(razao_tablet_aluno as float64) razao_tablet_aluno,
    safe_cast(quantidade_matricula as int64) quantidade_matricula,
    safe_cast(
        quantidade_matricula_maior_turno as int64
    ) quantidade_matricula_maior_turno,
    safe_cast(quantidade_turma as int64) quantidade_turma,
    safe_cast(quantidade_medicao as int64) quantidade_medicao,
    safe_cast(quantidade_medicoes_entorno as int64) quantidade_medicoes_entorno,
    safe_cast(quantidade_ipv6 as int64) quantidade_ipv6,
    safe_cast(quantidade_asn as int64) quantidade_asn,
    safe_cast(media_tcp_download_mbps as float64) media_tcp_download,
    safe_cast(media_tcp_upload_mbps as float64) media_tcp_upload,
    safe_cast(media_latencia_ms as float64) media_latencia,
    safe_cast(media_perda_pacote as float64) media_perda_pacote,
    safe_cast(media_jitter_download_ms as float64) media_jitter_download,
    safe_cast(media_jitter_upload_ms as float64) media_jitter_upload,
    safe_cast(media_download_entorno as float64) media_download_entorno,
    safe_cast(media_upload_entorno as float64) media_upload_entorno,
    safe_cast(media_latencia_entorno as float64) media_latencia_entorno,
    safe_cast(media_pacotes_entorno as float64) media_pacotes_entorno,
    safe_cast(media_jitter_upload_entorno as float64) media_jitter_upload_entorno,
    safe_cast(media_jitter_download_entorno as float64) media_jitter_download_entorno,
    safe_cast(comparador_empresas as string) comparador_empresas,
    safe_cast(comparativo_download_entorno as string) comparativo_download_entorno,
    safe_cast(comparativo_upload_entorno as string) comparativo_upload_entorno,
    safe_cast(comparativo_latencia_entorno as string) comparativo_latencia_entorno,
    safe_cast(
        velocidade_download_necessaria_mbit as float64
    ) velocidade_download_necessaria,
    safe_cast(razao_download_por_aluno_kbps as float64) razao_download_aluno,
    safe_cast(tipo_download_por_aluno_kbit as string) tipo_download_aluno,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64)) ponto,
    date(2024, 12, 12) data_ultima_atualizacao

from {{ set_datalake_project("br_simet_educacao_conectada_staging.escola") }} as t
