{{ config(alias="uf", schema="br_fbsp_absp") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(quantidade_cvli as int64) quantidade_cvli,
    safe_cast(quantidade_feminicidio as int64) quantidade_feminicidio,
    safe_cast(
        quantidade_ocorrencia_homicidio_doloso as int64
    ) quantidade_ocorrencia_homicidio_doloso,
    safe_cast(
        quantidade_vitima_homicidio_doloso as int64
    ) quantidade_vitima_homicidio_doloso,
    safe_cast(quantidade_latrocinio as int64) quantidade_latrocinio,
    safe_cast(
        quantidade_lesao_corporal_seguida_de_morte as int64
    ) quantidade_lesao_corporal_seguida_de_morte,
    safe_cast(quantidade_morte_a_esclarecer as int64) quantidade_morte_a_esclarecer,
    safe_cast(
        quantidade_morte_intervencao_policial_civil_servico as int64
    ) quantidade_morte_intervencao_policial_civil_servico,
    safe_cast(
        quantidade_morte_intervencao_policial_civil_fora_servico as int64
    ) quantidade_morte_intervencao_policial_civil_fora_servico,
    safe_cast(
        quantidade_morte_intervencao_policial_militar_servico as int64
    ) quantidade_morte_intervencao_policial_militar_servico,
    safe_cast(
        quantidade_morte_intervencao_policial_militar_fora_servico as int64
    ) quantidade_morte_intervencao_policial_militar_fora_servico,
    safe_cast(
        quantidade_morte_violenta_intencional as int64
    ) quantidade_morte_violenta_intencional,
    safe_cast(
        quantidade_policial_civil_morto_confronto_servico as int64
    ) quantidade_policial_civil_morto_confronto_servico,
    safe_cast(
        quantidade_policial_civil_morto_confronto_fora_servico as int64
    ) quantidade_policial_civil_morto_confronto_fora_servico,
    safe_cast(
        quantidade_policial_militar_morto_confronto_servico as int64
    ) quantidade_policial_militar_morto_confronto_servico,
    safe_cast(
        quantidade_policial_militar_morto_confronto_fora_servico as int64
    ) quantidade_policial_militar_morto_confronto_fora_servico,
    safe_cast(quantidade_suicidio as int64) quantidade_suicidio,
    safe_cast(quantidade_estupro as int64) quantidade_estupro,
    safe_cast(quantidade_tentativa_estupro as int64) quantidade_tentativa_estupro,
    safe_cast(quantidade_furto_veiculo as int64) quantidade_furto_veiculo,
    safe_cast(
        quantidade_roubo_instituicao_financeira as int64
    ) quantidade_roubo_instituicao_financeira,
    safe_cast(quantidade_roubo_carga as int64) quantidade_roubo_carga,
    safe_cast(quantidade_roubo_de_veiculo as int64) quantidade_roubo_de_veiculo,
    safe_cast(quantidade_arma_fogo_apreendida as int64) quantidade_arma_fogo_apreendida,
    safe_cast(
        quantidade_registro_pessoa_desaparecida as int64
    ) quantidade_registro_pessoa_desaparecida,
    safe_cast(
        quantidade_populacao_sistema_penitenciario as int64
    ) quantidade_populacao_sistema_penitenciario,
    safe_cast(
        despesa_empenhada_seguranca_publica as float64
    ) despesa_empenhada_seguranca_publica
from {{ set_datalake_project("br_fbsp_absp_staging.uf") }}
