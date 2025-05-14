{{
    config(
        alias="aihs_reduzidas",
        schema="br_ms_sih",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2024, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as int64) sigla_uf,
    safe_cast(n_aih as string) id_aih,
    safe_cast(ident as string) tipo_aih,
    case
        when gestor_cod = '00000' then '0' else cast(ltrim(gestor_cod, '0') as string)
    end as motivo_autorizacao_aih,
    safe_cast(ltrim(sequencia) as string) sequencial_aih,
    case
        when espec = '00' then '0' else cast(ltrim(espec, '0') as string)
    end as especialidade_leito,
    safe_cast(cobranca as string) motivo_saida,
    case
        when marca_uti = '00' then '0' else cast(ltrim(marca_uti, '0') as string)
    end as tipo_uti,
    case
        when marca_uci = '00' then '0' else cast(ltrim(marca_uci, '0') as string)
    end as tipo_uci,
    case
        when car_int = '00' then '0' else cast(ltrim(car_int, '0') as string)
    end as carater_internacao,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', dt_inter)) as date
    ) data_internacao,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', dt_saida)) as date
    ) data_saida,
    safe_cast(munic_mov as string) id_municipio_estabelecimento,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    safe_cast(nat_jur as string) natureza_juridica_estabelecimento,
    safe_cast(natureza as string) natureza_juridica_estabelecimento_ate_2012,
    safe_cast(regexp_replace(cgc_hosp, r'^0', '') as string) cnpj_estabelecimento,
    safe_cast(gestao as string) tipo_gestao_estabelecimento,
    safe_cast(
        trim(
            case
                when trim(uf_zi) like '%0000%'
                then null
                when trim(uf_zi) like '%9999%'
                then null
                else uf_zi
            end
        ) as string
    ) id_municipio_gestor,
    safe_cast(gestor_tp as string) tipo_gestor,
    safe_cast(gestor_cpf as string) cpf_gestor,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', gestor_dt)) as date
    ) data_autorizacao_gestor,
    safe_cast(regexp_replace(cnpj_mant, r'^0', '') as string) cnpj_mantenedora,
    safe_cast(munic_res as string) id_municipio_paciente,
    safe_cast(cep as string) cep_paciente,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', nasc)) as date
    ) data_nascimento_paciente,
    safe_cast(idade as int64) idade_paciente,
    safe_cast(cod_idade as string) unidade_medida_idade_paciente,
    case
        safe_cast(sexo as int64)
        when 0
        then 'Ignorado'
        when 9
        then 'Não definida'
        when 1
        then 'Masculino'
        when 2
        then 'Feminino'
        when 3
        then 'Feminino'
    end as sexo_paciente,
    case
        safe_cast(raca_cor as int64)
        when 0
        then 'Sem Informação'
        when 99
        then 'Sem Informação'
        when 1
        then 'Branca'
        when 2
        then 'Preta'
        when 3
        then 'Parda'
        when 4
        then 'Amarela'
        when 5
        then 'Indígena'
    end as raca_cor_paciente,
    case
        when etnia = '00' then '0' else cast(ltrim(etnia, '0') as string)
    end as etnia_paciente,
    safe_cast(nacional as int64) codigo_nacionalidade_paciente,
    safe_cast(
        trim(case when trim(cbor) = '000000' then null else cbor end) as string
    ) cbo_2002_paciente,
    safe_cast(homonimo as int64) indicador_paciente_homonimo,
    safe_cast(instru as string) grau_instrucao_paciente,
    safe_cast(num_filhos as int64) quantidade_filhos_paciente,
    safe_cast(
        trim(case when trim(cnaer) = '000' then null else cnaer end) as string
    ) id_acidente_trabalho,
    safe_cast(vincprev as string) tipo_vinculo_previdencia,
    safe_cast(
        trim(
            case when trim(insc_pn) = '0000000000' then null else insc_pn end
        ) as string
    ) id_gestante_pre_natal,
    safe_cast(gestrisco as int64) indicador_gestante_risco,
    case
        when contracep1 = '00' then '0' else cast(ltrim(contracep1, '0') as string)
    end as tipo_contraceptivo_principal,
    case
        when contracep2 = '00' then '0' else cast(ltrim(contracep2, '0') as string)
    end as tipo_contraceptivo_secundario,
    safe_cast(
        trim(case when trim(seq_aih5) = '000' then null else seq_aih5 end) as string
    ) sequencial_longa_permanencia,
    safe_cast(regexp_replace(proc_solic, r'^0', '') as string) procedimento_solicitado,
    safe_cast(regexp_replace(proc_rea, r'^0', '') as string) procedimento_realizado,
    safe_cast(infehosp as int64) indicador_infeccao_hospitalar,
    case
        when complex = '00' then '0' else cast(ltrim(complex, '0') as string)
    end as complexidade,
    safe_cast(ind_vdrl as string) indicador_exame_vdrl,
    safe_cast(financ as string) tipo_financiamento,
    safe_cast(faec_tp as string) subtipo_financiamento,
    safe_cast(regct as string) regra_contratual,
    safe_cast(
        trim(
            case when length(trim(cid_notif)) = 3 then cid_notif else null end
        ) as string
    ) as cid_notificacao_categoria,
    safe_cast(
        trim(
            case
                when length(trim(cid_notif)) = 4 and trim(cid_notif) not like '0000'
                then cid_notif
                else null
            end
        ) as string
    ) cid_notificacao_subcategoria,
    safe_cast(
        trim(case when length(trim(cid_asso)) = 3 then cid_asso else null end) as string
    ) as cid_causa_categoria,
    safe_cast(
        trim(
            case
                when length(trim(cid_asso)) = 4 and trim(cid_asso) not like '0000'
                then cid_asso
                else null
            end
        ) as string
    ) as cid_causa_subcategoria,
    safe_cast(
        trim(
            case when length(trim(diag_princ)) = 3 then diag_princ else null end
        ) as string
    ) as cid_principal_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diag_princ)) = 4 and trim(diag_princ) not like '0000'
                then diag_princ
                else null
            end
        ) as string
    ) as cid_principal_subcategoria,
    safe_cast(
        trim(
            case when length(trim(diag_secun)) = 3 then diag_secun else null end
        ) as string
    ) as cid_secundario_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diag_secun)) = 4 and trim(diag_secun) not like '0000'
                then diag_secun
                else null
            end
        ) as string
    ) as cid_secundario_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec1)) = 3 then diagsec1 else null end) as string
    ) as cid_diagnostico_secundario_1_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec1)) = 4 and trim(diagsec1) not like '0000'
                then diagsec1
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_1_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec2)) = 3 then diagsec2 else null end) as string
    ) as cid_diagnostico_secundario_2_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec2)) = 4 and trim(diagsec2) not like '0000'
                then diagsec2
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_2_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec3)) = 3 then diagsec3 else null end) as string
    ) as cid_diagnostico_secundario_3_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec3)) = 4 and trim(diagsec3) not like '0000'
                then diagsec3
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_3_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec4)) = 3 then diagsec4 else null end) as string
    ) as cid_diagnostico_secundario_4_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec4)) = 4 and trim(diagsec4) not like '0000'
                then diagsec4
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_4_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec5)) = 3 then diagsec5 else null end) as string
    ) as cid_diagnostico_secundario_5_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec5)) = 4 and trim(diagsec5) not like '0000'
                then diagsec5
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_5_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec6)) = 3 then diagsec6 else null end) as string
    ) as cid_diagnostico_secundario_6_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec6)) = 4 and trim(diagsec6) not like '0000'
                then diagsec6
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_6_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec7)) = 3 then diagsec7 else null end) as string
    ) as cid_diagnostico_secundario_7_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec7)) = 4 and trim(diagsec7) not like '0000'
                then diagsec7
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_7_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec8)) = 3 then diagsec8 else null end) as string
    ) as cid_diagnostico_secundario_8_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec8)) = 4 and trim(diagsec8) not like '0000'
                then diagsec8
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_8_subcategoria,
    safe_cast(
        trim(case when length(trim(diagsec9)) = 3 then diagsec9 else null end) as string
    ) as cid_diagnostico_secundario_9_categoria,
    safe_cast(
        trim(
            case
                when length(trim(diagsec9)) = 4 and trim(diagsec9) not like '0000'
                then diagsec9
                else null
            end
        ) as string
    ) as cid_diagnostico_secundario_9_subcategoria,
    safe_cast(tpdisec1 as int64) tipo_diagnostico_secundario_1,
    safe_cast(tpdisec2 as int64) tipo_diagnostico_secundario_2,
    safe_cast(tpdisec3 as int64) tipo_diagnostico_secundario_3,
    safe_cast(tpdisec4 as int64) tipo_diagnostico_secundario_4,
    safe_cast(tpdisec5 as int64) tipo_diagnostico_secundario_5,
    safe_cast(tpdisec6 as int64) tipo_diagnostico_secundario_6,
    safe_cast(tpdisec7 as int64) tipo_diagnostico_secundario_7,
    safe_cast(tpdisec8 as int64) tipo_diagnostico_secundario_8,
    safe_cast(tpdisec9 as int64) tipo_diagnostico_secundario_9,
    safe_cast(
        trim(
            case when length(trim(cid_morte)) = 3 then cid_morte else null end
        ) as string
    ) as cid_morte_categoria,
    safe_cast(
        trim(
            case
                when length(trim(cid_morte)) = 4 and trim(cid_morte) not like '0000'
                then cid_morte
                else null
            end
        ) as string
    ) as cid_morte_subcategoria,
    safe_cast(morte as int64) indicador_obito,
    safe_cast(remessa as string) remessa,
    safe_cast(aud_just as string) justificativa_auditor,
    safe_cast(sis_just as string) justificativa_estabelecimento,
    safe_cast(uti_mes_to as int64) quantidade_dias_uti_mes,
    safe_cast(uti_int_to as int64) quantidade_dias_unidade_intermediaria,
    safe_cast(diar_acom as int64) quantidade_dias_acompanhate,
    safe_cast(qt_diarias as int64) quantidade_dias,
    safe_cast(dias_perm as int64) quantidade_dias_permanencia,
    safe_cast(val_sh_fed as float64) valor_complemento_federal_servicos_hospitalares,
    safe_cast(val_sp_fed as float64) valor_complemento_federal_servicos_profissionais,
    safe_cast(val_sh_ges as float64) valor_complemento_gestor_servicos_hospitalares,
    safe_cast(val_sp_ges as float64) valor_complemento_gestor_servicos_profissionais,
    safe_cast(val_uci as float64) valor_uci,
    safe_cast(val_sh as float64) valor_serivico_hospitalar,
    safe_cast(val_sp as float64) valor_servico_profissional,
    safe_cast(val_uti as float64) valor_uti,
    safe_cast(us_tot as float64) valor_dolar,
    safe_cast(val_tot as float64) valor_aih,
from {{ set_datalake_project("br_ms_sih_staging.aihs_reduzidas") }}

{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
