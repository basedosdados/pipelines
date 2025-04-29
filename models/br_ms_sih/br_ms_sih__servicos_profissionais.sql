{{
    config(
        alias="servicos_profissionais",
        schema="br_ms_sih",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sp_aa as int64) ano_internacao,
    safe_cast(sp_mm as int64) mes_internacao,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', sp_dtinter)) as date
    ) data_entrada_internacao,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', sp_dtsaida)) as date
    ) data_saida_iternacao,
    safe_cast(id_municipio as string) id_municipio_estabelecimento_aih,
    safe_cast(sp_m_pac as string) id_municipio_paciente,
    safe_cast(sp_gestor as string) id_gestor,
    safe_cast(sp_cnes as string) id_estabelecimento_cnes,
    safe_cast(sp_naih as string) id_aih,
    safe_cast(sp_procrea as string) id_procedimento_principal,
    safe_cast(serv_cla as string) tipo_servico,
    safe_cast(sp_cpfcgc as string) id_prestador_servico,
    safe_cast(sp_atoprof as string) id_procedimento_secundario,
    safe_cast(sp_pf_cbo as string) cbo_2002_profissional,
    safe_cast(ltrim(sp_qt_proc) as int64) quantidade_procedimentos,
    safe_cast(
        trim(
            case when length(trim(sp_cidpri)) = 3 then sp_cidpri else null end
        ) as string
    ) id_cid_principal,
    safe_cast(
        trim(
            case
                when length(trim(sp_cidpri)) = 4 and sp_cidpri != '0000'
                then sp_cidpri
                when
                    length(trim(sp_cidpri)) = 3
                    and sp_cidpri in (
                        select subcategoria
                        from `basedosdados.br_bd_diretorios_brasil.cid_10`
                        where length(subcategoria) = 3
                    )
                then sp_cidpri
                else null
            end
        ) as string
    ) as id_cid_principal_subcategoria,
    safe_cast(
        trim(
            case when length(trim(sp_cidsec)) = 3 then sp_cidsec else null end
        ) as string
    ) id_cid_secundario,
    safe_cast(
        trim(
            case
                when length(trim(sp_cidsec)) = 4 and sp_cidsec != '0000'
                then sp_cidsec
                when
                    length(trim(sp_cidsec)) = 3
                    and sp_cidsec in (
                        select subcategoria
                        from `basedosdados.br_bd_diretorios_brasil.cid_10`
                        where length(subcategoria) = 3
                    )
                then sp_cidsec
                else null
            end
        ) as string
    ) as id_cid_secundario_subcategoria,
    safe_cast(sp_complex as string) complexidade_ato_profissional,
    safe_cast(ltrim(sp_qtd_ato) as int64) quantidade_atos_profissionais,
    safe_cast(ltrim(sp_ptsp) as int64) quantidade_pontos,
    safe_cast(sp_nf as string) nota_fiscal,
    safe_cast(ltrim(sp_valato) as float64) valor_ato_profissional,
    safe_cast(sp_des_hos as int64) indicador_uf_hospital,
    safe_cast(sp_des_pac as int64) indicador_uf_paciente,
    safe_cast(sp_u_aih as int64) indicador_id_aih,
    safe_cast(sp_financ as string) tipo_financiamento_ato_profissional,
    safe_cast(sp_co_faec as string) tipo_subtipo_financiamento_ato_profissional,
    safe_cast(sp_pf_doc as string) tipo_documento_pf,
    safe_cast(sp_pj_doc as string) tipo_documento_pj,
    safe_cast(in_tp_val as string) tipo_valor,
    safe_cast(ltrim(sequencia) as string) sequencia,
    safe_cast(remessa as string) nome_remessa,
from {{ set_datalake_project("br_ms_sih_staging.servicos_profissionais") }} as sih
left join
    (
        select id_municipio, id_municipio_6,
        from `basedosdados.br_bd_diretorios_brasil.municipio`
    ) as mun
    on sih.sp_m_hosp = mun.id_municipio_6
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
