{{
    config(
        alias="producao_ambulatorial",
        schema="br_ms_sia",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter                     ON {{this}}                     GRANT TO ("allUsers")                     FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter        ON  {{this}}                     GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")                     FILTER USING (True)',
        ],
    )
}}

with
    sia_add_municipios as (
        -- Adicionar id_municipio de 7 dígitos
        select *
        from
            {{ set_datalake_project("br_ms_sia_staging.producao_ambulatorial") }}
            as producao_ambulatorial

        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on producao_ambulatorial.pa_ufmun = mun.id_municipio_6
        {% if is_incremental() %}
            where
                date(cast(ano as int64), cast(mes as int64), 1) not in (
                    select distinct (date(cast(ano as int64), cast(mes as int64), 1))
                    from {{ this }}
                )
        {% endif %}
    ),

    sia as (

        select
            safe_cast(ano as int64) ano,
            safe_cast(mes as int64) mes,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(id_municipio as string) id_municipio,
            safe_cast(pa_coduni as string) id_estabelecimento_cnes,
            -- seguintes colunas foram omitidas pois podem ser obtidas ao cruzar o
            -- id_estabelecimento_cnes
            -- com a tabela estabelecimento do conjunto br_ms_cnes
            -- safe_cast(pa_nat_jur as string) natureza_juridica_estabelecimento,
            -- safe_cast(pa_tpups as string) tipo_unidade,
            -- safe_cast(pa_tippre as string) tipo_prestador,
            -- safe_cast(pa_cnpjcpf as string) cnpj_estabelecimento_executante,
            -- safe_cast(
            -- regexp_replace(pa_cnpjmnt, '0{14}', '') as string
            -- ) cnpj_mantenedora_estabalecimento,
            -- safe_cast(regexp_replace(pa_cnpj_cc, '0{14}', '') as string) cnpj_orgao,
            -- safe_cast(pa_mn_ind as string) tipo_mantenedor_estabelecimento,
            -- safe_cast(pa_gestao as string) id_gestao,
            -- safe_cast(pa_condic as string) tipo_gestao,
            safe_cast(pa_regct as string) tipo_regra_contratual,
            safe_cast(pa_ine as string) id_equipe,
            safe_cast(pa_srv_c as string) id_servico_especializado,
            safe_cast(pa_proc_id as string) id_processamento_ambulatorial,
            safe_cast(
                regexp_replace(pa_cnsmed, '0{15}', '') as string
            ) id_cns_executante,
            safe_cast(replace(pa_cbocod, '', null) as string) id_cbo_2002,
            safe_cast(
                regexp_replace(pa_autoriz, '0{13}', '') as string
            ) codigo_autorizacao_apac,
            safe_cast(pa_codoco as string) codigo_ocorrencia,
            case
                when pa_tpfin = '00' then '0' else cast(ltrim(pa_tpfin, '0') as string)
            end as tipo_financiamento_producao,
            case
                when pa_subfin = '0000'
                then '0'
                else cast(ltrim(pa_subfin, '0') as string)
            end as subtipo_financiamento_producao,
            -- - parse e criar ano mes data é yyyy-mm
            safe_cast(substr(pa_mvm, 1, 4) as int64) as ano_processamento_procedimento,
            safe_cast(substr(pa_mvm, 5, 2) as int64) as mes_processamento_procedimento,
            safe_cast(substr(pa_cmp, 1, 4) as int64) as ano_realizacao_procedimento,
            safe_cast(substr(pa_cmp, 5, 2) as int64) as mes_realizacao_procedimento,
            safe_cast(
                trim(
                    case when length(trim(pa_cidpri)) = 3 then pa_cidpri else null end
                ) as string
            ) as cid_principal_categoria,
            safe_cast(
                trim(
                    case
                        when length(trim(pa_cidpri)) = 4 and pa_cidpri != '0000'
                        then pa_cidpri
                        when
                            length(trim(pa_cidpri)) = 3
                            and pa_cidpri in (
                                select subcategoria
                                from `basedosdados.br_bd_diretorios_brasil.cid_10`
                                where length(subcategoria) = 3
                            )
                        then pa_cidpri
                        else null
                    end
                ) as string
            ) as cid_principal_subcategoria,
            safe_cast(
                trim(
                    case when length(trim(pa_cidsec)) = 3 then pa_cidsec else null end
                ) as string
            ) as cid_secundario_categoria,
            safe_cast(
                trim(
                    case
                        when length(trim(pa_cidsec)) = 4 and pa_cidsec != '0000'
                        then pa_cidsec
                        when
                            length(trim(pa_cidsec)) = 3
                            and pa_cidsec in (
                                select subcategoria
                                from `basedosdados.br_bd_diretorios_brasil.cid_10`
                                where length(subcategoria) = 3
                            )
                        then pa_cidsec
                        else null
                    end
                ) as string
            ) as cid_secundario_subcategoria,
            safe_cast(
                trim(
                    case when length(trim(pa_cidcas)) = 3 then pa_cidcas else null end
                ) as string
            ) as cid_causas_associadas_categoria,
            safe_cast(
                trim(
                    case
                        when length(trim(pa_cidcas)) = 4 and pa_cidcas != '0000'
                        then pa_cidcas
                        when
                            length(trim(pa_cidcas)) = 3
                            and pa_cidcas in (
                                select subcategoria
                                from `basedosdados.br_bd_diretorios_brasil.cid_10`
                                where length(subcategoria) = 3
                            )
                        then pa_cidcas
                        else null
                    end
                ) as string
            ) as cid_causas_associadas_subcategoria,
            case
                when pa_catend = '00'
                then '0'
                else cast(ltrim(pa_catend, '0') as string)
            end as carater_atendimento,
            safe_cast(
                regexp_replace(pa_munpcn, '9{6}', '') as string
            ) id_paciente_proto,
            safe_cast(replace(pa_sexo, '0', '') as string) sexo_paciente,
            safe_cast(regexp_replace(pa_idade, '9{3}', '') as int64) idade_paciente,
            case
                when pa_racacor = '00'
                then '0'
                else cast(ltrim(pa_racacor, '0') as string)
            end as raca_cor_paciente,
            safe_cast(pa_etnia as string) etnia_paciente,
            safe_cast(idademin as int64) idade_minima_paciente,
            safe_cast(idademax as int64) idade_maxima_paciente,
            case
                when pa_flidade = '00'
                then '0'
                else cast(ltrim(pa_flidade, '0') as string)
            end as compatibilidade_idade_procedimento,
            case
                when pa_nivcpl = '00'
                then '0'
                else cast(ltrim(pa_nivcpl, '0') as string)
            end as complexidade_procedimento,
            case
                when pa_docorig = '00'
                then '0'
                else cast(ltrim(pa_docorig, '0') as string)
            end as instrumento_registro,
            safe_cast(pa_valapr as float64) valor_aprovado_procedimento,
            safe_cast(pa_qtdapr as int64) quantidade_aprovada_procedimento,
            safe_cast(pa_valpro as float64) valor_produzido_procedimento,
            safe_cast(pa_qtdpro as int64) quantidade_produzida_procedimento,
            safe_cast(nu_vpa_tot as float64) valor_unitario_procedimento_vpa,
            safe_cast(nu_pa_tot as float64) valor_unitario_procedimento_sigtap,
            safe_cast(pa_dif_val as float64) diferenca_valor_unitario,
            safe_cast(pa_vl_cf as float64) valor_complemento_federal,
            safe_cast(pa_vl_cl as float64) valor_complemento_local,
            safe_cast(pa_vl_inc as float64) valor_incremento,
            case
                when pa_motsai = '00'
                then '0'
                else cast(ltrim(pa_motsai, '0') as string)
            end as motivo_saida_paciente,
            -- - em uf e muicipio replace de
            safe_cast(
                regexp_replace(pa_ufdif, '9{1}', '') as int64
            ) indicador_uf_residencia_paciente,
            safe_cast(
                regexp_replace(pa_mndif, '9{1}', '') as int64
            ) indicador_municipio_residencia_paciente,
            --
            safe_cast(
                case
                    when pa_incout = '0000'
                    then '0'
                    else regexp_replace(pa_incout, '[^0]', '1')
                end as int64
            ) as indicador_incrementos_outros,
            safe_cast(
                case
                    when pa_incurg = '0000'
                    then '0'
                    else regexp_replace(pa_incurg, '[^0]', '1')
                end as int64
            ) as indicador_incrementos_urgencia,
            safe_cast(pa_obito as int64) indicador_obito,
            safe_cast(pa_encerr as int64) indicador_encerramento,
            safe_cast(pa_perman as int64) indicador_permanencia,
            safe_cast(pa_alta as int64) indicador_alta,
            safe_cast(pa_transf as int64) indicador_transferencia,
            safe_cast(pa_indica as string) tipo_situacao_produzida,
            safe_cast(pa_flqt as string) tipo_erro_quantidade_produzida,
            safe_cast(pa_fler as string) flag_erro_corpo_apac,
        from sia_add_municipios
    )

select *
from sia

{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})

{% endif %}
