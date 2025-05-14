{{
    config(
        alias="psicossocial",
        schema="br_ms_sia",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
    )
}}


with
    sia_add_municipios as (
        -- Adicionar id_municipio de 7 dígitos
        select
            psicossocial.*,
            mun.id_municipio as id_municipio_executante,
            mun_res.id_municipio as id_municipio_residencia
        from
            {{ set_datalake_project("br_ms_sia_staging.psicossocial") }} as psicossocial
        left join
            (
                select id_municipio, id_municipio_6
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on psicossocial.ufmun = mun.id_municipio_6
        left join
            (
                select id_municipio, id_municipio_6
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun_res
            on psicossocial.munpac = mun_res.id_municipio_6
        {% if is_incremental() %}
            where
                date(cast(ano as int64), cast(mes as int64), 1) > (
                    select max(date(cast(ano as int64), cast(mes as int64), 1))
                    from {{ this }}
                )
        {% endif %}
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio_executante as string) id_municipio,
    safe_cast(cnes_exec as string) id_estabelecimento_cnes,
    safe_cast(cnes_esf as string) id_estabelecimento_cnes_familia,
    -- colunas removidas da materilização final pois estão todas presentes
    -- na tabela de estabelecimento -> basedosdados.br_ms_cnes.estabelecimento
    -- safe_cast(gestao as string) id_gestao,
    -- safe_cast(condic as string) tipo_gestao,
    -- safe_cast(tpups as string) tipo_unidade,
    -- safe_cast(tippre__ as string) tipo_prestador,
    -- safe_cast(mn_ind as string) tipo_mantenedor_estabelecimento,
    -- safe_cast(cnpjcpf as string) cnpj_estabelecimento_executante,
    -- safe_cast(cnpjmnt as string) cnpj_mantenedora_estabelecimento,
    -- safe_cast(nat_jur as string) natureza_juridica_estabelecimento,
    safe_cast(pa_proc_id as string) id_procedimento_ambulatorial,
    safe_cast(pa_srv as string) id_servico_especializado,
    safe_cast(pa_class_s as string) id_classificacao_servico,
    -- retirada pois apresenta dígitos sem sentido
    -- safe_cast(cns_pac as string) id_cns_paciente_criptografado,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', inicio)) as date
    ) data_inicio_atendimento,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', fim)) as date
    ) data_termino_atendimento,
    safe_cast(permanen as string) permanencia_atendimento,
    safe_cast(mot_cob as string) motivo_saida_permanencia,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', dt_motcob)) as date
    ) as data_motivo_saida_permanencia,
    safe_cast(substr(dt_process, 1, 4) as int64) as ano_processamento,
    safe_cast(substr(dt_process, 5, 2) as int64) as mes_processamento,
    safe_cast(substr(dt_atend, 1, 4) as int64) as ano_atendimento,
    safe_cast(substr(dt_atend, 5, 2) as int64) as mes_atendimento,
    safe_cast(
        format_date('%Y-%m-%d', safe.parse_date('%Y%m%d', dtnasc)) as date
    ) data_nascimento_paciente,
    safe_cast(id_municipio_residencia as string) id_municipio_residencia_paciente,
    safe_cast(ltrim(cast(origem_pac as string), '0') as string) origem_paciente,
    safe_cast(ltrim(cast(nacion_pac as string), '0') as string) nacionalidade_paciente,
    safe_cast(tpidadepac as string) tipo_idade,
    safe_cast(idadepac as int64) idade_paciente,
    safe_cast(sexopac as string) sexo_paciente,
    safe_cast(racacor as string) raca_cor_paciente,
    safe_cast(etnia as string) etnia_paciente,
    case
        when catend = '00' then '0' else cast(ltrim(catend, '0') as string)
    end as carater_atendimento,
    safe_cast(
        trim(case when length(trim(cidpri)) = 3 then cidpri else null end) as string
    ) as cid_principal_categoria,
    safe_cast(
        trim(
            case
                when length(trim(cidpri)) = 4 and cidpri != '0000'
                then cidpri
                when
                    length(trim(cidpri)) = 3
                    and cidpri in (
                        select subcategoria
                        from `basedosdados.br_bd_diretorios_brasil.cid_10`
                        where length(subcategoria) = 3
                    )
                then cidpri
                else null
            end
        ) as string
    ) as cid_principal_subcategoria,
    safe_cast(
        trim(case when length(trim(cidassoc)) = 3 then cidassoc else null end) as string
    ) as cid_causas_associadas_categoria,
    safe_cast(
        trim(
            case
                when length(trim(cidassoc)) = 4 and cidassoc != '0000'
                then cidassoc
                when
                    length(trim(cidassoc)) = 3
                    and cidassoc in (
                        select subcategoria
                        from `basedosdados.br_bd_diretorios_brasil.cid_10`
                        where length(subcategoria) = 3
                    )
                then cidassoc
                else null
            end
        ) as string
    ) as cid_causas_associadas_subcategoria,
    safe_cast(tp_droga as string) tipo_droga,
    safe_cast(destinopac as string) destino_paciente,
    safe_cast(loc_realiz as string) local_realizacao_atendimento,
    safe_cast(
        case
            when sit_rua = 'S' then '1' when sit_rua = 'N' then '0' else sit_rua
        end as int64
    ) indicador_situacao_rua,
    safe_cast(cob_esf as int64) indicador_estrategia_familia,
    safe_cast(pa_qtdpro as int64) quantidade_produzida_procedimento,
    safe_cast(pa_qtdapr as int64) quantidade_aprovada_procedimento,
    safe_cast(qtdate as int64) quantidade_atendimentos,
    safe_cast(qtdpcn as string) quantidade_pacientes,
from sia_add_municipios as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
