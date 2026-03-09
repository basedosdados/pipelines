{{
    config(
        schema="br_ans_beneficiario",
        alias="informacao_consolidada",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2014, "end": 2026, "interval": 1},
        },
        cluster_by=["id_municipio", "mes", "sigla_uf"],
        labels={"project_id": "basedosdados"},
    )
}}

with
    ans as (
        select
            cast(ano as int64) ano,
            cast(mes as int64) mes,
            cast(t.sigla_uf as string) sigla_uf,
            bd.id_municipio as id_municipio,
            cast(cd_operadora as string) codigo_operadora,
            cast(
                initcap(
                    translate(
                        nm_razao_social,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) razao_social,
            cast(lpad(nr_cnpj, 14, '0') as string) cnpj,
            initcap(modalidade_operadora) as modalidade_operadora,
            cast(tp_sexo as string) sexo,
            cast(
                lower(
                    translate(
                        de_faixa_etaria,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) faixa_etaria,
            cast(
                lower(
                    translate(
                        de_faixa_etaria_reaj,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) faixa_etaria_reajuste,
            cast(cd_plano as string) codigo_plano,
            cast(tp_vigencia_plano as string) tipo_vigencia_plano,
            cast(
                initcap(
                    translate(
                        de_contratacao_plano,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) contratacao_beneficiario,
            cast(
                initcap(
                    translate(
                        de_segmentacao_plano,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) segmentacao_beneficiario,
            cast(de_abrg_geografica_plano as string) abrangencia_beneficiario,
            cast(
                initcap(
                    translate(
                        cobertura_assist_plan,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) cobertura_assistencia_beneficiario,
            cast(
                initcap(
                    translate(
                        tipo_vinculo,
                        'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                        'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'
                    )
                ) as string
            ) tipo_vinculo,
            cast(qt_beneficiario_ativo as int64) quantidade_beneficiario_ativo,
            cast(qt_beneficiario_aderido as int64) quantidade_beneficiario_aderido,
            cast(qt_beneficiario_cancelado as int64) quantidade_beneficiario_cancelado,
            coalesce(
                safe.parse_date('%d/%m/%Y', dt_carga),
                safe.parse_date('%Y-%m-%d', dt_carga)
            ) data_carga,
        from
            {{
                set_datalake_project(
                    "br_ans_beneficiario_staging.informacao_consolidada"
                )
            }} t
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` bd
            on t.cd_municipio = bd.id_municipio_6
    )
select *
from
    ans
    {# from ans
{% if is_incremental() %}
    where data_carga > (select max(data_carga) from {{ this }})
{% endif %} #}
