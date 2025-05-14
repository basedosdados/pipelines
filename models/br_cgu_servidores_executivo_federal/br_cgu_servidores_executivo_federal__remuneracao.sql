{{
    config(
        schema="br_cgu_servidores_executivo_federal",
        alias="remuneracao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2013, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "mes"],
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 7)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 7)',
        ],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(replace(id_servidor, ".0", "") as string) id_servidor,
    safe_cast(cpf as string) cpf,
    safe_cast(nome as string) nome,
    safe_cast(
        replace(remuneracao_bruta_brl, ",", ".") as float64
    ) remuneracao_bruta_brl,
    safe_cast(
        replace(remuneracao_bruta_usd, ",", ".") as float64
    ) remuneracao_bruta_usd,
    safe_cast(replace(abate_teto_brl, ",", ".") as float64) abate_teto_brl,
    safe_cast(replace(abate_teto_usd, ",", ".") as float64) abate_teto_usd,
    safe_cast(
        replace(gratificao_natalina_brl, ",", ".") as float64
    ) gratificao_natalina_brl,
    safe_cast(
        replace(gratificao_natalina_usd, ",", ".") as float64
    ) gratificao_natalina_usd,
    safe_cast(
        replace(abate_teto_gratificacao_natalina_brl, ",", ".") as float64
    ) abate_teto_gratificacao_natalina_brl,
    safe_cast(
        replace(abate_teto_gratificacao_natalina_usd, ",", ".") as float64
    ) abate_teto_gratificacao_natalina_usd,
    safe_cast(replace(ferias_brl, ",", ".") as float64) ferias_brl,
    safe_cast(replace(ferias_usd, ",", ".") as float64) ferias_usd,
    safe_cast(
        replace(outras_remuneracoes_brl, ",", ".") as float64
    ) outras_remuneracoes_brl,
    safe_cast(
        replace(outras_remuneracoes_usd, ",", ".") as float64
    ) outras_remuneracoes_usd,
    safe_cast(replace(irrf_brl, ",", ".") as float64) irrf_brl,
    safe_cast(replace(irrf_usd, ",", ".") as float64) irrf_usd,
    safe_cast(replace(pss_rgps_brl, ",", ".") as float64) pss_rgps_brl,
    safe_cast(replace(pss_rgps_usd, ",", ".") as float64) pss_rgps_usd,
    safe_cast(replace(demais_deducoes_brl, ",", ".") as float64) demais_deducoes_brl,
    safe_cast(replace(demais_deducoes_usd, ",", ".") as float64) demais_deducoes_usd,
    safe_cast(replace(pensao_militar_brl, ",", ".") as float64) pensao_militar_brl,
    safe_cast(replace(pensao_militar_usd, ",", ".") as float64) pensao_militar_usd,
    safe_cast(replace(fundo_saude_brl, ",", ".") as float64) fundo_saude_brl,
    safe_cast(replace(fundo_saude_usd, ",", ".") as float64) fundo_saude_usd,
    safe_cast(
        replace(taxa_ocupacao_imovel_funcional_brl, ",", ".") as float64
    ) taxa_ocupacao_imovel_funcional_brl,
    safe_cast(
        replace(taxa_ocupacao_imovel_funcional_usd, ",", ".") as float64
    ) taxa_ocupacao_imovel_funcional_usd,
    safe_cast(
        replace(remuneracao_liquida_militar_brl, ",", ".") as float64
    ) remuneracao_liquida_militar_brl,
    safe_cast(
        replace(remuneracao_liquida_militar_usd, ",", ".") as float64
    ) remuneracao_liquida_militar_usd,
    safe_cast(
        replace(verba_indenizatoria_civil_brl, ",", ".") as float64
    ) verba_indenizatoria_civil_brl,
    safe_cast(
        replace(verba_indenizatoria_civil_usd, ",", ".") as float64
    ) verba_indenizatoria_civil_usd,
    safe_cast(
        replace(verba_indenizatoria_militar_brl, ",", ".") as float64
    ) verba_indenizatoria_militar_brl,
    safe_cast(
        replace(verba_indenizatoria_militar_usd, ",", ".") as float64
    ) verba_indenizatoria_militar_usd,
    safe_cast(
        replace(verba_indenizatoria_deslig_voluntario_brl, ",", ".") as float64
    ) verba_indenizatoria_deslig_voluntario_brl,
    safe_cast(
        replace(verba_indenizatoria_deslig_voluntario_usd, ",", ".") as float64
    ) verba_indenizatoria_deslig_voluntario_usd,
    safe_cast(
        replace(total_verba_indenizatoria_brl, ",", ".") as float64
    ) total_verba_indenizatoria_brl,
    safe_cast(
        replace(total_verba_indenizatoria_usd, ",", ".") as float64
    ) total_verba_indenizatoria_usd,
    safe_cast(origem as string) origem,
from
    {{
        set_datalake_project(
            "br_cgu_servidores_executivo_federal_staging.remuneracao"
        )
    }} as t
