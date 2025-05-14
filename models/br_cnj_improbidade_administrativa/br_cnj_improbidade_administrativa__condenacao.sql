{{
    config(
        schema="br_cnj_improbidade_administrativa",
        alias="condenacao",
        materialized="table",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter                         ON {{this}}                         GRANT TO ("allUsers")                         FILTER USING (DATE_DIFF(CURRENT_DATE(), data_propositura, MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter                         ON  {{this}}                         GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")                         FILTER USING (True)',
        ],
    )
}}

select
    safe_cast(nome as string) nome,
    safe_cast(num_processo as string) numero_processo,
    safe_cast(condenacao_id as string) id_condenacao,
    safe_cast(processo_id as string) id_processo,
    case
        when tipo_pessoa = "F"
        then "Física"
        when tipo_pessoa = "J"
        then "Jurídica"
        else null
    end as tipo_pessoa,
    safe_cast(pessoa_id as string) id_pessoa,
    case
        when sexo = "M" then "Masculino" when sexo = "F" then "Feminino" else null
    end as sexo,
    case
        when publico = "S" then true when publico = "N" then false else null
    end as funcionario_publico,
    case
        when esfera_pessoa = "F"
        then "Federal"
        when esfera_pessoa = "D"
        then "Distrital"
        when esfera_pessoa = "E"
        then "Estadual"
        when esfera_pessoa = "M"
        then "Municipal"
        else null
    end as esfera_funcionario,
    safe_cast(orgao as string) orgao,
    safe_cast(cargo as string) cargo,
    safe_cast(if(length(uf) = 2, uf, null) as string) sigla_uf,
    case
        when
            coalesce(`1o_grau___justica_estadual`, `1o_grau___justica_federal`)
            is not null
        then "1 grau"
        when
            coalesce(`2o_grau___justica_estadual`, `2o_grau___justica_federal`)
            is not null
        then "2 grau"
        when `auditoria_militar` is not null
        then "Militar"
        else null
    end as instancia,
    safe_cast(
        coalesce(
            tribunal_de_justica_estadual,
            tribunal_regional_federal,
            tribunal_militar_estadual
        ) as string
    ) as tribunal,
    safe_cast(esfera as string) esfera_processo,
    safe_cast(comarca as string) comarca,
    safe_cast(parse_datetime('%d/%m/%Y', data_propositura) as date) data_propositura,
    safe_cast(
        parse_datetime('%d/%m/%Y %H:%M:%S', data_cadastro) as date
    ) as data_cadastro,
    case
        when gabinete_de_desembargador_estadual is not null
        then "Estadual"
        when gabinete_de_desembargador_federal is not null
        then "Federal"
        else null
    end as esfera_gabinete_desembargador,
    safe_cast(
        coalesce(
            gabinete_de_desembargador_estadual, gabinete_de_desembargador_federal
        ) as string
    ) as gabinete_desembargador,
    safe_cast(secao_judiciaria as string) secao_judiciaria,
    safe_cast(coalesce(subsecao, subsecao_1) as string) as subsecao_1,
    safe_cast(subsecao_2 as string) as subsecao_2,
    case
        when
            coalesce(
                varas_e_juizados_federais,
                varas_e_juizados_federais_1,
                varas_e_juizados_federais_2
            )
            is not null
        then "Federal"
        when
            coalesce(
                varas_e_juizados_estaduais,
                varas_e_juizados_estaduais_1,
                varas_e_juizados_estaduais_2
            )
            is not null
        then "Estadual"
        else null
    end as esfera_vara_juizado,
    coalesce(
        varas_e_juizados_federais,
        varas_e_juizados_federais_1,
        varas_e_juizados_estaduais,
        varas_e_juizados_estaduais_1
    ) as vara_juizados_1,
    coalesce(
        varas_e_juizados_federais_2, varas_e_juizados_estaduais_2
    ) as vara_juizados_2,
    safe_cast(auditoria_militar as string) auditoria_militar,
    safe_cast(
        parse_datetime('%d/%m/%Y', data_do_transito_em_julgado) as date
    ) as data_pena,
    safe_cast(if(inelegibilidade = "SIM", true, false) as bool) teve_inelegivel,
    safe_cast(
        if(
            pagamento_de_multa is not null,
            contains_substr(pagamento_de_multa, "SIM"),
            false
        ) as bool
    ) as teve_multa,
    safe_cast(
        replace(
            replace(substr(pagamento_de_multa, length("SIM Valor R$ ")), ".", ""),
            ",",
            "."
        ) as float64
    ) as valor_multa,
    safe_cast(
        if(
            pena_privativa_de_liberdade is not null,
            contains_substr(pena_privativa_de_liberdade, "SIM"),
            false
        ) as bool
    ) teve_pena,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                pena_privativa_de_liberdade, r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_pena,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                pena_privativa_de_liberdade, r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_pena,
    safe_cast(
        regexp_extract(
            pena_privativa_de_liberdade, r'Anos: [^ ]+ Meses: [^ ]+ Dias: [^ ]+'
        ) as string
    ) as duracao_pena,
    safe_cast(
        if(
            perda_de_bens_ou_valores_acrescidos_ilicitamente_ao_patrimonio is not null,
            contains_substr(
                perda_de_bens_ou_valores_acrescidos_ilicitamente_ao_patrimonio, "SIM"
            ),
            false
        ) as bool
    ) teve_perda_bens,
    safe_cast(
        replace(
            replace(
                substr(
                    perda_de_bens_ou_valores_acrescidos_ilicitamente_ao_patrimonio,
                    length("SIM Valor R$ ")
                ),
                ".",
                ""
            ),
            ",",
            "."
        ) as float64
    ) as valor_perda_bens,
    safe_cast(
        if(perda_de_emprego_cargo_funcao_publica = "SIM", true, false) as bool
    ) teve_perda_cargo,
    safe_cast(
        if(
            proibicao_de_contratar_com_o_poder_publico_ou_receber_incentivos_fiscais_ou_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario
            = "SIM",
            true,
            false
        ) as bool
    ) proibicao,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_contratar_com_o_poder_publico_ou_receber_incentivos_fiscais_ou_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_proibicao,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_contratar_com_o_poder_publico_ou_receber_incentivos_fiscais_ou_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_proibicao,

    safe_cast(
        if(
            proibicao_de_contratar_com_o_poder_publico__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario
            = "SIM",
            true,
            false
        ) as bool
    ) proibicao_contratar_poder_publico,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_contratar_com_o_poder_publico__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_proibicao_contratar_poder_publico,
    -- safe_cast(fim_pena as date) fim_pena,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_contratar_com_o_poder_publico__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_proibicao_contratar_poder_publico,

    safe_cast(
        if(
            proibicao_de_receber_incentivos_fiscais__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario
            = "SIM",
            true,
            false
        ) as bool
    ) proibicao_receber_incentivos_fiscais,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_receber_incentivos_fiscais__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_proibicao_receber_incentivos_fiscais,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_receber_incentivos_fiscais__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_proibicao_receber_incentivos_fiscais,

    safe_cast(
        if(
            proibicao_de_receber_incentivos_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario
            = "SIM",
            true,
            false
        ) as bool
    ) proibicao_receber_incentivos_crediticios,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_receber_incentivos_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_proibicao_receber_incentivos_crediticios,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                proibicao_de_receber_incentivos_crediticios__direta_ou_indiretamente__ainda_que_por_intermedio_de_pessoa_juridica_da_qual_seja_socio_majoritario,
                r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_proibicao_receber_incentivos_crediticios,

    safe_cast(
        contains_substr(ressarcimento_integral_do_dano, "SIM") as bool
    ) teve_ressarcimento,
    safe_cast(
        replace(
            replace(
                substr(ressarcimento_integral_do_dano, length("SIM Valor R$ ")), ".", ""
            ),
            ",",
            "."
        ) as float64
    ) as valor_ressarcimento,
    safe_cast(
        if(
            ressarcimento_integral_do_dano is not null,
            contains_substr(ressarcimento_integral_do_dano, "SIM"),
            false
        ) as bool
    ) teve_suspensao,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                ressarcimento_integral_do_dano, r'De:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as inicio_suspensao,
    safe_cast(
        parse_datetime(
            "%d/%m/%Y",
            regexp_extract(
                ressarcimento_integral_do_dano, r'Até:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})'
            )
        ) as date
    ) as fim_suspensao,
    safe_cast(
        if(
            suspensao_dos_direitos_politicos is not null,
            contains_substr(
                suspensao_dos_direitos_politicos, "Comunicação à Justiça Eleitoral SIM"
            ),
            false
        ) as bool
    ) comunicado_tse,
    safe_cast(situacao as string) situacao,
    safe_cast(
        if(tipo_julgamento = "J", "Trânsito em julgado", "Órgão colegiado") as string
    ) tipo_julgamento,
    safe_cast(assunto_1 as string) assunto_1,
    safe_cast(assunto_2 as string) assunto_2,
    safe_cast(assunto_3 as string) assunto_3,
    safe_cast(assunto_4 as string) assunto_4,
    safe_cast(assunto_5 as string) assunto_5,
from {{ set_datalake_project("br_cnj_improbidade_administrativa_staging.condenacao") }}
