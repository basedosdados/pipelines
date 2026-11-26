{{
    config(
        alias="operacoes_fgi_tradicional",
        schema="br_bndes_fundos_garantidores",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2010, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf", "id_municipio"],
        labels={"project_id": "basedosdados"},
    )
}}

with
    staging as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(data_solicitacao_outorga as date) data_solicitacao_outorga,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(nome_municipio as string) nome_municipio,
            safe_cast(sigla_uf_sede_cliente as string) sigla_uf_sede_cliente,
            safe_cast(
                nome_municipio_sede_cliente as string
            ) nome_municipio_sede_cliente,
            safe_cast(cpf_cnpj_cliente as string) cpf_cnpj_cliente,
            safe_cast(nome_cliente as string) nome_cliente,
            safe_cast(porte_cliente as string) porte_cliente,
            safe_cast(valor_operacao as float64) valor_operacao,
            safe_cast(valor_garantido as float64) valor_garantido,
            safe_cast(valor_desembolsado as float64) valor_desembolsado,
            safe_cast(nome_agente_financeiro as string) nome_agente_financeiro
        from
            {{
                set_datalake_project(
                    "br_bndes_fundos_garantidores_staging.operacoes_fgi_tradicional"
                )
            }} as t
    ),

    municipio as (
        select
            id_municipio,
            sigla_uf,
            -- sem acento, apóstrofo ou hífen (ex.: Sant'Ana, Mogi-Guaçu)
            replace(
                replace(
                    regexp_replace(normalize(upper(nome), nfd), r'\pM', ''), "'", ''
                ),
                '-',
                ' '
            ) nome_norm
        from basedosdados.br_bd_diretorios_brasil.municipio
    ),

    -- grafias da fonte que não casam com o diretório
    correcao as (
        select *
        from
            unnest(
                [
                    struct(
                        'RJ' as sigla_uf,
                        'PARATI' as nome_norm,
                        '3303807' as id_municipio
                    ),
                    ('SC', 'BALNEARIO DE PICARRAS', '4212809'),
                    ('MG', 'ITABIRINHA DE MANTENA', '3131802'),
                    ('CE', 'ITAPAJE', '2306306'),
                    ('MA', 'LAGOA DOS RODRIGUES', '2105948'),
                    ('BA', 'MUQUEM DO SAO FRANCISCO', '2922250'),
                    ('PE', 'LAGOA DO ITAENGA', '2608503'),
                    ('TO', 'SAO VALERIO DA NATIVIDADE', '1720499'),
                    ('TO', 'COUTO DE MAGALHAES', '1706001'),
                    ('PE', 'BELEM DE SAO FRANCISCO', '2601607'),
                    ('MG', 'DONA EUZEBIA', '3122900'),
                    ('MT', 'POXOREU', '5107008'),
                    ('MG', 'AMPARO DA SERRA', '3102506'),
                    ('PE', 'ITAMARACA', '2607604'),
                    ('MG', 'BRASOPOLIS', '3108909'),
                    ('SC', 'PRESIDENTE CASTELO BRANCO', '4213906'),
                    ('SP', 'SAO LUIZ DO PARAITINGA', '3550001')
                ]
            )
    ),

    staging_norm as (
        select
            *,
            replace(
                replace(
                    regexp_replace(normalize(upper(nome_municipio), nfd), r'\pM', ''),
                    "'",
                    ''
                ),
                '-',
                ' '
            ) nome_norm,
            replace(
                replace(
                    regexp_replace(
                        normalize(upper(nome_municipio_sede_cliente), nfd), r'\pM', ''
                    ),
                    "'",
                    ''
                ),
                '-',
                ' '
            ) nome_norm_sede
        from staging
    )

select
    s.ano,
    s.data_solicitacao_outorga,
    s.sigla_uf,
    coalesce(m.id_municipio, c.id_municipio) id_municipio,
    s.sigla_uf_sede_cliente,
    coalesce(ms.id_municipio, cs.id_municipio) id_municipio_sede_cliente,
    s.cpf_cnpj_cliente,
    s.nome_cliente,
    s.porte_cliente,
    s.valor_operacao,
    s.valor_garantido,
    s.valor_desembolsado,
    s.nome_agente_financeiro
from staging_norm as s
left join municipio as m on s.sigla_uf = m.sigla_uf and s.nome_norm = m.nome_norm
left join correcao as c on s.sigla_uf = c.sigla_uf and s.nome_norm = c.nome_norm
left join
    municipio as ms
    on s.sigla_uf_sede_cliente = ms.sigla_uf
    and s.nome_norm_sede = ms.nome_norm
left join
    correcao as cs
    on s.sigla_uf_sede_cliente = cs.sigla_uf
    and s.nome_norm_sede = cs.nome_norm
