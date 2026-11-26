-- nome de município sem acento, apóstrofo ou hífen (ex.: Sant'Ana, Mogi-Guaçu)
{% macro fundos_garantidores_nome_norm(coluna) %}
    replace(
        replace(
            regexp_replace(normalize(upper({{ coluna }}), nfd), r'\pM', ''), "'", ''
        ),
        '-',
        ' '
    )
{% endmacro %}

-- (sigla_uf, nome normalizado) -> id_municipio: diretório da BD mais as grafias da
-- fonte que não casam com ele; o diretório tem prioridade se as duas casarem
{% macro fundos_garantidores_municipio() %}
    select sigla_uf, nome_norm, id_municipio
    from
        (
            select
                sigla_uf,
                {{ fundos_garantidores_nome_norm("nome") }} nome_norm,
                id_municipio,
                1 prioridade
            from basedosdados.br_bd_diretorios_brasil.municipio
            union all
            select sigla_uf, nome_norm, id_municipio, 2 prioridade
            from
                unnest(
                    [
                        struct(
                            'BA' as sigla_uf,
                            'MUQUEM DO SAO FRANCISCO' as nome_norm,
                            '2922250' as id_municipio
                        ),
                        ('CE', 'ITAPAJE', '2306306'),
                        ('MA', 'LAGOA DOS RODRIGUES', '2105948'),
                        ('MG', 'AMPARO DA SERRA', '3102506'),
                        ('MG', 'BRASOPOLIS', '3108909'),
                        ('MG', 'DONA EUZEBIA', '3122900'),
                        ('MG', 'GOUVEA', '3127602'),
                        ('MG', 'ITABIRINHA DE MANTENA', '3131802'),
                        ('MG', 'SANTA RITA DO IBITIPOCA', '3159407'),
                        ('MG', 'SAO TOME DAS LETRAS', '3165206'),
                        ('MT', 'POXOREU', '5107008'),
                        ('PA', 'ELDORADO DOS CARAJAS', '1502954'),
                        ('PB', 'SANTAREM', '2513653'),
                        ('PB', 'SAO DOMINGOS DE POMBAL', '2513968'),
                        ('PE', 'BELEM DE SAO FRANCISCO', '2601607'),
                        ('PE', 'ITAMARACA', '2607604'),
                        ('PE', 'LAGOA DO ITAENGA', '2608503'),
                        ('PI', 'CAPITAO GERVASIO DE OLIVEIRA', '2202455'),
                        ('PI', 'NOSSA SENHORA DO NAZARE', '2206753'),
                        ('RJ', 'PARATI', '3303807'),
                        ('RN', 'AUGUSTO SEVERO', '2401305'),
                        ('RN', 'JANUARIO CICCO', '2405306'),
                        ('RN', 'VENHAR VER', '2414753'),
                        ('RO', 'ALTO ALEGRE DO PARECIS', '1100379'),
                        ('SC', 'BALNEARIO DE PICARRAS', '4212809'),
                        ('SC', 'PRESIDENTE CASTELO BRANCO', '4213906'),
                        ('SC', 'SAO MIGUEL DOESTE', '4217204'),
                        ('SP', 'SAO LUIZ DO PARAITINGA', '3550001'),
                        ('TO', 'COUTO DE MAGALHAES', '1706001'),
                        ('TO', 'SAO VALERIO DA NATIVIDADE', '1720499')
                    ]
                )
        )
    qualify row_number() over (partition by sigla_uf, nome_norm order by prioridade) = 1
{% endmacro %}
