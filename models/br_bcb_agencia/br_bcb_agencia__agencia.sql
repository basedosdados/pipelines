{{
    config(
        alias="agencia",
        schema="br_bcb_agencia",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2025, "interval": 1},
        },
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}


with
    wrang_data as (
        select
            case
                when sigla_uf = 'SP' and nome = 'mogimirim'
                then '3530805'
                when sigla_uf = 'SP' and nome = 'mogiguacu'
                then '3530706'
                when sigla_uf = 'DF' and nome = 'brasilia ceilandia'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia brazlandia'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia sobradinho'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia samambaia'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia gama'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia taguatinga'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia guara'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia paranoa'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia nucleo bandeirante'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia cruzeiro'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia sudoesteoctogonal'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia aguas claras'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia planaltina'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia recanto das emas'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia santa maria'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia riacho fundo'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia sao sebastiao'
                then '5300108'
                when sigla_uf = 'DF' and nome = 'brasilia candangolandia'
                then '5300108'
                when sigla_uf = 'RJ' and nome = 'trajano de morais'
                then '3305901'
                when sigla_uf = 'RS' and nome = 'entre ijuis'
                then '4306932'
                when sigla_uf = 'MG' and nome = 'brasopolis'
                then '3108909'
                when sigla_uf = 'PR' and nome = 'santa cruz do monte castelo'
                then '4123303'
                when sigla_uf = 'PA' and nome = 'eldorado dos carajas'
                then '1502954'
                when sigla_uf = 'PE' and nome = 'belem de sao francisco'
                then '2601607'
                when sigla_uf = 'SC' and nome = 'sao lourenco doeste'
                then '4216909'
                when sigla_uf = 'MG' and nome = 'sao tome das letras'
                then '3165206'
                when sigla_uf = 'MG' and nome = 'dona euzebia'
                then '3122900'
                when sigla_uf = 'SC' and nome = 'picarras'
                then '4212809'
                when sigla_uf = 'SP' and nome = 'florinea'
                then '3516101'
                when sigla_uf = 'MA' and nome = 'pindare mirim'
                then '2108504'
                when sigla_uf = 'SC' and nome = 'presidente castelo branco'
                then '4120408'
                when sigla_uf = 'RO' and nome = 'alta floresta do oeste'
                then '1100015'
                when sigla_uf = 'PB' and nome = 'campo de santana'
                then '2516409'
                when sigla_uf = 'RN' and nome = 'augusto severo'
                then '2401305'
                when sigla_uf = 'SC' and nome = 'luis alves'
                then '4210001'
                when sigla_uf = 'SP' and nome = 'luisiania'
                then '3527702'
                when sigla_uf = 'RO' and nome = 'alvorada do oeste'
                then '1100346'
                when sigla_uf = 'RO' and nome = 'santa luzia do oeste'
                then '1100296'
                when sigla_uf = 'PE' and nome = 'itamaraca'
                then '2607604'
                when sigla_uf = 'RS' and nome = 'chiapeta'
                then '4305405'
                when sigla_uf = 'MG' and nome = 'itabirinha de mantena'
                then '3131802'
                when sigla_uf = 'MS' and nome = 'bataipora'
                then '3528502'
                when sigla_uf = 'SP' and nome = 'brodosqui'
                then '3507803'
                when sigla_uf = 'TO' and nome = 'paraiso do norte de goias'
                then '1716109'
                when sigla_uf = 'PE' and nome = 'cabo'
                then '2602902'
                when sigla_uf = 'TO' and nome = 'miracema do norte'
                then '1713205'
                when sigla_uf = 'RJ' and nome = 'pati do alferes'
                then '3303856'
                when sigla_uf = 'TO' and nome = 'colinas de goias'
                then '1705508'
                when sigla_uf = 'RN' and nome = 'assu'
                then '2400208'
                when sigla_uf = 'BA' and nome = 'camaca'
                then '2905602'
                when sigla_uf = 'SE' and nome = 'caninde do sao francisco'
                then '2801207'
                when sigla_uf = 'MT' and nome = 'quatro marcos'
                then '5107107'
                when sigla_uf = 'SP' and nome = 'ipaucu'
                then '3520905'
                when sigla_uf = 'MT' and nome = 'rio claro'
                then '3543907'
                when sigla_uf = 'SP' and nome = 'sud menucci'
                then '3552304'
                when sigla_uf = 'RS' and nome = 'eldorado'
                then '4306767'
                when sigla_uf = 'RS' and nome = 'portolandia'
                then '5218102'
                when sigla_uf = 'MG' and nome = 'gouvea'
                then '3127602'
                when sigla_uf = 'MG' and nome = 'sao joao da manteninha'
                then '3162575'
                when sigla_uf = 'MT' and nome = 'vila bela da sstrindade'
                then '5105507'
                when sigla_uf = 'SP' and nome = 'salmorao'
                then '3545100'
                when sigla_uf = 'MG' and nome = 'gouveia'
                then '3127602'
                when sigla_uf = 'MT' and nome = 'poxoreu'
                then '5107008'
                when sigla_uf = 'GO' and nome = 'portolandia'
                then '5218102'
                when sigla_uf = 'TO' and nome = 'alianca do norte'
                then '1700350'
                when sigla_uf = 'MA' and nome = 'sao luiz gonzaga maranhao'
                then '2111409'
                when sigla_uf = 'MG' and nome = 'cachoeira do pajeu'
                then '3102704'
                when sigla_uf = 'TO' and nome = 'divinopolis de goias'
                then '1707108'
                when sigla_uf = 'GO' and nome = 'cocalzinho'
                then '5205513'
                when sigla_uf = 'RO' and nome = 'sao francisco do guarope'
                then '1101492'
                when sigla_uf = 'PE' and nome = 'lagoa do itaenga'
                then '2608503'
                when sigla_uf = 'RJ' and nome = 'parati'
                then '3303807'
                when sigla_uf = 'SC' and nome = 'sao miguel doeste'
                then '4217204'
                when sigla_uf = 'PR' and nome = 'rosario'
                then '4122651'
                when sigla_uf = 'AM' and nome = 'careiro castanho'
                then '1301100'
                when sigla_uf = 'SP' and nome = 'embu'
                then '3515004'
                when sigla_uf = 'RO' and nome = 'nova brasilandia'
                then '1100148'
                when sigla_uf = 'GO' and nome = 'costelandia'
                then '5205059'
                else id_municipio
            end as id_municipio_fixed,
            case when length(cnpj) != 14 then null else cnpj end as cnpj1,
            lpad(cep, 8, '0') as cep1,
            nullif(sigla_uf, 'nan') as sigla_uf1,
            nullif(nome_agencia, 'nan') as nome_agencia1,
            nullif(instituicao, 'nan') as instituicao1,
            nullif(segmento, 'nan') as segmento1,
            nullif(id_compe_bcb_agencia, 'nan') as id_compe_bcb_agencia1,
            nullif(id_compe_bcb_instituicao, 'nan') as id_compe_bcb_instituicao1,
            nullif(endereco, 'nan') as endereco1,
            nullif(complemento, 'nan') as complemento1,
            nullif(bairro, 'nan') as bairro1,
            nullif(ddd, 'nan') as ddd1,
            nullif(fone, 'nan') as fone1,
            nullif(id_instalacao, 'nan') as id_instalacao1,
            data_inicio,
            ano,
            mes
        from {{ set_datalake_project("br_bcb_agencia_staging.agencia") }} as t
        -- os arquivos mensais possuem cabeçalhos e rodapés que variam de posição;
        -- Este filtro remove linhas com valores inteiramente
        -- nulos
        where fone != '00000nan'
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf1 as string) sigla_uf,
    safe_cast(nullif(id_municipio_fixed, 'nan') as string) id_municipio,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(cnpj1 as string) cnpj,
    safe_cast(nome_agencia1 as string) nome_agencia,
    safe_cast(instituicao1 as string) instituicao,
    safe_cast(segmento1 as string) segmento,
    safe_cast(id_compe_bcb_agencia1 as string) id_compe_bcb_agencia,
    safe_cast(id_compe_bcb_instituicao1 as string) id_compe_bcb_instituicao,
    case when regexp_contains(cep1, r'^0{8}$') then null else cep1 end as cep,
    safe_cast(endereco1 as string) endereco,
    safe_cast(complemento1 as string) complemento,
    safe_cast(bairro1 as string) bairro,
    safe_cast(ddd1 as string) ddd,
    safe_cast(fone1 as string) fone,
    safe_cast(id_instalacao1 as string) id_instalacao
from wrang_data
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
