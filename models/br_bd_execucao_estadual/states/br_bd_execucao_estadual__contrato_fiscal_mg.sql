{{ config(materialized="ephemeral") }}

-- Minas Gerais contract oversight officers, mapped onto the canonical `contrato_fiscal`
-- schema.
--
-- Source: `portal_fiscais_contratos/fiscais_contratos_<ano>.csv`
-- (github.com/transparencia-mg), 2022-2026, 17,629 contracts. Nothing to union: no
-- other
-- state publishes who supervises a contract, and the `compras_contratos` dimensional
-- model does not either.
--
-- WHAT THIS ADDS. The first table in this dataset whose grain is a PERSON rather than
-- an
-- organisation or a document: the named public servants designated to manage and
-- inspect
-- each contract. It makes officer-level questions askable at all -- workload, rotation,
-- whether oversight assignment correlates with contract outcomes.
--
-- THE SOURCE PUBLISHES LISTS, NOT ROWS, so this model explodes them. Both
-- `gestores_do_contrato_portal_de_compras` and `fiscais_do_contrato` are
-- comma-separated names in a single cell, up to a dozen per contract:
-- 'JOAO FRANCISCO JUNIOR, JOEL CASTILHO FERREIRA'
-- One row per (contract, role, person) is the only grain that makes the table usable;
-- keeping the raw list would just move the parsing problem downstream to every user.
--
-- Splitting on comma is a judgement call with a known failure mode: a name containing a
-- comma would split into two people. Brazilian personal names do not normally contain
-- commas and no such case was visible in the data, but this is the one assumption in
-- the
-- model that the source does not guarantee. It is not silent -- a spurious one-word
-- `nome` is the tell.
--
-- MASP, where the source gives it. 3,352 of 17,629 contracts (19%) write the
-- inspector's
-- civil-servant registration inline:
-- 'Kelly Cristina Nicolau - MASP 1444796-5'
-- That is a real person identifier and is extracted into its own column rather than
-- left
-- buried in the name. The other 81% have names only, so `masp` is sparse by source, not
-- by parsing failure. `gestores` never carries MASP.
--
-- `_x000D_` is stripped: 434 rows carry Excel's escaped carriage return inside the name
-- list, an artefact of the upstream spreadsheet export.
--
-- FOREIGN KEY. `id_contrato_bd` is built with the same coalesce as
-- `contrato_item_mg`, so
-- it agrees with `contrato`. Of the 17,629 contracts here, 11,986 are in the CKAN
-- dimension and 17,575 in the portal export; exactly 2 are in neither and therefore
-- have
-- no parent row in `contrato`. Those 2 are kept rather than dropped, so there is no
-- `relationships` test on this table -- the same reasoning as `nota_fiscal_item`.
-- Contrast
-- `contrato_item`, where the test passes with zero orphans and is enforced.
--
-- `orgaos_participantes` is also a comma-separated list, but of AGENCIES sharing the
-- contract, not people. It belongs to the contract rather than to an officer, so it is
-- deliberately left out of this table rather than repeated on every officer row.
--
-- Contract attributes the source repeats here -- objeto, situacao, tipo, the validity
-- dates, valor_inicial, valor_atual -- are NOT projected: they already live in
-- `contrato`,
-- and `contrato_mg` already draws `valor_inicial` from this same file. Repeating them
-- would create two places to disagree. `valor_inicial` here is also the field with the
-- known 2022 column-shift defect (160 rows of prose), documented in `contrato_mg.sql`.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field.
with
    contrato_key as (
        select
            trim(nr_contrato) as nr_contrato,
            min(safe_cast(id_contrato as string)) as id_contrato
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_contrato") }}
        where nullif(trim(nr_contrato), '') is not null
        group by 1
    ),
    portal_known as (
        select distinct nullif(trim(numero_contrato), '') as nr_contrato
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_contrato") }}
        where nullif(trim(numero_contrato), '') is not null
    ),
    base as (
        select
            nullif(trim(numero_do_contrato), '') as numero_contrato,
            nullif(trim(numero_do_processo_formatado), '') as numero_processo,
            nullif(trim(unidade_gestora_do_contrato), '') as id_unidade_gestora,
            safe.parse_date(
                '%Y-%m-%d', substr(trim(data_de_publicacao), 1, 10)
            ) as data_publicacao,
            gestores_do_contrato_portal_de_compras as lista_gestores,
            fiscais_do_contrato as lista_fiscais
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_contrato_fiscal"
                )
            }}
        where nullif(trim(numero_do_contrato), '') is not null
    ),
    exploded as (
        select
            b.numero_contrato,
            b.numero_processo,
            b.id_unidade_gestora,
            b.data_publicacao,
            p.papel,
            trim(regexp_replace(p.pessoa, r'_x000D_', '')) as pessoa
        from base as b
        cross join
            unnest(
                array_concat(
                    array(
                        select as struct 'gestor' as papel, x as pessoa
                        from unnest(split(coalesce(b.lista_gestores, ''), ',')) as x
                    ),
                    array(
                        select as struct 'fiscal' as papel, x as pessoa
                        from unnest(split(coalesce(b.lista_fiscais, ''), ',')) as x
                    )
                )
            ) as p
    ),
    parsed as (
        select
            numero_contrato,
            numero_processo,
            id_unidade_gestora,
            data_publicacao,
            papel,
            -- The registration number, where the source appends it to the name.
            nullif(
                regexp_extract(pessoa, r'(?i)-\s*masp\s*([0-9][0-9.\-]*)'), ''
            ) as masp,
            nullif(
                trim(
                    regexp_replace(pessoa, r'(?i)\s*-\s*masp\s*[0-9][0-9.\-]*\s*$', '')
                ),
                ''
            ) as nome
        from exploded
        where nullif(trim(pessoa), '') is not null
    )
select
    case
        when extract(year from data_publicacao) between 1990 and 2030
        then extract(year from data_publicacao)
    end as ano,
    'MG' as sigla_uf,
    coalesce(
        concat('MG-', k.id_contrato),
        case when pk.nr_contrato is not null then concat('MG-P-', p.numero_contrato) end
    ) as id_contrato_bd,
    concat(
        'MG-',
        p.numero_contrato,
        '-',
        p.papel,
        '-',
        row_number() over (
            partition by p.numero_contrato, p.papel order by p.nome, p.masp
        )
    ) as id_fiscal_bd,
    p.numero_contrato,
    p.numero_processo,
    p.id_unidade_gestora,
    p.papel,
    p.nome,
    p.masp
from parsed as p
left join contrato_key as k on p.numero_contrato = k.nr_contrato
left join portal_known as pk on p.numero_contrato = pk.nr_contrato
where p.nome is not null
