{{ config(materialized="ephemeral") }}

-- Minas Gerais contracts, mapped onto the canonical `contrato` schema.
--
-- TWO SOURCES, AND A UNION BETWEEN THEM.
--
-- 1. SIAD/MG via the `compras_contratos` dimensional model on dados.mg.gov.br --
-- `mg_dm_contrato` (the contract dimension) with `mg_ft_compras_contrato` for the
-- foreign keys. The full history, back to 1996.
-- 2. `portal_contratos` from github.com/transparencia-mg, 2022-2026. Supplies the
-- signature date and the unmasked counterparty for contracts source 1 also has,
-- and is the ONLY source for contracts source 1 lacks.
--
-- WHY SOURCE 2 IS NOT OPTIONAL, part one: identity. `mg_dm_contratado` masks the
-- counterparty, and the mask is not uniform -- it covers all 20,770 natural persons
-- (tp_documento=1) and 2,639 of 64,836 companies (tp_documento=2). So company identity
-- was largely available already; what source 2 adds is every individual contractor plus
-- the withheld companies, about 28% of MG rows. The existing MG models pass the mask
-- through (`licitacao_item_mg.sql` documento_vencedor/nome_vencedor, `despesa_mg.sql`
-- documento_credor/nome_credor) without saying so anywhere.
--
-- WHY SOURCE 2 IS NOT OPTIONAL, part two: coverage. The CKAN extract in staging stops
-- at
-- dt_publicacao 2025-04-05 (and `mg_dm_processo` at 2024-03-19, which is why
-- `licitacao`
-- ends in 2024). Driving from CKAN alone dropped 8,427 of 25,468 portal contracts --
-- 33.1%, essentially all of 2025 H2 and 2026. Those are unioned in here rather than
-- lost. The proper fix is to refresh the CKAN extract, which also repairs `licitacao`,
-- `licitacao_item` and `despesa`; this union keeps `contrato` current until that
-- happens, and stays correct afterwards because membership is decided by an anti-join,
-- not by year.
--
-- The two branches are disjoint by construction: branch 2 selects exactly the portal
-- contracts absent from `mg_dm_contrato`. `numero_contrato` is a 7-digit SIAD
-- identifier, unique across all five year files (25,468 distinct in 25,468 rows),
-- all-numeric, no nulls, and it matches CKAN for 99.8% of 2022-2024 contracts (15,940
-- of 15,978) -- so the anti-join is reliable and cannot double-count. `numero_processo`
-- is deliberately NOT part of the key: the two sources format it differently.
--
-- Portal-only rows are nearly complete: the flat export carries 17 of the 18 canonical
-- columns on its own. The exception is `valor_inicial`, which comes from
-- `portal_fiscais_contratos` (unique on `numero_do_contrato`, so no fan-out), covering
-- 5,641 of the 8,427 portal-only contracts (66.9%). The rest are NULL.
--
-- GRAIN. One row per contract. Branch 1 drives from `mg_dm_contrato`, a dimension, so
-- the grain is guaranteed; `mg_ft_compras_contrato` is the fact, keyed
-- (id_tempo, id_processo, id_orgao_contrato, id_contrato, id_contratado,
-- id_situacao_cont), so a contract spanning several processes or periods has several
-- fact rows and is collapsed to one first. Where a contract spans several processes
-- only
-- the lowest id_processo is kept, and `numero_processo` should be read as "a process
-- this contract belongs to", not "the" process.
--
-- `modalidade` is MG's contracting route, passed through verbatim rather than recoded
-- onto the Lei 8.666 modality numbers -- same reasoning as `licitacao_mg.sql`: a
-- numeric
-- recode corrupts the column whenever the source's numbering disagrees with the
-- target's, and the label is unambiguous on its own. Branch 1 takes it from
-- `mg_dm_processo.procedimento`, branch 2 from
-- `procedimento_contratacao_especializacao`, the same vocabulary.
--
-- `numero_contrato` IS NOT UNIQUE, and that is the source, not a defect here.
-- `mg_dm_contrato` holds 91,035 rows over 68,884 distinct `nr_contrato`: 8,146 numbers
-- are reused, worst case 15 times, with objeto, dates and values all differing -- MG
-- reuses contract numbers across agencies and years. `id_contrato_bd` keeps them
-- distinct, which is why the uniqueness test is on (sigla_uf, id_contrato_bd) and not
-- on
-- the number; ES uses a row_number in its own id for the same reason.
--
-- This does NOT make the portal join ambiguous: of those 8,146 reused numbers, exactly
-- zero appear in `mg_contrato`, so no CKAN row receives identity that could belong to a
-- sibling. Measured, not assumed -- the portal covers only formalised 2022+ contracts,
-- a different population. Re-check this if the portal's coverage ever widens.
--
-- NUMERIC FORMATS DIFFER BETWEEN BRANCHES. CKAN values are dot-decimal (`0.00`); the
-- portal exports are comma-decimal (`12257730357,88`) with no thousands separator.
-- Branch 2 therefore replaces the comma before casting; branch 1 must not.
--
-- KNOWN SOURCE DEFECT, handled: 160 rows of `fiscais_contratos_2022.csv` carry prose in
-- `valor_inicial` -- objeto text shifted rightward, which is also why that one file has
-- an extra column. A numeric guard nulls them rather than casting garbage. The other
-- four year files are clean.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field. Columns a source does not publish are explicit typed NULLs.
with
    fato as (
        select
            id_contrato,
            min(id_processo) as id_processo,
            min(id_orgao_contrato) as id_orgao_contrato,
            min(id_contratado) as id_contratado,
            min(id_situacao_cont) as id_situacao_cont
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_ft_compras_contrato"
                )
            }}
        where id_contrato is not null
        group by id_contrato
    ),
    portal as (
        select *
        from
            (
                select
                    nullif(trim(numero_contrato), '') as nr_contrato,
                    safe.parse_date(
                        '%Y-%m-%d', substr(trim(data_assinatura_contrato), 1, 10)
                    ) as dt_assin,
                    safe.parse_date(
                        '%Y-%m-%d', substr(trim(data_inicio_vigencia_contrato), 1, 10)
                    ) as dt_ini,
                    safe.parse_date(
                        '%Y-%m-%d', substr(trim(data_termino_vigencia_contrato), 1, 10)
                    ) as dt_fim,
                    nullif(
                        regexp_replace(
                            coalesce(cnpj_cpf_fornecedor_formatado, ''), r'[^0-9]', ''
                        ),
                        ''
                    ) as documento_contratado,
                    nullif(
                        trim(nome_empresarial_nome_fornecedor), ''
                    ) as nome_contratado,
                    nullif(trim(numero_processo_formatado), '') as numero_processo,
                    nullif(
                        trim(codigo_orgao_entidade_contratante), ''
                    ) as id_unidade_gestora,
                    nullif(
                        trim(nome_orgao_entidade_contratante), ''
                    ) as nome_unidade_gestora,
                    nullif(trim(objeto_contrato), '') as objeto,
                    nullif(
                        trim(procedimento_contratacao_especializacao), ''
                    ) as modalidade,
                    nullif(trim(descricao_tipo_de_contrato), '') as tipo_contrato,
                    nullif(trim(situacao_contrato), '') as situacao,
                    safe_cast(
                        replace(valor_total_atualizado, ',', '.') as float64
                    ) as valor_atual,
                    row_number() over (
                        partition by nullif(trim(numero_contrato), '')
                        order by
                            safe_cast(ano_assinatura_contrato as int64) desc,
                            trim(data_assinatura_contrato) desc,
                            trim(nome_empresarial_nome_fornecedor)
                    ) as rn
                from
                    {{
                        set_datalake_project(
                            "br_bd_execucao_estadual_staging.mg_contrato"
                        )
                    }}
                where nullif(trim(numero_contrato), '') is not null
            )
        where rn = 1
    ),
    fiscal as (
        select nr_contrato, valor_inicial
        from
            (
                select
                    nullif(trim(numero_do_contrato), '') as nr_contrato,
                    safe_cast(
                        replace(
                            regexp_extract(
                                trim(valor_inicial), r'^[0-9]+(?:[.,][0-9]+)?$'
                            ),
                            ',',
                            '.'
                        ) as float64
                    ) as valor_inicial,
                    row_number() over (
                        partition by nullif(trim(numero_do_contrato), '')
                        order by trim(valor_inicial) desc
                    ) as rn
                from
                    {{
                        set_datalake_project(
                            "br_bd_execucao_estadual_staging.mg_contrato_fiscal"
                        )
                    }}
                where nullif(trim(numero_do_contrato), '') is not null
            )
        where rn = 1
    ),
    ckan as (
        select
            safe_cast(c.nr_contrato as string) as numero_contrato,
            safe_cast(p.cd_processo_formatado as string) as numero_processo,
            safe_cast(oc.cd_orgao_contrato as string) as id_unidade_gestora,
            safe_cast(oc.nome as string) as nome_unidade_gestora,
            safe_cast(c.objeto as string) as objeto,
            safe_cast(p.procedimento as string) as modalidade,
            safe_cast(c.tipo as string) as tipo_contrato,
            coalesce(
                pc.documento_contratado,
                safe_cast(ct.nr_documento_anonimizado as string)
            ) as documento_contratado,
            coalesce(
                pc.nome_contratado, safe_cast(ct.nome_anonimizado as string)
            ) as nome_contratado,
            coalesce(
                safe_cast(sc.nome as string), safe_cast(c.situacao as string)
            ) as situacao,
            pc.dt_assin as data_assinatura,
            safe_cast(c.dt_inicio_vigencia as date) as data_inicio_vigencia,
            coalesce(
                safe_cast(c.dt_fim_vigencia_atual as date),
                safe_cast(c.dt_fim_vigencia as date)
            ) as data_fim_vigencia,
            safe_cast(c.vr_homologado as float64) as valor_inicial,
            safe_cast(c.vr_atualizado as float64) as valor_atual,
            concat('MG-', safe_cast(c.id_contrato as string)) as id_contrato_bd,
            safe_cast(c.dt_publicacao as date) as dt_publicacao
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_contrato") }}
            as c
        left join fato as f on c.id_contrato = f.id_contrato
        left join
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_processo") }}
            as p
            on f.id_processo = p.id_processo
        left join
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_orgao_contrato"
                )
            }} as oc on f.id_orgao_contrato = oc.id_orgao_contrato
        left join
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_contratado"
                )
            }} as ct on f.id_contratado = ct.id_contratado
        left join
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_situacao_cont"
                )
            }} as sc on f.id_situacao_cont = sc.id_situacao_cont
        left join portal as pc on safe_cast(c.nr_contrato as string) = pc.nr_contrato
        where c.id_contrato is not null
    ),
    -- `MG-P-` marks the provenance and cannot collide with branch 1's
    -- `MG-<id_contrato>` (a small integer surrogate key) even once CKAN catches up.
    portal_only as (
        select
            pc.nr_contrato as numero_contrato,
            pc.numero_processo,
            pc.id_unidade_gestora,
            pc.nome_unidade_gestora,
            pc.objeto,
            pc.modalidade,
            pc.tipo_contrato,
            pc.documento_contratado,
            pc.nome_contratado,
            pc.situacao,
            pc.dt_assin as data_assinatura,
            pc.dt_ini as data_inicio_vigencia,
            pc.dt_fim as data_fim_vigencia,
            fi.valor_inicial,
            pc.valor_atual,
            concat('MG-P-', pc.nr_contrato) as id_contrato_bd,
            cast(null as date) as dt_publicacao
        from portal as pc
        left join fiscal as fi on pc.nr_contrato = fi.nr_contrato
        where
            not exists (
                select 1
                from
                    {{
                        set_datalake_project(
                            "br_bd_execucao_estadual_staging.mg_dm_contrato"
                        )
                    }} as k
                where safe_cast(k.nr_contrato as string) = pc.nr_contrato
            )
    ),
    base as (
        select *
        from ckan
        union all
        select *
        from portal_only
    )
select
    case
        when
            extract(
                year from coalesce(data_assinatura, dt_publicacao, data_inicio_vigencia)
            )
            between 1990 and 2030
        then
            extract(
                year from coalesce(data_assinatura, dt_publicacao, data_inicio_vigencia)
            )
    end as ano,
    'MG' as sigla_uf,
    id_contrato_bd,
    numero_contrato,
    numero_processo,
    id_unidade_gestora,
    nome_unidade_gestora,
    objeto,
    modalidade,
    tipo_contrato,
    documento_contratado,
    nome_contratado,
    situacao,
    data_assinatura,
    data_inicio_vigencia,
    data_fim_vigencia,
    valor_inicial,
    valor_atual
from base
where numero_contrato is not null
