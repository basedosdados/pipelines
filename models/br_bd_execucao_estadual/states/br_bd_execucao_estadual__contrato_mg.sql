{{ config(materialized="ephemeral") }}

-- Minas Gerais contracts, mapped onto the canonical `contrato` schema.
--
-- Two sources, deliberately combined:
--
-- 1. SIAD/MG via the `compras_contratos` dimensional model on dados.mg.gov.br --
-- `mg_dm_contrato` (the contract dimension) with `mg_ft_compras_contrato` for the
-- foreign keys. This is the full history and the row source.
-- 2. `portal_contratos` from github.com/transparencia-mg, 2022-2026, for two things
-- the dimensional model cannot give: the signature date, and the REAL identity of
-- the counterparty.
--
-- WHY SOURCE 2 IS NOT OPTIONAL. `mg_dm_contratado` publishes only
-- `nr_documento_anonimizado` and `nome_anonimizado` -- MG's open procurement model
-- masks
-- its counterparties. The existing MG models pass that mask straight through
-- (`licitacao_item_mg.sql` documento_vencedor/nome_vencedor, `despesa_mg.sql`
-- documento_credor/nome_credor), so MG rows in those tables support no supplier-level
-- analysis at all. `portal_contratos` publishes the same contracts with unmasked CNPJ
-- and company name -- verified on contratos2024.csv: 5,333 contracts, 2,416 distinct
-- suppliers, zero masking markers. A contracts table whose contratado is anonymised
-- would be close to useless, so the join is part of the definition, not an enrichment.
--
-- COVERAGE ASYMMETRY, and why CKAN drives. `portal_contratos` carries only contracts
-- with a formal contract instrument -- `indicador_termo_de_contrato` is 'SIM' on every
-- one of the 5,333 rows of contratos2024.csv -- and starts in 2022. It is therefore a
-- subset, and is joined, never unioned. Contracts before 2022, and contracts with no
-- termo de contrato, keep a NULL `data_assinatura` and the anonymised contratado. That
-- is visible in the data rather than silently imputed; `nome_contratado` beginning with
-- the mask pattern is the tell.
--
-- JOIN KEY. `numero_contrato` is a 7-digit SIAD contract identifier, globally unique in
-- the source (5,333 distinct in 5,333 rows) and all-numeric. It joins to
-- `mg_dm_contrato.nr_contrato` directly. `numero_processo` is carried for
-- cross-checking but deliberately NOT part of the join: the two sources format it
-- differently and a composite join would silently drop rows.
--
-- GRAIN. One row per contract, guaranteed by driving from `mg_dm_contrato`, which is a
-- dimension. `mg_ft_compras_contrato` is the fact and is keyed
-- (id_tempo, id_processo, id_orgao_contrato, id_contrato, id_contratado,
-- id_situacao_cont), so a contract spanning more than one process or appearing in more
-- than one period has several fact rows. Joining the fact directly would fan the table
-- out, so it is collapsed to one row per id_contrato first. Where a contract does span
-- several processes only the lowest id_processo is kept, and `numero_processo` should
-- be
-- read as "a process this contract belongs to", not "the" process.
--
-- `modalidade` is MG's contracting route from `mg_dm_processo.procedimento` (PREGAO,
-- INEXIGIBILIDADE, DISPENSA, ...), passed through verbatim rather than recoded onto the
-- Lei 8.666 modality numbers -- same reasoning as `licitacao_mg.sql`: a numeric recode
-- corrupts the column whenever the source's numbering disagrees with the target's, and
-- the label is unambiguous on its own.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field. Columns the source does not publish are explicit typed NULLs.
with
    -- One row per contract from the fact, for the foreign keys only. The fact's own
    -- vr_atualizado is summed: when a contract spans several processes the per-process
    -- amounts are parts of one contract value.
    fato as (
        select
            id_contrato,
            min(id_processo) as id_processo,
            min(id_orgao_contrato) as id_orgao_contrato,
            min(id_contratado) as id_contratado,
            min(id_situacao_cont) as id_situacao_cont,
            sum(safe_cast(vr_atualizado as float64)) as vr_atualizado_fato
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_ft_compras_contrato"
                )
            }}
        where id_contrato is not null
        group by id_contrato
    ),
    -- Real counterparty identity and signature date, one row per contract.
    portal as (
        select nr_contrato, dt_assin, documento_contratado, nome_contratado
        from
            (
                select
                    nullif(trim(numero_contrato), '') as nr_contrato,
                    safe.parse_date(
                        '%Y-%m-%d', substr(trim(data_assinatura_contrato), 1, 10)
                    ) as dt_assin,
                    nullif(
                        regexp_replace(
                            coalesce(cnpj_cpf_fornecedor_formatado, ''), r'[^0-9]', ''
                        ),
                        ''
                    ) as documento_contratado,
                    nullif(
                        trim(nome_empresarial_nome_fornecedor), ''
                    ) as nome_contratado,
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
    base as (
        select
            safe_cast(c.nr_contrato as string) as numero_contrato,
            safe_cast(p.cd_processo_formatado as string) as numero_processo,
            safe_cast(oc.cd_orgao_contrato as string) as id_unidade_gestora,
            safe_cast(oc.nome as string) as nome_unidade_gestora,
            safe_cast(c.objeto as string) as objeto,
            safe_cast(p.procedimento as string) as modalidade,
            safe_cast(c.tipo as string) as tipo_contrato,
            -- Unmasked where portal_contratos covers the contract, anonymised
            -- otherwise.
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
            -- The amended end date when the contract has one, else the original.
            coalesce(
                safe_cast(c.dt_fim_vigencia_atual as date),
                safe_cast(c.dt_fim_vigencia as date)
            ) as data_fim_vigencia,
            safe_cast(c.vr_homologado as float64) as valor_inicial,
            coalesce(
                safe_cast(c.vr_atualizado as float64), f.vr_atualizado_fato
            ) as valor_atual,
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
