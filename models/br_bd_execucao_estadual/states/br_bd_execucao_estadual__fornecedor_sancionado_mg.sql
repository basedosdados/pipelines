{{ config(materialized="ephemeral") }}

-- Minas Gerais supplier sanctions, mapped onto the canonical `fornecedor_sancionado`
-- schema. Two registries, unioned, distinguished by `origem`.
--
-- 1. CAFIMP (`portal_cafimp`), 2,087 rows: the register of suppliers barred from
-- contracting with the state -- the penalty, its legal basis and the window it runs
-- for. `origem = 'cafimp'`.
-- 2. Administrative sanction proceedings (`portal_empresas_sancionadas`), 54 rows: the
-- CGE's own findings against firms, with the conduct alleged, the decision and the
-- fine. `origem = 'processo_administrativo'`.
--
-- WHAT THIS ADDS. The enforcement margin, which nothing else in the dataset observes.
-- It
-- makes the obvious question askable: does being sanctioned actually keep a firm out of
-- later contracts? Join `documento_fornecedor` to `contrato.documento_contratado`,
-- `licitacao_participante.documento` or `nota_fiscal.documento_emissor` -- all unmasked
-- for MG now -- and compare activity before and after `data_inicio`.
--
-- THE TWO REGISTRIES ARE VERY DIFFERENT SIZES AND THAT IS THE SOURCE, not a load
-- failure.
-- 2,087 against 54. CAFIMP accumulates every impediment since 2006; the sanction-
-- proceedings file covers a far smaller set of formal administrative cases. Do not read
-- the 54 as "all sanctions in MG", and do not sum the two as if they were one register:
-- a firm can appear in both for the same episode.
--
-- COLUMN NAMES CAME FROM THE REAL CSV HEADERS, not the repos' datapackage.json, which
-- is
-- wrong for both files. CAFIMP actually publishes `ds_tipo_penalidade`, `sansao` (sic),
-- `dt_inicio`, `dt_fim`, `dt_registro` and `num_processo`; the schema claims
-- `dstipo_penalidade`, `motivo`, `sancao`, `dtinicio` and no process column at all. The
-- sanctions file actually publishes `empresas_processadas`, `conduta`,
-- `data_publicacao_decisao` and `valor_multa_aplicada`; the schema claims
-- `empresa_processada`, `conuta` (sic), `data_decisao` and `valor_multa`.
--
-- DOCUMENT FORMATS DIFFER BETWEEN THE TWO and are normalised to digits here: CAFIMP
-- writes `05608786000148`, the sanctions file `11.753.865/0001-45`. Without this the
-- two
-- halves would not join to each other, let alone to the procurement tables.
--
-- `cd_orgao` ARRIVES FLOAT-FORMATTED in CAFIMP -- `1260,0`, not `1260` -- because the
-- upstream spreadsheet typed the agency code as a number. The trailing `,0` is
-- stripped so
-- the code matches `contrato.id_unidade_gestora` and friends.
--
-- KNOWN SOURCE DEFECT, handled: one CAFIMP row carries `dt_publicacao = 2049-06-14`
-- against a penalty window of 2018-12-12 to 2023-12-11 -- a typo for 2019. `ano` falls
-- back to `data_inicio` for it; `data_publicacao` keeps the published value.
--
-- Fields only one registry has are explicit NULLs in the other: CAFIMP has no conduct,
-- fine or procedural stage; the sanctions file has no penalty window and no agency
-- code,
-- only agency names.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field.
with
    cafimp as (
        select
            safe.parse_date(
                '%Y-%m-%d', substr(trim(dt_publicacao), 1, 10)
            ) as data_publicacao,
            'cafimp' as origem,
            nullif(
                regexp_replace(coalesce(cnpj_cpf_fornecedor, ''), r'[^0-9]', ''), ''
            ) as documento_fornecedor,
            nullif(trim(nome), '') as nome_fornecedor,
            cast(null as string) as tipo_societario,
            nullif(trim(ds_tipo_penalidade), '') as tipo_penalidade,
            nullif(trim(sansao), '') as sancao,
            cast(null as string) as conduta,
            cast(null as string) as fase,
            safe.parse_date('%Y-%m-%d', substr(trim(dt_inicio), 1, 10)) as data_inicio,
            safe.parse_date('%Y-%m-%d', substr(trim(dt_fim), 1, 10)) as data_fim,
            -- `1260,0` -> `1260`.
            nullif(
                regexp_extract(trim(cd_orgao), r'^([0-9]+)'), ''
            ) as id_unidade_gestora,
            nullif(trim(nome_orgao), '') as nome_unidade_gestora,
            cast(null as string) as nome_orgao_lesado,
            cast(null as float64) as valor_multa,
            nullif(trim(num_processo), '') as processo
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_fornecedor_impedido"
                )
            }}
    ),
    sancionada as (
        select
            coalesce(
                safe.parse_date(
                    '%Y-%m-%d', substr(trim(data_publicacao_decisao), 1, 10)
                ),
                safe.parse_date(
                    '%Y-%m-%d', substr(trim(data_publicacao_portaria), 1, 10)
                )
            ) as data_publicacao,
            'processo_administrativo' as origem,
            nullif(
                regexp_replace(coalesce(cnpj, ''), r'[^0-9]', ''), ''
            ) as documento_fornecedor,
            nullif(trim(empresas_processadas), '') as nome_fornecedor,
            nullif(trim(tipo_societario), '') as tipo_societario,
            cast(null as string) as tipo_penalidade,
            nullif(trim(decisao), '') as sancao,
            nullif(trim(conduta), '') as conduta,
            nullif(trim(fase), '') as fase,
            cast(null as date) as data_inicio,
            cast(null as date) as data_fim,
            cast(null as string) as id_unidade_gestora,
            nullif(trim(orgao_instaurador), '') as nome_unidade_gestora,
            nullif(trim(orgao_lesado), '') as nome_orgao_lesado,
            safe_cast(
                replace(replace(valor_multa_aplicada, '.', ''), ',', '.') as float64
            ) as valor_multa,
            nullif(trim(sei), '') as processo
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_empresa_sancionada"
                )
            }}
    ),
    base as (
        select *
        from cafimp
        union all
        select *
        from sancionada
    )
select
    -- One CAFIMP row is published `2049-06-14`, a typo: its penalty window is
    -- 2018-12-12 to 2023-12-11. Rather than leave the row unpartitioned, the year falls
    -- back to `data_inicio` when the publication date is outside a plausible range. The
    -- published `data_publicacao` is left exactly as the source has it.
    coalesce(
        case
            when extract(year from data_publicacao) between 1990 and 2030
            then extract(year from data_publicacao)
        end,
        case
            when extract(year from data_inicio) between 1990 and 2030
            then extract(year from data_inicio)
        end
    ) as ano,
    'MG' as sigla_uf,
    concat(
        'MG-',
        origem,
        '-',
        coalesce(documento_fornecedor, 'SEMDOC'),
        '-',
        row_number() over (
            partition by origem, documento_fornecedor
            order by data_publicacao, sancao, processo, nome_fornecedor
        )
    ) as id_sancao_bd,
    origem,
    documento_fornecedor,
    nome_fornecedor,
    tipo_societario,
    tipo_penalidade,
    sancao,
    conduta,
    fase,
    data_publicacao,
    data_inicio,
    data_fim,
    id_unidade_gestora,
    nome_unidade_gestora,
    nome_orgao_lesado,
    valor_multa,
    processo
from base
