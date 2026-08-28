{{ config(materialized="ephemeral") }}

-- Pernambuco execution, 2008-2010 -- the legacy e-Fisco export.
--
-- PE changed its export schema twice, and the column names share almost nothing
-- across the
-- three eras:
--
-- 2008        40 cols  numero_empenho,      razao_social,         valor_empenhado
-- 2009-2010   41-47    numero_empenho_ne, _13_02_razao_social, empenhado
-- 2011-2026   22       numero_empenho,        credor,                 vlrempenhado
--
-- Modelling only the modern names leaves 1,031,326 rows -- 2008 through 2010, 21% of
-- PE --
-- present in the table and entirely NULL, with no error anywhere. The two legacy eras
-- are
-- similar enough in content to share one model, so each field coalesces the two
-- spellings.
--
-- The eras are also SEPARATE STAGING TABLES, decided by the header rather than the
-- year. A
-- wildcard parquet load infers one schema, so mixing them made BigQuery keep the
-- modern 22
-- columns and load the legacy rows as all-NULL, while still reporting the full row
-- count.
--
-- The legacy era is RICHER than the modern one in one respect that matters: it
-- publishes
-- `Data de geração do empenho` and a month. So PE has a real date for 2008-2010 and
-- none
-- from 2011, which is the opposite of what the modern schema alone suggests.
--
-- Column order must match br_bd_execucao_estadual__despesa_mg exactly: the parent union
-- resolves positionally.
--
-- The source headings are normalised to BigQuery-legal names at load time, so
-- "Numero Empenho (NE)" is read here as numero_empenho_ne and "13.02 - Razao Social" as
-- _13_02_razao_social. BigQuery rejects a '.' in a parquet field name outright and will
-- not accept a name starting with a digit, so the renaming is not cosmetic.
with
    fonte as (
        select
            *,
            coalesce(numero_empenho, numero_empenho_ne) as nr_empenho,
            coalesce(data_de_geracao_do_empenho, data_geracao_empenho) as dt_empenho,
            coalesce(no_do_mes, mes) as nr_mes,
            coalesce(cpf_cnpj_ig, cgc_cpf_ig) as doc_credor,
            coalesce(razao_social, _13_02_razao_social) as nm_credor,
            coalesce(cod_da_funcao, cod_funcao) as cd_funcao,
            coalesce(cod_da_subfuncao, cod_subfuncao) as cd_subfuncao,
            coalesce(valor_empenhado, empenhado) as vl_empenhado,
            coalesce(valor_liquidado, liquidado) as vl_liquidado,
            coalesce(valor_pago, pago) as vl_pago
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.pe_despesa_legado"
                )
            }}
    )

select
    safe_cast(ano as int64) as ano,
    safe_cast(nr_mes as int64) as mes,
    -- The legacy export writes "01/02/2008 00:00:00" and variants; only the date half
    -- carries information.
    safe.parse_date('%d/%m/%Y', left(dt_empenho, 10)) as data,
    'PE' as sigla_uf,
    safe_cast(cod_orgao as string) as orgao,
    safe_cast(coalesce(nome_orgao, orgao) as string) as nome_orgao,
    safe_cast(
        coalesce(cod_unidade_gestora, cod_ug_geral) as string
    ) as id_unidade_gestora,
    safe_cast(
        coalesce(nome_unidade_gestora, unidade_gestora_geral) as string
    ) as nome_unidade_gestora,
    concat('PE', ano, '-', nr_empenho) as id_empenho_bd,
    cast(null as string) as id_empenho,
    safe_cast(nr_empenho as string) as numero_empenho,
    safe_cast(modalidade_empenho as string) as tipo_empenho,
    cast(null as string) as descricao,
    cast(null as string) as id_licitacao_bd,
    cast(null as string) as id_licitacao,
    safe_cast(
        nullif(
            trim(
                regexp_replace(coalesce(modalidade_licitacao, licitacao), r'\s+', ' ')
            ),
            ''
        ) as string
    ) as modalidade_licitacao,
    safe_cast(nullif(doc_credor, '') as string) as documento_credor,
    safe_cast(nullif(nm_credor, '') as string) as nome_credor,
    cast(null as string) as tipo_documento_credor,
    safe_cast(cd_funcao as string) as funcao,
    safe_cast(cd_subfuncao as string) as subfuncao,
    safe_cast(cod_programa as string) as programa,
    safe_cast(cod_acao as string) as acao,
    safe_cast(cod_categoria_economica as string) as categoria_economica,
    safe_cast(cod_grupo_despesa as string) as grupo_despesa,
    safe_cast(cod_modalidade_aplicacao as string) as modalidade_aplicacao,
    safe_cast(cod_elemento_despesa as string) as elemento_despesa,
    safe_cast(cod_subacao as string) as item_despesa,
    safe_cast(cod_fonte_recursos as string) as fonte_recurso,
    cast(null as string) as tipo_documento,
    -- Legacy files use the same US number format as the modern ones.
    safe_cast(trim(vl_empenhado) as float64) as valor_empenhado,
    safe_cast(trim(vl_liquidado) as float64) as valor_liquidado,
    safe_cast(trim(vl_pago) as float64) as valor_pago
from fonte
