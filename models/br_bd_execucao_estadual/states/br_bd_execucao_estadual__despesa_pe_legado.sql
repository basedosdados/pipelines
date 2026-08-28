{{ config(materialized="ephemeral") }}

-- Pernambuco execution, 2008-2010 -- the legacy e-Fisco export.
--
-- PE changed its export schema twice, and the column names share almost nothing
-- across the
-- three eras:
--
-- 2008        40 cols  "Numero Empenho",      "Razao Social",         "Valor Empenhado"
-- 2009-2010   41-47    "Numero Empenho (NE)", "13.02 - Razao Social", "Empenhado"
-- 2011-2026   22       numero_empenho,        credor,                 vlrempenhado
--
-- Modelling only the modern names leaves 1,031,326 rows -- 2008 through 2010, 21% of
-- PE --
-- present in the table and entirely NULL, with no error anywhere. The two legacy eras
-- are
-- similar enough in content to share one model, so each field coalesces the two
-- spellings.
--
-- The legacy era is RICHER than the modern one in one respect that matters: it
-- publishes
-- `Data de geração do empenho` and a month. So PE has a real date for 2008-2010 and
-- none
-- from 2011, which is the opposite of what the modern schema alone suggests.
--
-- Column order must match br_bd_execucao_estadual__despesa_mg exactly: the parent union
-- resolves positionally.
with
    fonte as (
        select
            *,
            coalesce("Numero Empenho", "Numero Empenho (NE)") as nr_empenho,
            coalesce(
                "Data de geração do empenho", "Data Geração Empenho"
            ) as dt_empenho,
            coalesce("No. do Mes", "Mes") as nr_mes,
            coalesce("CPF/CNPJ/IG", "CGC / CPF / IG") as doc_credor,
            coalesce("Razao Social", "13.02 - Razao Social") as nm_credor,
            coalesce("Cod. da função", "Cod. Função") as cd_funcao,
            coalesce("Cod. da subfunção", "Cod. Subfunção") as cd_subfuncao,
            coalesce("Valor Empenhado", "Empenhado") as vl_empenhado,
            coalesce("Valor Liquidado", "Liquidado") as vl_liquidado,
            coalesce("Valor Pago", "Pago") as vl_pago
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pe_despesa") }}
        -- The era is identified by the presence of a legacy column, not by a year
        -- range:
        -- a re-publication that changed the boundary would otherwise be silently
        -- miscut.
        where "Numero Empenho" is not null or "Numero Empenho (NE)" is not null
    )

select
    safe_cast(ano as int64) as ano,
    safe_cast(nr_mes as int64) as mes,
    -- The legacy export writes "01/02/2008 00:00:00" and variants; only the date half
    -- carries information.
    safe.parse_date('%d/%m/%Y', left(dt_empenho, 10)) as data,
    'PE' as sigla_uf,
    safe_cast("Cod. Orgao" as string) as orgao,
    safe_cast(coalesce("Nome Orgao", "Orgao") as string) as nome_orgao,
    safe_cast(
        coalesce("Cod. Unidade Gestora", "Cod. UG Geral") as string
    ) as id_unidade_gestora,
    safe_cast(
        coalesce("Nome Unidade Gestora", "Unidade Gestora Geral") as string
    ) as nome_unidade_gestora,
    concat('PE', ano, '-', nr_empenho) as id_empenho_bd,
    cast(null as string) as id_empenho,
    safe_cast(nr_empenho as string) as numero_empenho,
    safe_cast("Modalidade Empenho" as string) as tipo_empenho,
    cast(null as string) as descricao,
    cast(null as string) as id_licitacao_bd,
    cast(null as string) as id_licitacao,
    safe_cast(
        nullif(
            trim(
                regexp_replace(
                    coalesce("Modalidade Licitacao", "Licitacao"), r'\s+', ' '
                )
            ),
            ''
        ) as string
    ) as modalidade_licitacao,
    safe_cast(nullif(doc_credor, '') as string) as documento_credor,
    safe_cast(nullif(nm_credor, '') as string) as nome_credor,
    cast(null as string) as tipo_documento_credor,
    safe_cast(cd_funcao as string) as funcao,
    safe_cast(cd_subfuncao as string) as subfuncao,
    safe_cast("Cod. Programa" as string) as programa,
    safe_cast("Cod. Acao" as string) as acao,
    safe_cast("Cod. Categoria Economica" as string) as categoria_economica,
    safe_cast("Cod. Grupo Despesa" as string) as grupo_despesa,
    safe_cast("Cod. Modalidade Aplicacao" as string) as modalidade_aplicacao,
    safe_cast("Cod. Elemento Despesa" as string) as elemento_despesa,
    safe_cast("Cod. Subação" as string) as item_despesa,
    safe_cast("Cod. Fonte Recursos" as string) as fonte_recurso,
    cast(null as string) as tipo_documento,
    -- Legacy files use the same US number format as the modern ones.
    safe_cast(trim(vl_empenhado) as float64) as valor_empenhado,
    safe_cast(trim(vl_liquidado) as float64) as valor_liquidado,
    safe_cast(trim(vl_pago) as float64) as valor_pago
from fonte
