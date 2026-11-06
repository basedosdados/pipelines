{{ config(materialized="ephemeral") }}

-- Minas Gerais invoice line items, mapped onto the canonical `nota_fiscal_item` schema.
--
-- Source: `portal_notas_fiscais/itensnota_<mes><aa>.csv`, 57 monthly files, 2022-01 to
-- 2026-09, 1,983,725 rows. The finest-grained thing in this dataset: an individual
-- catalogued product on an individual invoice, with the quantity delivered and the unit
-- price actually charged.
--
-- `id_nota_fiscal_bd` is built from EXACTLY the same eight columns, in the same
-- order, as
-- `nota_fiscal_mg` builds it from -- orgao_emissor, tipo_de_documento,
-- fornecedor_cnpj_cpf, numero, serie, sequencial, natureza, indicador_de_orcamento.
-- Those
-- eight are the columns the two files share, and they are the only link between them.
-- If either construction changes, change both or the tables stop joining.
--
-- 1,458 of the 1,342,543 distinct invoice keys present here (0.11%) have no matching
-- row
-- in the invoice file. So there is deliberately NO `relationships` test from this
-- table to
-- `nota_fiscal`: it would fail, and the honest representation is an orphan rate
-- stated in
-- the docs rather than a test that has to be disabled. Filter on a successful join to
-- `nota_fiscal` if you need guaranteed parents.
--
-- `ano` / `mes` come from the source filename, because these rows carry NO date
-- column at
-- all -- not emission, not receipt, not registration. See `nota_fiscal_mg.sql` for why
-- both tables are partitioned this way rather than on a date.
--
-- GRAIN. (invoice, catalogued product, occurrence). `item_da_nota_fiscal` is the line
-- number on the invoice and is kept as `numero_item`, but the occurrence suffix is what
-- makes the id unique -- the same construction `licitacao_item_mg.sql` and
-- `contrato_item_mg.sql` use, for the same reason: a product can legitimately appear
-- more
-- than once on one document.
--
-- Values are comma-decimal with no thousands separator.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field.
with
    base as (
        select
            nullif(trim(orgao_emissor), '') as id_unidade_gestora,
            nullif(trim(tipo_de_documento), '') as tipo_documento,
            nullif(
                regexp_replace(coalesce(fornecedor_cnpj_cpf, ''), r'[^0-9]', ''), ''
            ) as documento_emissor,
            nullif(trim(fornecedor_nome_empresarial), '') as nome_emissor,
            nullif(trim(numero), '') as numero,
            nullif(trim(serie), '') as serie,
            nullif(trim(sequencial), '') as sequencial,
            nullif(trim(natureza), '') as natureza,
            nullif(trim(indicador_de_orcamento), '') as indicador_orcamento,
            nullif(trim(item_da_nota_fiscal), '') as numero_item,
            nullif(trim(codigo_do_item), '') as codigo_catalogo,
            nullif(trim(desc_do_item_de_material), '') as descricao,
            nullif(trim(desc_da_unid_de_fornecimento), '') as unidade_medida,
            nullif(trim(unid_orcamentaria), '') as unidade_orcamentaria,
            nullif(trim(elemento_item_de_despesa), '') as item_despesa,
            safe_cast(replace(qtde_na_nota_fiscal, ',', '.') as float64) as quantidade,
            safe_cast(replace(valor_unitario_r, ',', '.') as float64) as valor_unitario,
            safe_cast(replace(valor_total_r, ',', '.') as float64) as valor_total,
            -- The reference period, parsed from the source filename
            -- (`itensnota_jan22.csv`). Portuguese three-letter month, two-digit year.
            2000 + safe_cast(
                regexp_extract(arquivo_origem, r'([0-9]{2})\.csv$') as int64
            ) as ano,
            case
                regexp_extract(arquivo_origem, r'([a-z]{3})[0-9]{2}\.csv$')
                when 'jan'
                then 1
                when 'fev'
                then 2
                when 'mar'
                then 3
                when 'abr'
                then 4
                when 'mai'
                then 5
                when 'jun'
                then 6
                when 'jul'
                then 7
                when 'ago'
                then 8
                when 'set'
                then 9
                when 'out'
                then 10
                when 'nov'
                then 11
                when 'dez'
                then 12
            end as mes
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_nota_fiscal_item"
                )
            }}
        where nullif(trim(numero), '') is not null
    ),
    keyed as (
        select
            *,
            concat(
                'MG-',
                coalesce(id_unidade_gestora, ''),
                '-',
                coalesce(tipo_documento, ''),
                '-',
                coalesce(documento_emissor, ''),
                '-',
                coalesce(numero, ''),
                '-',
                coalesce(serie, ''),
                '-',
                coalesce(sequencial, ''),
                '-',
                coalesce(natureza, ''),
                '-',
                coalesce(indicador_orcamento, '')
            ) as id_nota_fiscal_bd
        from base
    )
select
    ano,
    mes,
    'MG' as sigla_uf,
    id_nota_fiscal_bd,
    concat(
        id_nota_fiscal_bd,
        '-',
        coalesce(codigo_catalogo, 'SEMCAT'),
        '-',
        row_number() over (
            partition by id_nota_fiscal_bd, codigo_catalogo
            order by numero_item, valor_total, quantidade
        )
    ) as id_item_bd,
    numero_item,
    codigo_catalogo,
    descricao,
    unidade_medida,
    unidade_orcamentaria,
    item_despesa,
    documento_emissor,
    nome_emissor,
    quantidade,
    valor_unitario,
    valor_total
from keyed
