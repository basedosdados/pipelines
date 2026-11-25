{{ config(materialized="ephemeral") }}

-- Paraíba execution, mapped directly onto the canonical `despesa` schema.
--
-- Source: CGE-PB's REST API (`/despesas/notas_empenho`), 2015-2026. See
-- code/download_pb.py.
--
-- **PB needs no pivot.** It is the only source here that publishes `valorEmpenhado`,
-- `valorLiquidado` and `valorPago` on the SAME empenho row, so it maps across the way
-- MG and ES do, not the way SC and RS do.
--
-- Grain is one row per empenho document. Measured over the whole of 2024 (320,201
-- rows), the key is (ano, órgão, unidade, número) and nothing shorter works:
--
-- numeroEmpenho                     40,626 distinct   fan-out x7.88
-- (ano, unidade, numero)           317,300 distinct   fan-out x1.01
-- (ano, orgao, unidade, numero)    320,201 distinct   UNIQUE
--
-- `descricaoTipo` splits the rows into PRINCIPAL (294,298) and three kinds of
-- modification -- ANULAÇÃO PARCIAL, ANULAÇÃO TOTAL, SUPLEMENTAÇÃO (25,903 together).
-- Modifications carry their own document number, share **no** key with any principal,
-- and carry zero `valorLiquidado` and zero `valorPago`.
--
-- **Every row is kept, and `valorDespesa` is the value used.** `valorDespesa` is
-- exactly `valorEmpenhado + valorAnulado` (21,948,418,213.27 - 439,415,531.48 =
-- 21,509,002,681.79 for 2024 principals), i.e. the net of the document's own
-- cancellation, so summing it double-counts nothing within a row.
--
-- ONE THING IS UNRESOLVED AND IS DELIBERATELY NOT DECIDED HERE. Across rows, keeping
-- the modification documents gives R$20.49bn for 2024 while keeping only the principals
-- gives R$21.51bn. The two cannot both be the net: either a principal's own
-- `valorAnulado` and the separate ANULAÇÃO document record the same cancellation twice,
-- or they record different ones (a cancellation of a prior-year empenho would be a
-- separate document with no principal in this exercise, which is consistent with the
-- zero key overlap). The source does not say which, and the difference is R$1.0bn.
-- Keeping every document is the choice that loses nothing and matches what this table
-- claims to be -- one row per empenho document -- but `validate_pb.py` reports both
-- figures so the question stays visible instead of being settled by silence.
with
    fonte as (
        select *
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.pb_empenho") }}
    ),
    base as (
        select
            safe_cast(ano as int64) as ano,
            safe_cast(mes as int64) as mes,
            safe.parse_date('%Y-%m-%d', substr(trim(dataempenho), 1, 10)) as data,
            trim(codigoorgao) as cod_orgao,
            nullif(trim(nomeorgao), '') as nome_orgao,
            trim(codigounidade) as cod_unidade,
            trim(numeroempenho) as numero_empenho,
            nullif(trim(tipocredito), '') as tipo_credito,
            nullif(trim(descricaotipo), '') as tipo_documento,
            nullif(trim(descricaoempenho), '') as descricao,
            -- `numeroProcessoCompras` is the ONLY real tender link and it is
            -- space-padded. `codigoLicitacao` is NOT an identifier: it takes 17
            -- distinct values whose labels are modalities (DISPENSA - SERVICOS,
            -- PREGAO - PRESENCIAL), the same trap as SP's `ddlLicitacao`.
            nullif(trim(numeroprocessocompras), '') as numero_processo_compras,
            nullif(trim(descricaolicitacao), '') as modalidade_licitacao,
            nullif(trim(cpfcnpj), '') as documento_credor,
            nullif(trim(nomecredor), '') as nome_credor,
            trim(codigofuncao) as cod_funcao,
            trim(codigosubfuncao) as cod_subfuncao,
            trim(codigoprograma) as cod_programa,
            trim(codigoacao) as cod_acao,
            -- 8-digit natureza da despesa on all 320,201 rows checked:
            -- C G MM EE = categoria, grupo, modalidade de aplicação, elemento.
            trim(codigonatureza) as cod_natureza,
            trim(codigoitemdespesa) as cod_item,
            trim(codigofonterecurso) as cod_fonte,
            -- Net of this document's own cancellation; see the header note.
            safe_cast(valordespesa as float64) as valor_empenhado,
            safe_cast(valorliquidado as float64) as valor_liquidado,
            safe_cast(valorpago as float64) as valor_pago
        from fonte
        where nullif(trim(numeroempenho), '') is not null
    )
select
    ano as ano,
    mes as mes,
    data as data,
    'PB' as sigla_uf,
    cod_orgao as orgao,
    nome_orgao as nome_orgao,
    cod_unidade as id_unidade_gestora,
    -- PB names the órgão but not the unidade; emitting the órgão's name here would
    -- assert they are the same body.
    safe_cast(null as string) as nome_unidade_gestora,
    concat(
        'PB-',
        cast(ano as string),
        '-',
        cod_orgao,
        '-',
        cod_unidade,
        '-',
        numero_empenho
    ) as id_empenho_bd,
    concat(
        cast(ano as string), '|', cod_orgao, '|', cod_unidade, '|', numero_empenho
    ) as id_empenho,
    numero_empenho as numero_empenho,
    tipo_credito as tipo_empenho,
    descricao as descricao,
    -- Only 446 of 320,201 rows in 2024 carry a compras process, so the link exists but
    -- is nearly empty. It is emitted where present and null elsewhere, rather than
    -- filled from `codigoLicitacao`, which is a modality.
    case
        when numero_processo_compras is not null
        then concat('PB-', numero_processo_compras)
    end as id_licitacao_bd,
    numero_processo_compras as id_licitacao,
    modalidade_licitacao as modalidade_licitacao,
    documento_credor as documento_credor,
    nome_credor as nome_credor,
    -- Derived from the published format: PB prints companies as a raw 14-digit CNPJ and
    -- natural persons masked. 2024: 120,177 CNPJ, 200,024 masked, none formatted.
    case
        when
            length(documento_credor) = 14 and not contains_substr(documento_credor, '*')
        then 'PJ'
        when contains_substr(documento_credor, '*')
        then 'PF'
    end as tipo_documento_credor,
    cod_funcao as funcao,
    cod_subfuncao as subfuncao,
    cod_programa as programa,
    cod_acao as acao,
    substr(cod_natureza, 1, 1) as categoria_economica,
    substr(cod_natureza, 2, 1) as grupo_despesa,
    substr(cod_natureza, 3, 2) as modalidade_aplicacao,
    substr(cod_natureza, 5, 2) as elemento_despesa,
    cod_item as item_despesa,
    cod_fonte as fonte_recurso,
    tipo_documento as tipo_documento,
    valor_empenhado as valor_empenhado,
    valor_liquidado as valor_liquidado,
    valor_pago as valor_pago
from base
