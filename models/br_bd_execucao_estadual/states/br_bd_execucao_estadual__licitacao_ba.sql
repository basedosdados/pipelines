{{ config(materialized="ephemeral") }}

-- Bahia procurement processes, mapped onto the canonical `licitacao` schema.
--
-- Source: SIMPAS/SAEB via `VW_PROC_AQUISICAO_LIC_REQ` on dados.ba.gov.br -- 108,663
-- processes from **2004**, the longest procurement series of any state here.
--
-- BA is the only source that publishes `Registro de preço` as an explicit flag. In
-- MiDES
-- that signal has to be recovered from `natureza_processo = 2`, and it is what
-- separates a
-- tender authorising instalment purchases over twelve months from a one-off buy -- the
-- distinction behind "one tender per year" not meaning "one purchase per year". Carried
-- through as `registro_preco` rather than folded into the modality.
--
-- Column order must match br_bd_execucao_estadual__licitacao_mg exactly: the parent
-- union
-- resolves positionally.
select
    safe_cast(t.ano as int64) as ano,
    -- BA writes timestamps as "29/10/2009 00:00:00"; only the date half is
    -- meaningful, and
    -- parse_date on the trimmed left ten characters is safer than a format string
    -- spanning
    -- the time, which varies.
    extract(
        month from safe.parse_date('%d/%m/%Y', left(t.data_de_abertura, 10))
    ) as mes,
    'BA' as sigla_uf,
    safe_cast(t.codigo_do_orgao_solicitante as string) as orgao,
    safe_cast(trim(t.orgao_solicitante) as string) as nome_orgao,
    concat('BA-', t.processo_de_aquisicao) as id_licitacao_bd,
    safe_cast(t.processo_de_aquisicao_formatado as string) as id_licitacao,
    safe_cast(t.n_da_licitacao_formatado as string) as numero_licitacao,
    safe.parse_date('%d/%m/%Y', left(t.data_de_abertura, 10)) as data_abertura,
    safe.parse_date('%d/%m/%Y', left(t.data_de_publicacao_doe, 10)) as data_publicacao,
    safe.parse_date('%d/%m/%Y', left(t.data_de_homologacao, 10)) as data_homologacao,
    safe_cast(t.objeto as string) as descricao_objeto,
    safe_cast(t.modalidade as string) as modalidade,
    safe_cast(t.tipo as string) as tipo,
    safe_cast(t.desc_tipo as string) as criterio_julgamento,
    -- BA pads several label columns to a fixed width ("Homologada        ").
    safe_cast(trim(t.situacao) as string) as situacao,
    safe_cast(trim(t.poder) as string) as poder,
    safe_cast(t.forma_de_contratacao as string) as forma_contratacao,
    safe_cast(t.categoria as string) as categoria,
    safe_cast(t.registro_de_preco as string) as registro_preco,
    safe_cast(t.descricao_do_grupo as string) as grupo,
    safe_cast(replace(t.valor_estimado, ',', '.') as float64) as valor_referencia,
    safe_cast(replace(t.valor_homologado, ',', '.') as float64) as valor_homologado,
    cast(null as string) as url_edital,
    safe_cast(nullif(trim(t.processo_sei), '') as string) as processo_sei
from {{ set_datalake_project("br_bd_execucao_estadual_staging.ba_licitacao") }} as t
