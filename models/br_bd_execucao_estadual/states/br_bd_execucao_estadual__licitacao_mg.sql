{{ config(materialized="ephemeral") }}

-- Minas Gerais procurement processes, mapped onto the canonical `licitacao` schema.
--
-- Source: SIAD/MG via the `compras_contratos` dimensional model on dados.mg.gov.br,
-- 410,935 processes from 2010.
--
-- `procedimento` is MG's contracting route (PREGAO, INEXIGIBILIDADE, DISPENSA, ...)
-- and is
-- passed through verbatim rather than remapped onto the Lei 8.666 modality codes.
-- Remapping
-- silently corrupts the column when the source's numbering disagrees with the
-- target's --
-- the failure mode hit on TCE-TO in MiDES, where source 1=Dispensa against MiDES
-- 1=Convite.
-- The label is unambiguous on its own; a numeric recode is not, so the dicionario
-- carries
-- the crosswalk instead.
--
-- Every state model must project the canonical columns in THIS order: the union in the
-- parent resolves positionally, so a reordered or missing column silently shifts values
-- into the wrong field. Columns the source does not publish are explicit typed NULLs.
--
-- KNOWN SOURCE ERROR, passed through deliberately: process `1561122 000030/2011`
-- (INTENDENCIA DA CIDADE ADMINISTRATIVA, coffee and hot drinks vending) carries
-- `vr_referencia = 81755995676572.81` -- R$81.76 TRILLION -- against a homologated
-- value of
-- R$4,582,816.48. It is a data-entry error in MG's published file, verified against
-- the raw
-- CSV, not a parsing artefact. That single row is 99.8% of MG's entire valor_referencia
-- total (R$81.92tn); without it the total is R$167.9bn, against R$193.9bn homologated.
--
-- It is NOT nulled here. Staging mirrors the source, and silently editing a published
-- value
-- is worse than a documented outlier. Anyone aggregating `valor_referencia` for MG must
-- filter it -- the median is R$3,327.75 and the 99th percentile R$4.37M, so a simple
-- upper bound removes it along with the 14 other rows above R$1bn.
select
    safe_cast(extract(year from safe_cast(dt_cad_processo as date)) as int64) as ano,
    safe_cast(extract(month from safe_cast(dt_cad_processo as date)) as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(cd_orgao as string) as orgao,
    safe_cast(orgao as string) as nome_orgao,
    concat('MG-', id_processo) as id_licitacao_bd,
    safe_cast(cd_processo_formatado as string) as id_licitacao,
    cast(null as string) as numero_licitacao,
    safe_cast(dt_cad_processo as date) as data_abertura,
    cast(null as date) as data_publicacao,
    cast(null as date) as data_homologacao,
    safe_cast(objeto as string) as descricao_objeto,
    safe_cast(procedimento as string) as modalidade,
    safe_cast(tp_licitacao as string) as tipo,
    safe_cast(criterio_julgamento as string) as criterio_julgamento,
    safe_cast(situacao as string) as situacao,
    cast(null as string) as poder,
    cast(null as string) as forma_contratacao,
    cast(null as string) as categoria,
    -- MG does not flag registro de preços in its open procurement model. Left NULL
    -- rather
    -- than inferred from the procedure label, which would not be the same thing.
    cast(null as string) as registro_preco,
    cast(null as string) as grupo,
    safe_cast(vr_referencia as float64) as valor_referencia,
    safe_cast(vr_homologado as float64) as valor_homologado,
    safe_cast(nullif(url_edital, '') as string) as url_edital,
    cast(null as string) as processo_sei
from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_processo") }} as t
where dt_cad_processo is not null
