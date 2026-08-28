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
select
    safe_cast(extract(year from safe_cast(dt_cad_processo as date)) as int64) as ano,
    safe_cast(extract(month from safe_cast(dt_cad_processo as date)) as int64) as mes,
    'MG' as sigla_uf,
    safe_cast(cd_orgao as string) as orgao,
    safe_cast(orgao as string) as nome_orgao,
    concat('MG-', id_processo) as id_licitacao_bd,
    safe_cast(cd_processo_formatado as string) as id_licitacao,
    safe_cast(dt_cad_processo as date) as data_abertura,
    safe_cast(objeto as string) as descricao_objeto,
    safe_cast(procedimento as string) as modalidade,
    safe_cast(tp_licitacao as string) as tipo,
    safe_cast(criterio_julgamento as string) as criterio_julgamento,
    safe_cast(situacao as string) as situacao,
    safe_cast(vr_referencia as float64) as valor_referencia,
    safe_cast(vr_homologado as float64) as valor_homologado,
    safe_cast(nullif(url_edital, '') as string) as url_edital
from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_processo") }} as t
where dt_cad_processo is not null
