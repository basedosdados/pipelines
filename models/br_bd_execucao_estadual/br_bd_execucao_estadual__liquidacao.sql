{{
    config(
        alias="liquidacao",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2011, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Liquidações dos governos estaduais, no nível do documento de liquidação. Uma linha
-- por
-- lançamento de liquidação, com data, valor, credor e o vínculo com o empenho.
--
-- É a fase intermediária da execução -- empenho -> LIQUIDAÇÃO -> pagamento -- e existe
-- apenas para os três estados que publicam o documento de liquidação separadamente:
-- Santa
-- Catarina, Paraíba e Ceará. Os seis estados originais (MG, BA, PE, SP, ES, RS) não
-- emitem
-- documento de liquidação; naqueles a liquidação aparece só como a coluna
-- `valor_liquidado` em `despesa`.
--
-- Ligue a `despesa` por `id_empenho_bd`. O vínculo existe para SC e PB (o documento
-- carrega
-- o empenho de origem); para o CE é nulo, porque a fonte cearense usa um sistema de
-- códigos
-- de unidade gestora que não resolve para o empenho -- `numero_empenho` traz a
-- referência
-- crua. A soma de `valor_liquidado` aqui NÃO tem de bater com
-- `despesa.valor_liquidado`:
-- esta tabela é o razão de movimentos, e inclui anulações de liquidação.
--
-- Cada estado é um modelo efêmero em states/, e as colunas são resolvidas
-- posicionalmente
-- a partir do primeiro termo da união, então todo modelo estadual projeta as colunas
-- nesta
-- ordem exata.
select *
from {{ ref("br_bd_execucao_estadual__liquidacao_sc") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__liquidacao_pb") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__liquidacao_ce") }}
