{{
    config(
        alias="contrato_fiscal",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Gestores e fiscais de contrato dos governos estaduais: os servidores nomeados para
-- gerir e fiscalizar cada contrato. Uma linha por (contrato, papel, pessoa).
--
-- É a única tabela do conjunto cujo grão é uma PESSOA, e não um órgão ou um documento.
-- Permite perguntar sobre carga de fiscalização, rotatividade e relação entre quem
-- fiscaliza e o desempenho do contrato.
--
-- A fonte publica listas separadas por vírgula numa só célula, e este modelo as
-- desmembra. O campo `masp` é a matrícula do servidor, presente em 19% dos contratos
-- porque só aí a fonte a informa; nos demais há apenas o nome.
--
-- Ligue a `contrato` por `id_contrato_bd`. Dois contratos da fonte não existem em
-- `contrato` e ficam com `id_contrato_bd` nulo, então não há teste de integridade
-- referencial.
--
-- Existe só para Minas Gerais.
select *
from {{ ref("br_bd_execucao_estadual__contrato_fiscal_mg") }}
