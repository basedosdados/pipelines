{{
    config(
        alias="contrato",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Contratos dos governos estaduais: os instrumentos contratuais celebrados, com objeto,
-- contratado, vigência e valor. Uma linha por contrato.
--
-- Existe para os três estados que publicam um cadastro de contratos: Espírito Santo,
-- Santa Catarina e Rio Grande do Sul. Complementa as tabelas de execução (`despesa`,
-- `liquidacao`, `pagamento`): o contrato é o compromisso, a execução é o gasto contra
-- ele. Ligue à licitação por `numero_processo` quando presente.
--
-- **Só contratos.** A base "Contratos" do ES mistura contratos com autorizações de
-- compra, ordens de fornecimento e notas de empenho; aqui o ES é filtrado para os
-- documentos do tipo Contrato, para que a tabela signifique o mesmo nos três estados
-- (SC e RS já publicam apenas contratos). O RS separa os contratos em quatro tipos
-- (fornecimento de bens, locação, obras e serviços de engenharia, serviços de
-- terceiros), unificados aqui com o tipo em `tipo_contrato`.
--
-- `ano` é o ano de assinatura/início de vigência; contratos com data ausente ou
-- claramente inválida (o ES registra 1753 e 5024, o SC datas em 3031) ficam com `ano`
-- nulo em vez de poluir uma partição.
--
-- Cada estado é um modelo efêmero em states/; as colunas são resolvidas
-- posicionalmente a partir do primeiro termo, então todo modelo projeta esta ordem.
select *
from {{ ref("br_bd_execucao_estadual__contrato_es") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__contrato_sc") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__contrato_rs") }}
