{{
    config(
        alias="relacionamentos",
        schema="br_bd_execucao_estadual",
        materialized="table",
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Ponte entre processos licitatórios e empenhos.
--
-- Só existe onde a fonte publica o vínculo. Em MiDES esse elo existe apenas para o
-- Paraná,
-- via a tabela `relacionamentos` do TCE-PR; aqui dois estados o publicam, o que torna a
-- cadeia licitação -> empenho -> pagamento utilizável sem heurística de chave.
--
-- Minas Gerais publica o vínculo direto (`fl_compras_empenho`) e identifica o empenho
-- pelo
-- seu id interno. A Bahia liga em dois passos -- item da licitação -> instrumento
-- orçamentário -> empenho -- e não tem id interno, então identifica o empenho pelo
-- número.
-- Por isso as duas colunas coexistem e cada estado preenche uma delas.
--
-- A relação é muitos-para-muitos por construção: um processo pode gerar vários
-- empenhos e,
-- em princípio, um empenho pode atender a mais de um processo. Nada aqui é deduplicado.
select *
from {{ ref("br_bd_execucao_estadual__relacionamentos_mg") }}
union all
select *
from {{ ref("br_bd_execucao_estadual__relacionamentos_ba") }}
