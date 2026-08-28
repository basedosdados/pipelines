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
-- Só existe onde a fonte publica o vínculo de forma explícita. Em MiDES esse elo existe
-- apenas para o Paraná, via a tabela `relacionamentos` do TCE-PR; aqui MG o publica
-- nativamente (`fl_compras_empenho`), o que torna a cadeia licitação -> empenho ->
-- pagamento
-- utilizável sem heurística de chave.
--
-- A relação é muitos-para-muitos por construção: um processo pode gerar vários
-- empenhos e,
-- em princípio, um empenho pode atender a mais de um processo. Nada aqui é deduplicado.
select
    'MG' as sigla_uf,
    concat('MG-', id_processo) as id_licitacao_bd,
    safe_cast(id_processo as string) as id_licitacao_origem,
    safe_cast(id_empenho as string) as id_empenho,
    safe_cast(dotacao_orcamentaria as string) as dotacao_orcamentaria
from
    {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_fl_compras_empenho") }}
    as t
