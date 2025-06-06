{{
    config(
        alias="licitacao",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2021, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        labels={"tema": "economia"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(id_licitacao_bd as string) id_licitacao_bd,
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_dispensa as string) id_dispensa,
    safe_cast(ano_processo as int64) ano_processo,
    safe_cast(data_abertura as date) data_abertura,
    safe_cast(data_edital as date) data_edital,
    safe_cast(data_homologacao as date) data_homologacao,
    safe_cast(data_publicacao_dispensa as date) data_publicacao_dispensa,
    safe_cast(descricao_objeto as string) descricao_objeto,
    safe_cast(natureza_objeto as string) natureza_objeto,
    safe_cast(modalidade as string) modalidade,
    safe_cast(natureza_processo as string) natureza_processo,
    safe_cast(tipo as string) tipo,
    safe_cast(forma_pagamento as string) forma_pagamento,
    safe_cast(valor_orcamento as float64) valor_orcamento,
    safe_cast(valor as float64) valor,
    safe_cast(valor_corrigido as float64) valor_corrigido,
    safe_cast(situacao as string) situacao,
    safe_cast(estagio as string) estagio,
    safe_cast(preferencia_micro_pequena as string) preferencia_micro_pequena,
    safe_cast(exclusiva_micro_pequena as string) exclusiva_micro_pequena,
    safe_cast(contratacao as string) contratacao,
    safe_cast(quantidade_convidados as int64) quantidade_convidados,
    safe_cast(tipo_cadastro as string) tipo_cadastro,
    safe_cast(carona as string) carona,
    safe_cast(covid_19 as string) covid_19
from {{ set_datalake_project("world_wb_mides_staging.licitacao") }} as t
