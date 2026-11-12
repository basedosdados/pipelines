{{
    config(
        schema="br_tse_filiacao_partidaria",
        alias="microdados",
        materialized="table",
        unique_key="registro_filiacao",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        cluster_by=["sigla_uf"],
    )
}}

with
    tabela as (
        select
            safe_cast(t.sqregistrofiliacao as string) registro_filiacao,
            safe_cast(t.sgpartido as string) sigla_partido,
            safe_cast(t.sigla_uf as string) sigla_uf,
            safe_cast(m.id_municipio as string) id_municipio,
            safe_cast(t.codlocalidadetse as string) id_municipio_tse,
            safe_cast(t.numzona as string) zona,
            safe_cast(t.numsecao as string) secao,
            safe_cast(t.nrtituloeleitor as string) titulo_eleitor,
            safe_cast(t.numcpf as string) cpf,
            safe_cast(t.nmeleitor as string) nome,
            safe_cast(t.nmsocialeleitor as string) nome_social,
            safe_cast(t.tpsexo as string) sexo,
            safe_cast(t.dessituacaoeleitor as string) situacao_registro,
            safe_cast(t.cdmotivodesfiliacao as string) motivo_desfiliacao,
            safe_cast(t.cdmotivocancelamento as string) motivo_cancelamento,
            safe_cast(t.indorigem as string) indicador_origem,
            safe_cast(t.dtfiliacao as date) data_filiacao,
            safe_cast(t.dtdesfiliacao as date) data_desfiliacao,
            safe_cast(t.tscadastrodesfiliacao as date) data_cadastro_desfiliacao,
            safe_cast(t.dtcancelamento as date) data_cancelamento,
            safe_cast(t.dtexclusao as date) data_exclusao,
            safe_cast(t.data_extracao as date) data_extracao,
        from
            {{ set_datalake_project("br_tse_filiacao_partidaria_staging.microdados") }}
            as t
        left join
            basedosdados.br_bd_diretorios_brasil.municipio as m
            on t.codlocalidadetse = m.id_municipio_tse
    ),
    select_rows as (
        select
            *,
            row_number() over (
                partition by registro_filiacao order by data_extracao desc
            ) as rn
        from tabela
    )
select * except (rn)
from select_rows
where rn = 1
