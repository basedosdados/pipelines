{{ config(alias="licitacao_pedido", schema="br_camara_dados_abertos") }}
select
    safe_cast(ano as int64) ano_licitacao,
    safe_cast(idlicitacao as string) id_licitacao,
    safe_cast(tiporegistro as string) tipo_registo,
    safe_cast(anopedido as int64) ano_pedido,
    safe_cast(numpedido as string) id_pedido,
    safe_cast(
        split(format_timestamp('%Y-%m-%d', timestamp(datahoracadastro)), ' ')[
            safe_offset(0)
        ] as date
    ) data_cadastro,
    safe_cast(
        split(
            format_timestamp('%Y-%m-%d %H:%M:%E*S', timestamp(datahoracadastro)), ' '
        )[safe_offset(1)] as time
    ) horario_cadastro,
    safe_cast(idorgao as string) id_orgao,
    safe_cast(orgao as string) orgao,
    safe_cast(objeto as string) descricao,
    safe_cast(observacoes as string) observacao,
from {{ set_datalake_project("br_camara_dados_abertos_staging.licitacao_pedido") }} as t
