{{
    config(
        schema="br_tse_filiacao_partidaria",
        alias="microdados",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
        cluster_by=["sigla_uf"],
        pre_hook="{% if adapter.get_relation(this.database, this.schema, this.identifier) %}DROP ALL ROW ACCESS POLICIES ON {{ this }}{% else %}SELECT 1{% endif %}",
    )
}}

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
    safe_cast(t.data_extracao as date) data_extracao
from {{ set_datalake_project("br_tse_filiacao_partidaria_staging.microdados") }} as t
left join
    basedosdados.br_bd_diretorios_brasil.municipio as m
    on t.codlocalidadetse = m.id_municipio_tse
{% if is_incremental() %}
        {%- set max_date_result = run_query(
            "select format_date('%Y-%m-%d', max(safe.parse_date('%Y%m%d', partition_id))) from `"
            ~ this.database
            ~ "`.`"
            ~ this.schema
            ~ "`.INFORMATION_SCHEMA.PARTITIONS where table_name = '"
            ~ this.identifier
            ~ "' and total_rows > 0"
        ) -%}
        {%- set max_date = "" -%}
        {%- if execute and max_date_result.rows[0][0] -%}
            {%- set max_date = max_date_result.rows[0][0] -%}
        {%- endif -%}
    where t.data_extracao > '{{ max_date }}'
{% endif %}
