{{ config(alias="recurso", schema="br_cgu_dados_abertos") }}
select
    safe_cast(nullif(id, "") as string) id,
    safe_cast(nullif(id_conjunto, "") as string) id_conjunto,
    safe_cast(nullif(nome, "") as string) nome,
    safe_cast(nullif(descricao, "") as string) descricao,
    safe_cast(nullif(tipo, "") as string) tipo,
    safe_cast(nullif(formato, "") as string) formato,
    safe_cast(nullif(tamanho_bytes, 0) as int64) tamanho_bytes,
    safe_cast(nullif(url_download, "") as string) url_download,
    extract(date from safe_cast(data_criacao as timestamp)) data_criacao,
    extract(
        date from safe.parse_datetime('%d/%m/%Y %T', data_modificacao_metadados)
    ) data_modificacao_metadados,
    safe_cast(quantidade_downloads as int64) quantidade_downloads,
from {{ set_datalake_project("br_cgu_dados_abertos_staging.recurso") }} as r
