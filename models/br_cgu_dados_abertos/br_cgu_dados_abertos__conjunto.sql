{{ config(alias="conjunto", schema="br_cgu_dados_abertos") }}
select
    safe_cast(nullif(id, "") as string) id,
    safe_cast(nullif(titulo, "") as string) nome,
    safe_cast(nullif(nome, "") as string) nome_tokenizado,
    safe_cast(nullif(descricao, "") as string) descricao,
    safe_cast(nullif(mantenedor, "") as string) mantenedor,
    safe_cast(nullif(email_mantenedor, "") as string) email_mantenedor,
    safe_cast(
        nullif(id_organizacao_responsavel, "") as string
    ) id_organizacao_responsavel,
    safe_cast(nullif(organizacao_responsavel, "") as string) organizacao_responsavel,
    extract(date from safe.parse_datetime('%d/%m/%Y %T', data_criacao)) data_criacao,
    extract(
        date from safe.parse_datetime('%d/%m/%Y %T', data_atualizacao)
    ) data_atualizacao,
    safe_cast(quantidade_reusos as int64) quantidade_reusos,
    safe_cast(quantidade_recursos as int64) quantidade_recursos,
    safe_cast(quantidade_downloads as int64) quantidade_downloads,
from {{ set_datalake_project("br_cgu_dados_abertos_staging.conjunto") }} as c
