{{ config(alias="deputado_ocupacao", schema="br_camara_dados_abertos") }}
# register
select distinct
    safe_cast(id as string) id_deputado,
    safe_cast(anoinicio as int64) ano_inicio,
    safe_cast(anofim as int64) ano_fim,
    safe_cast(entidadeuf as string) sigla_uf,
    safe_cast(entidade as string) entidade,
    safe_cast(titulo as string) titulo,
from
    {{ set_datalake_project("br_camara_dados_abertos_staging.deputado_ocupacao") }} as t
