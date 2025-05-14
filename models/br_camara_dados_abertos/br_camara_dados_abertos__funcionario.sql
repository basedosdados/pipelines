{{ config(alias="funcionario", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(nome as string) nome,
    safe_cast(cargo as string) cargo,
    safe_cast(funcao as string) funcao,
    safe_cast(datainiciohistorico as date) data_inicio_historico,
    safe_cast(datanomeacao as date) data_nomeacao,
    safe_cast(datapubnomeacao as date) data_publicacao_nomeacao,
    safe_cast(grupo as string) grupo,
    safe_cast(ponto as string) ponto,
    safe_cast(atonomeacao as string) ato_nomeacao,
    safe_cast(lotacao as string) lotacao,
from {{ set_datalake_project("br_camara_dados_abertos_staging.funcionario") }} as t
