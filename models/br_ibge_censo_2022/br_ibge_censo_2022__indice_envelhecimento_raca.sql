{{ config(alias="indice_envelhecimento_raca", schema="br_ibge_censo_2022") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(cor_ou_raca as string) cor_raca,
    safe_cast(
        replace(
            indice_de_envelhecimento_idosos_60_anos_ou_mais_de_idade_razao_, ",", "."
        ) as float64
    ) indice_envelhecimento,
    safe_cast(idade_mediana_anos_ as int64) idade_mediana,
    safe_cast(replace(razao_de_sexo_razao_, ",", ".") as float64) razao_sexo,
from
    {{ set_datalake_project("br_ibge_censo_2022_staging.indice_envelhecimento_raca") }} t
