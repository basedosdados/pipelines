{{ config(alias="senador_mandato", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_senador as string) id_senador,
    safe_cast(id_mandato as string) id_mandato,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(participacao as string) participacao,
    safe_cast(numero_legislatura_1 as string) numero_legislatura_1,
    safe_cast(data_inicio_legislatura_1 as date) data_inicio_legislatura_1,
    safe_cast(data_fim_legislatura_1 as date) data_fim_legislatura_1,
    safe_cast(numero_legislatura_2 as string) numero_legislatura_2,
    safe_cast(data_inicio_legislatura_2 as date) data_inicio_legislatura_2,
    safe_cast(data_fim_legislatura_2 as date) data_fim_legislatura_2,
from {{ set_datalake_project("br_senado_dados_abertos_staging.senador_mandato") }} as t
