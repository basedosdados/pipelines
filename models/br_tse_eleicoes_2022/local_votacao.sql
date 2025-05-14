select *
from {{ set_datalake_project("br_tse_eleicoes_2022_staging.local_votacao") }} as t
