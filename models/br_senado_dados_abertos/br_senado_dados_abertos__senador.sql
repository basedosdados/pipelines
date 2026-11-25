{{ config(alias="senador", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome as string) nome,
    safe_cast(nome_completo as string) nome_completo,
    safe_cast(sexo as string) sexo,
    safe_cast(forma_tratamento as string) forma_tratamento,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(email as string) email,
    safe_cast(url_foto as string) url_foto,
    safe_cast(url_pagina as string) url_pagina,
    safe_cast(url_pagina_particular as string) url_pagina_particular,
    safe_cast(id_publico_legislatura_atual as string) id_publico_legislatura_atual,
from {{ set_datalake_project("br_senado_dados_abertos_staging.senador") }} as t
