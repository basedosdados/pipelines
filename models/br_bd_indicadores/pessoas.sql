select
    safe_cast(id as string) id,
    safe_cast(nome as string) nome,
    safe_cast(descricao as string) descricao,
    safe_cast(email as string) email,
    safe_cast(twitter as string) twitter,
    safe_cast(github as string) github,
    safe_cast(website as string) website,
    safe_cast(linkedin as string) linkedin,
    safe_cast(url_foto as string) url_foto
from {{ set_datalake_project("br_bd_indicadores_staging.pessoas") }} as t
