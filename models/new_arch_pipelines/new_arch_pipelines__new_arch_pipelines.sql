{{ config(alias="new_arch_pipelines", schema="new_arch_pipelines") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(equipe_dados as string) equipe_dados,
    safe_cast(github as string) github,
    safe_cast(idade as int64) idade,
    safe_cast(sexo as string) sexo
from basedosdados - dev.new_arch_pipeline_staging.new_arch_pipeline as t
