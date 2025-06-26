from databasers_utils import TableArchitecture

arch = TableArchitecture(
    dataset_id="world_iea_timss",
    tables={
        "school_context_grade_4": "URL DA ARQUITETURA",
        "school_context_grade_8": "URL DA ARQUITETURA",
        "student_achievement_grade_4": "URL DA ARQUITETURA",
        "student_achievement_grade_8": "URL DA ARQUITETURA",
        "student_context_grade_4": "URL DA ARQUITETURA",
        "student_context_grade_8": "URL DA ARQUITETURA",
        "teacher_context_grade_4": "URL DA ARQUITETURA",
        "teacher_mathematics_grade_8": "URL DA ARQUITETURA",
        "teacher_science_grade_8": "URL DA ARQUITETURA",
        "dicionario": "URL DA ARQUITETURA",
    },
)

# Cria o yaml file
arch.create_yaml_file()

# Cria os arquivos sql
arch.create_sql_files()

# Atualiza o dbt_project.yml
arch.update_dbt_project()

# Faz o upload das colunas para o DJango
# Para essa etapa é necessário ter duas variáveis de ambiente configuradas
# BD_DJANGO_EMAIL="seuemail@basedosdados.org"
# aBD_DJANGO_PASSWORD="password"
# ach.upload_columns()
