from databasers_utils import TableArchitecture

arch = TableArchitecture(
    dataset_id="br_ibge_censo_2022",
    tables={
        # "estudantes_tea_raca_cor_grupo_idade": "https://docs.google.com/spreadsheets/d/169n7y4zWqUNUkfGbmLcENlaCHtSg7FUCb_UQdow8jhI/edit?gid=0#gid=0",
        # "estudantes_tea_sexo_grupo_idade": "https://docs.google.com/spreadsheets/d/1LsAFwz44rYggkjesNvPirseVwnwiU_AhEcokofVSgrQ/edit?gid=0#gid=0",
        "estudantes_tea_grupo_idade_nivel_escolaridade": "https://docs.google.com/spreadsheets/d/1sfrTAifinAoUeAVemjLmlX2TAEb94brwi8LPYDLRH44/edit?gid=0#gid=0",
        # "populacao_tea_indigena": "https://docs.google.com/spreadsheets/d/1y3wRAMCvRayFu6E48eVhN7v2oSk3rXfKYf4XQftlnkQ/edit?gid=0#gid=0",
        # "populacao_tea_raca_cor": "https://docs.google.com/spreadsheets/d/1rYN__xkX1ZJUEj0ZhORwZqZMNealSwabWhqpQ-qwdXY/edit?gid=0#gid=0",
        # "populacao_tea_sexo_grupo_idade": "https://docs.google.com/spreadsheets/d/1r9VKImVH6JW11TQkUGBMGY2Q34u0PUCkoqX9yCpKn4o/edit?gid=0#gid=0",
        # "populacao_tea_sexo_nivel_escolaridade":"https://docs.google.com/spreadsheets/d/1Cg9xXufoQsSppZ5f-BpEpaQPs076AoQZXO7K_T8qWv8/edit?gid=0#gid=0",
    },
)

# Cria o yaml file
arch.create_yaml_file()

# Cria os arquivos sql
arch.create_sql_files()

# Atualiza o dbt_project.yml
arch.update_dbt_project()

# Faz o upload das colunas para o DJango
# Para essa etapa é necessário ter duas varíaveis de ambiente configurada
# BD_DJANGO_EMAIL="seuemail@basedosdados.org"
# BD_DJANGO_PASSWORD="password"
# arch.upload_columns()
