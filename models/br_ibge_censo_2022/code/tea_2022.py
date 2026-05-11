import basedosdados as bd

DATASET_ID = "br_ibge_censo_2022"

# # Dicionário com nome da tabela : caminho completo do arquivo CSV
tables = {
    #     #"estudantes_tea_raca_cor_grupo_idade": "/home/laribrito/BD/tea_censo_2022/estudantes_tea_raca_cor_grupo_idade.csv",
    # "estudantes_tea_sexo_grupo_idade": "/home/laribrito/BD/tea_censo_2022/estudantes_tea_sexo_grupo_idade.csv",
    #     #"estudantes_tea_grupo_idade_nivel_escolaridade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_grupo_idade_nivel_escolaridade.csv",
    #     #"populacao_tea_indigena": "/home/laribrito/BD/tea_censo_2022/populacao_tea_indigena.csv",
    #     #"populacao_tea_raca_cor": "/home/laribrito/BD/tea_censo_2022/populacao_tea_raca_cor.csv",
    #     #"populacao_tea_sexo_grupo_idade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_sexo_grupo_idade.csv",
    #     #"populacao_tea_sexo_nivel_escolaridade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_sexo_nivel_escolaridade.csv",
    #     #"estudantes_deficiencia_grupo_idade_nivel_escolaridade": "/home/laribrito/BD/tea_censo_2022/estudantes_deficiencia_grupo_idade_nivel_escolaridade.csv",
    #     #"estudantes_deficiencia_grupo_idade_raca_cor": "/home/laribrito/BD/tea_censo_2022/estudantes_deficiencia_grupo_idade_raca_cor.csv",
    #     #"estudantes_deficiencia_grupo_idade_sexo": "/home/laribrito/BD/tea_censo_2022/estudantes_deficiencia_grupo_idade_sexo.csv",
    #     #"estudantes_indigenas_deficiencia_grupo_idade": "/home/laribrito/BD/tea_censo_2022/estudantes_indigenas_deficiencia_grupo_idade.csv",
    # "taxa_escolarizacao_tea_sexo_grupo_idade":"/home/laribrito/BD/tea_censo_2022/taxa_escolarizacao_tea_sexo_grupo_idade.csv",
    "taxa_escolarizacao_tea_raca_cor_grupo_idade": "/home/laribrito/BD/tea_censo_2022/taxa_escolarizacao_tea_raca_cor_grupo_idade.csv",
}

for table_id, csv_path in tables.items():
    tb = bd.Table(dataset_id=DATASET_ID, table_id=table_id)
    print(f"Subindo a tabela {table_id} do arquivo {csv_path}...")
    tb.create(
        path=csv_path,
        if_storage_data_exists="replace",
        if_table_exists="replace",
        source_format="csv",
    )

# from databasers_utils import TableArchitecture

# arch = TableArchitecture(
#     dataset_id="br_ibge_censo_2022",
#     tables={
#         "taxa_escolarizacao_tea_sexo_grupo_idade": "https://docs.google.com/spreadsheets/d/1cf68Slhnm4f9jzk4_Jg0YO94sZo4gfOlc1_0zYjzc-8/edit?gid=0#gid=0",
#         "taxa_escolarizacao_tea_raca_cor_grupo_idade": "https://docs.google.com/spreadsheets/d/1lb3l6RABuTDmt7HRBpi2FuQDGP7pEKCtEJHg81yVhwo/edit?gid=0#gid=0", # Exemplo https://docs.google.com/spreadsheets/d/1K1svie4Gyqe6NnRjBgJbapU5sTsLqXWTQUmTRVIRwQc/edit?usp=drive_link
#     },
# )

# Cria o yaml file
# arch.create_yaml_file()

# Cria os arquivos sql
# arch.create_sql_files()

# Atualiza o dbt_project.yml
# arch.update_dbt_project()

# Faz o upload das colunas para o DJango
# Para essa etapa é necessário ter duas varíaveis de ambiente configurada
# BD_DJANGO_EMAIL="seuemail@basedosdados.org"
# BD_DJANGO_PASSWORD="password"
# arch.upload_columns()
