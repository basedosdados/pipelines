import basedosdados as bd

DATASET_ID = "br_ibge_censo_2022"

# Dicionário com nome da tabela : caminho completo do arquivo CSV
tables = {
    # "estudantes_tea_raca_cor_grupo_idade": "/home/laribrito/BD/tea_censo_2022/estudantes_tea_raca_cor_grupo_idade.csv",
    # "estudantes_tea_sexo_grupo_idade": "/home/laribrito/BD/tea_censo_2022/estudantes_tea_sexo_grupo_idade.csv",
    "estudantes_tea_grupo_idade_nivel_escolaridade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_grupo_idade_nivel_escolaridade.csv",
    # "populacao_tea_indigena": "/home/laribrito/BD/tea_censo_2022/populacao_tea_indigena.csv",
    # "populacao_tea_raca_cor": "/home/laribrito/BD/tea_censo_2022/populacao_tea_raca_cor.csv",
    # "populacao_tea_sexo_grupo_idade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_sexo_grupo_idade.csv",
    # "populacao_tea_sexo_nivel_escolaridade": "/home/laribrito/BD/tea_censo_2022/populacao_tea_sexo_nivel_escolaridade.csv",
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
