# Como atualizar os dados da PEVS

- Você precisará entrar na pasta `code/{tabela}`, por exemplo: `code/extracao_vegetal`

  1. Rode primeiramente o arquivo: `1-extracao_vegetal_metadados_enriquecidos.py` -> Esse código irá transformar os metadados da tabela em uma tabela csv chamada `extracao_vegetal_metadados_enriquecidos.csv` na raiz do projeto.
  2. Posteriormente, rode o arquivo: `2-extracao_vegetal_api_to_json.py` -> Esse código irá baixar os dados da API do IBGE e irá transforma-lo em um json.
  3. Por último, rode o arquivo: `3-extracao_vegetal_json_to_parquet` -> Esse código irá transformar o Json em um arquivo parquet tratado e organizado, pronto para atualizar os dados.




# Em relação ao `where` do modelo dbt.

* Durante a análise, identificamos que vários produtos não possuíam qualquer registro de produção nos municípios. Isso resultava em uma tabela pouco útil, já que cerca de 90% das linhas estavam vazias. Para melhorar a qualidade dos dados e reduzir o peso da tabela, optamos por remover esses produtos sem produção, mantendo apenas informações realmente relevantes para as consultas e análises.
