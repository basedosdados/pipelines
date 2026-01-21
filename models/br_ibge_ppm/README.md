# Como atualizar os dados da PPM

- Você precisará entrar na pasta `{tabela}/code/`, por exemplo: `efetivo_rebanhos/code`:


  1. Primeiramente, rode o arquivo: `api_to_json.py` -> Esse código irá baixar os dados da API do IBGE e irá transforma-lo em um json.
  3. Por último, rode o arquivo: `json_to_parquet` -> Esse código irá transformar o Json em um arquivo parquet tratado e organizado, pronto para atualizar os dados.

- Em relação a tabela de `producao_pecuaria`, precisaríamos concatenar duas tabelas, dessa forma:
  1. Primeiramente, caminhe até a pasta `producao_pecuaria/source/` -> Rode tanto `ovinos_tosquiados` e `vacas_ordenhadas` na ordem de `api_to_json.py` e `json_to_parquet.py`.
  2. Por último, rode o código `join_parquet.py` dentro de `producao_pecuaria/code`. -> Dessa forma, você terá a tabela completa para subir no data lakehouse da BD.



# Em relação ao `where` do modelo dbt.

* Durante a análise, identificamos que vários produtos não possuíam qualquer registro de produção nos municípios. Isso resultava em uma tabela pouco útil, já que cerca de 90% das linhas estavam vazias. Para melhorar a qualidade dos dados e reduzir o peso da tabela, optamos por remover esses produtos ou tipo de rebanho sem produção, mantendo apenas informações realmente relevantes para as consultas e análises.
