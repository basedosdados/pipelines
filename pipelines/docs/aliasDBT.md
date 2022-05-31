# Como funciona o table alias no DBT

Uma limitação importante nos modelos DBT é que não é possível criar modelos com o mesmo nome. Como, por *default*, o nome das tabelas é o nome do arquivo do modelo, isso implica que, em um *Data Lake* que tenha nomes de tabelas duplicados, a materialização do DBT vai falhar. Nesses casos, o DBT gera um erro como esse:

```bash
Exception: Error running sync command: RPC server failed to compile project, call the "status" method for compile status: Compilation Error
  dbt found two resources with the name "mes_brasil". Since these resources have the same name,
  dbt will be unable to find the correct resource when ref("mes_brasil") is used. To fix this,
  change the name of one of these resources:
  - model.basedosdados.mes_brasil (models/br_ibge_inpc/mes_brasil.sql)
  - model.basedosdados.mes_brasil (models/br_ibge_ipca/mes_brasil.sql)
```

Para evitar esse problema usamos um recurso do DBT chamado `alias`. Para criar um alias de uma tabela, basta incluir a seguinte linha no topo do arquivo SQL do modelo DBT:

```
{{ config(alias='mes_brasil', schema='br_ibge_ipca') }}
SELECT *
FROM ...
```

O nome dos arquivos SQL, porém, devem permanecer únicos. Na **BD** adotamos a convenção de dar o nome desses arquivos no seguinte formato `dataset_id__table_id`.
É importante notar que esse é o nome que dever ser utilizado na hora de rodar o modelo DBT:

```
dbt run -s `dataset_id__table_id`
```

Para mais detalhes, ver documentação pertinente: [DBT alias](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-aliases)