## 2. Testes dbt

Alguns erros aconteceram:

### Date in range
```sql
select data_exclusao_mei as invalid_date
from `basedosdados-dev`.`br_rf_cnpj`.`simples`
where data_exclusao_mei is not null
and (
    date(data_exclusao_mei) < date('1900-01-01')
    or date(data_exclusao_mei) > current_date()
)
```

Falhou para 351.014 linhas

`data_exclusao_simples` e `data_exclusao_mei` de fato aparecem na fonte com valores `'20261231'`.
O teste será modificado para usar como limite superior, o último dia do ano corrente, dado que a exclusão da opção MEI geralmente entra em vigor (no caso de irregularidade) no primeiro dia do ano seguinte.
Será adotado este padrão para todas as datas de exclusão.

