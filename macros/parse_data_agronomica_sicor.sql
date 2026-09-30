-- As colunas de plantio, colheita e evento das tabelas do Proagro trazem erros
-- de digitação com o ano transposto — '02/10/3202' por 2023, '06/11/1202' por
-- 2021, '20/08/0217' por 2017 — além de anos simplesmente impossíveis (1914,
-- 1935, 2224, 9202). O BigQuery aceita todos eles como datas válidas, então o
-- filtro tem de ser explícito. A janela 2000–2100 anula no máximo 0,072% de
-- qualquer uma das colunas afetadas; uma janela mais estreita passaria a anular
-- valores possivelmente legítimos (ver o README do pipeline).
--
-- É a mesma ideia do tratamento aplicado em `br_bcb_sicor__operacao`, que ali
-- só cobre o limite superior (> 2100); aqui a janela é bilateral porque os
-- erros aparecem nas duas pontas.
{% macro parse_data_agronomica_sicor(coluna, ano_min=2000, ano_max=2100) %}
    case
        when
            extract(year from parse_date("%d/%m/%Y", {{ coluna }}))
            between {{ ano_min }} and {{ ano_max }}
        then safe_cast(parse_date("%d/%m/%Y", {{ coluna }}) as date)
    end
{% endmacro %}
