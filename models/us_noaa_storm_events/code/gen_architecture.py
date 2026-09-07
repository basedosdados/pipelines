"""Write the architecture CSVs for us_noaa_storm_events.

The CSVs under ``architecture/`` are the single source of truth for column
names, order, BigQuery types, units, directory links and the raw -> clean name
mapping. The cleaning transform, the dbt models and the backend metadata are all
generated from them, so a schema change is made here and nowhere else.

Descriptions are carried in all three languages in the CSV itself rather than
translated downstream, so no second source of truth exists.

Run: uv run python models/us_noaa_storm_events/code/gen_architecture.py
"""

import csv
from pathlib import Path

OUT = Path(__file__).resolve().parent / "architecture"

HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]

DIR_YEAR = "diretorios_data_tempo.ano:ano"
DIR_MONTH = "diretorios_data_tempo.mes:mes"
DIR_STATE = "diretorios_us.state:id_state"
DIR_COUNTY = "diretorios_us.county:id_county"


def col(
    name,
    bq,
    pt,
    en,
    es,
    *,
    dict_=False,
    directory="",
    unit="",
    sensitive=False,
    obs=("", "", ""),
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": bq,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": coverage,
        "covered_by_dictionary": "yes" if dict_ else "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "yes" if sensitive else "no",
        "observations_pt": obs[0],
        "observations_en": obs[1],
        "observations_es": obs[2],
        "original_name": original,
    }


# --------------------------------------------------------------------------
# event — one row per storm event
# --------------------------------------------------------------------------

EVENT = [
    col(
        "year",
        "INT64",
        "Ano-calendário do início do evento",
        "Calendar year in which the event began",
        "Año calendario en que comenzó el evento",
        directory=DIR_YEAR,
        unit="year",
        original="YEAR",
        obs=(
            "Coluna de particionamento; corresponde ao ano do arquivo anual de origem",
            "Partition column; matches the year of the source annual file",
            "Columna de particionamiento; coincide con el año del archivo anual de origen",
        ),
    ),
    col(
        "month",
        "INT64",
        "Mês-calendário do início do evento",
        "Calendar month in which the event began",
        "Mes calendario en que comenzó el evento",
        directory=DIR_MONTH,
        unit="month",
        original="BEGIN_YEARMONTH",
    ),
    col(
        "event_id",
        "STRING",
        "Identificador do evento atribuído pelo NWS",
        "Event identifier assigned by the NWS",
        "Identificador del evento asignado por el NWS",
        original="EVENT_ID",
        obs=(
            "Chave primária lógica da tabela e chave de ligação com fatality e event_location; único em todo o período",
            "Logical primary key of the table and the join key to fatality and event_location; unique across the whole record",
            "Clave primaria lógica de la tabla y clave de unión con fatality y event_location; único en todo el período",
        ),
    ),
    col(
        "episode_id",
        "STRING",
        "Identificador do episódio a que o evento pertence, atribuído pelo NWS",
        "Identifier of the episode the event belongs to, assigned by the NWS",
        "Identificador del episodio al que pertenece el evento, asignado por el NWS",
        original="EPISODE_ID",
        obs=(
            "Um episódio agrupa eventos de um mesmo sistema meteorológico. Vazio em 232.245 registros, praticamente todos anteriores a 1996, quando o conceito de episódio ainda não era registrado",
            "An episode groups events from the same weather system. Empty on 232,245 records, almost all before 1996, when the episode concept was not yet recorded",
            "Un episodio agrupa eventos de un mismo sistema meteorológico. Vacío en 232.245 registros, casi todos anteriores a 1996, cuando el concepto de episodio aún no se registraba",
        ),
    ),
    col(
        "state_id",
        "STRING",
        "Código FIPS de duas posições do estado, território ou distrito onde o evento ocorreu",
        "Two-digit FIPS code of the state, territory or district where the event occurred",
        "Código FIPS de dos posiciones del estado, territorio o distrito donde ocurrió el evento",
        directory=DIR_STATE,
        original="STATE_FIPS",
        obs=(
            "Derivado de state_fips_nws. O NWS usa códigos próprios para os territórios (99 Porto Rico, 98 Guam, 97 Samoa Americana, 96 Ilhas Virgens), convertidos aqui para os códigos FIPS reais (72, 66, 60, 78). Nulo nas 51.642 linhas de zonas marítimas e dos Grandes Lagos, que não são estados e não possuem código FIPS",
            "Derived from state_fips_nws. The NWS uses its own codes for the territories (99 Puerto Rico, 98 Guam, 97 American Samoa, 96 Virgin Islands), converted here to the real FIPS codes (72, 66, 60, 78). Null on the 51,642 marine and Great Lakes zone rows, which are not states and have no FIPS code",
            "Derivado de state_fips_nws. El NWS usa códigos propios para los territorios (99 Puerto Rico, 98 Guam, 97 Samoa Americana, 96 Islas Vírgenes), convertidos aquí a los códigos FIPS reales (72, 66, 60, 78). Nulo en las 51.642 filas de zonas marítimas y de los Grandes Lagos, que no son estados y no tienen código FIPS",
        ),
    ),
    col(
        "county_id",
        "STRING",
        "Código FIPS de cinco posições do condado onde o evento ocorreu",
        "Five-digit FIPS code of the county where the event occurred",
        "Código FIPS de cinco posiciones del condado donde ocurrió el evento",
        directory=DIR_COUNTY,
        original="CZ_FIPS",
        obs=(
            "Preenchido apenas quando cz_type = C, ou seja, quando cz_fips é de fato um código de condado: 1.301.381 dos 2.041.816 eventos. Nas demais linhas cz_fips identifica uma zona de previsão (Z) ou marítima (M) e não há condado. Dos códigos formados, 99,51% constam do diretório br_bd_diretorios_us.county; os 6.379 restantes são códigos históricos ou de zona, com destaque para os condados de Connecticut (09001 a 09015), que a fonte segue usando embora o diretório traga apenas as regiões de planejamento adotadas em 2022",
            "Populated only when cz_type = C, that is, when cz_fips really is a county code: 1,301,381 of 2,041,816 events. On the remaining rows cz_fips identifies a forecast (Z) or marine (M) zone and there is no county. Of the codes built, 99.51% are present in the br_bd_diretorios_us.county directory; the remaining 6,379 are historical or zone codes, chiefly the Connecticut counties (09001 to 09015), which the source still uses although the directory carries only the planning regions adopted in 2022",
            "Completado solo cuando cz_type = C, es decir, cuando cz_fips es efectivamente un código de condado: 1.301.381 de 2.041.816 eventos. En las demás filas cz_fips identifica una zona de pronóstico (Z) o marítima (M) y no hay condado. De los códigos formados, 99,51% constan en el directorio br_bd_diretorios_us.county; los 6.379 restantes son códigos históricos o de zona, en particular los condados de Connecticut (09001 a 09015), que la fuente sigue usando aunque el directorio traiga solo las regiones de planificación adoptadas en 2022",
        ),
    ),
    col(
        "state_name",
        "STRING",
        "Nome do estado, território ou zona marítima onde o evento ocorreu, conforme publicado",
        "Name of the state, territory or marine zone where the event occurred, as published",
        "Nombre del estado, territorio o zona marítima donde ocurrió el evento, tal como se publica",
        original="STATE",
        obs=(
            "Preservado exatamente como publicado, em caixa alta. Além dos estados e territórios, inclui 14 zonas de água — GULF OF MEXICO, ATLANTIC NORTH, ATLANTIC SOUTH, E PACIFIC, os cinco Grandes Lagos, LAKE ST CLAIR, ST LAWRENCE R, GULF OF ALASKA, HAWAII WATERS e GUAM WATERS",
            "Preserved exactly as published, in upper case. Besides the states and territories it includes 14 water zones — GULF OF MEXICO, ATLANTIC NORTH, ATLANTIC SOUTH, E PACIFIC, the five Great Lakes, LAKE ST CLAIR, ST LAWRENCE R, GULF OF ALASKA, HAWAII WATERS and GUAM WATERS",
            "Preservado exactamente como se publica, en mayúsculas. Además de los estados y territorios, incluye 14 zonas de agua — GULF OF MEXICO, ATLANTIC NORTH, ATLANTIC SOUTH, E PACIFIC, los cinco Grandes Lagos, LAKE ST CLAIR, ST LAWRENCE R, GULF OF ALASKA, HAWAII WATERS y GUAM WATERS",
        ),
    ),
    col(
        "state_fips_nws",
        "STRING",
        "Código de estado atribuído pelo NWS, conforme publicado",
        "State code assigned by the NWS, as published",
        "Código de estado asignado por el NWS, tal como se publica",
        original="STATE_FIPS",
        obs=(
            "Coincide com o código FIPS nos 50 estados e no Distrito de Columbia, mas não nos territórios nem nas zonas marítimas. Mantido ao lado de state_id para que o valor de origem não se perca",
            "Matches the FIPS code for the 50 states and the District of Columbia, but not for the territories or the marine zones. Kept alongside state_id so the source value is not lost",
            "Coincide con el código FIPS en los 50 estados y en el Distrito de Columbia, pero no en los territorios ni en las zonas marítimas. Mantenido junto a state_id para que el valor de origen no se pierda",
        ),
    ),
    col(
        "cz_type",
        "STRING",
        "Indica se o evento foi registrado por condado (C), por zona de previsão do NWS (Z) ou por zona marítima (M)",
        "Indicates whether the event was recorded by county (C), by NWS forecast zone (Z) or by marine zone (M)",
        "Indica si el evento se registró por condado (C), por zona de pronóstico del NWS (Z) o por zona marítima (M)",
        dict_=True,
        original="CZ_TYPE",
        obs=(
            "Determina como cz_fips deve ser lido e é a condição para county_id ser preenchido. Uma linha de 1996 traz o valor 5, ausente do vocabulário e preservado como publicado",
            "Determines how cz_fips is to be read and is the condition for county_id to be populated. One 1996 row carries the value 5, absent from the vocabulary and preserved as published",
            "Determina cómo debe leerse cz_fips y es la condición para que county_id se complete. Una fila de 1996 trae el valor 5, ausente del vocabulario y preservado tal como se publica",
        ),
    ),
    col(
        "cz_fips",
        "STRING",
        "Código do condado ou da zona de previsão onde o evento ocorreu, conforme publicado",
        "Code of the county or forecast zone where the event occurred, as published",
        "Código del condado o de la zona de pronóstico donde ocurrió el evento, tal como se publica",
        original="CZ_FIPS",
        obs=(
            "Código de condado quando cz_type = C e número de zona do NWS quando cz_type = Z ou M. O valor 0 aparece em 1.667 registros e indica ocorrência de âmbito estadual, sem condado",
            "County code when cz_type = C and NWS zone number when cz_type = Z or M. The value 0 appears on 1,667 records and marks a state-wide occurrence with no county",
            "Código de condado cuando cz_type = C y número de zona del NWS cuando cz_type = Z o M. El valor 0 aparece en 1.667 registros e indica una ocurrencia de ámbito estatal, sin condado",
        ),
    ),
    col(
        "cz_name",
        "STRING",
        "Nome do condado, da zona de previsão ou da zona marítima onde o evento ocorreu",
        "Name of the county, forecast zone or marine zone where the event occurred",
        "Nombre del condado, de la zona de pronóstico o de la zona marítima donde ocurrió el evento",
        original="CZ_NAME",
    ),
    col(
        "wfo",
        "STRING",
        "Sigla do escritório de previsão do NWS responsável pela área em que o evento ocorreu",
        "Code of the NWS forecast office responsible for the area in which the event occurred",
        "Sigla de la oficina de pronóstico del NWS responsable del área en que ocurrió el evento",
        original="WFO",
    ),
    col(
        "event_type",
        "STRING",
        "Tipo de evento meteorológico",
        "Type of weather event",
        "Tipo de evento meteorológico",
        dict_=True,
        original="EVENT_TYPE",
        obs=(
            "A cobertura por tipo não é uniforme ao longo do tempo: apenas tornados são registrados de 1950 a 1954; tornado, vento de tempestade e granizo de 1955 a 1995; e o vocabulário completo somente a partir de 1996. Contar eventos por ano sem separar por tipo produz uma série sem sentido. O registro traz 57 valores distintos, contra os 48 previstos na diretiva NWS 10-1605, incluindo grafias concorrentes (Volcanic Ash e Volcanic Ashfall; Hurricane (Typhoon) e Hurricane) e valores fora do vocabulário (Northern Lights, em 2001). Os valores são preservados como publicados; a tabela dicionario registra os anos em que cada um aparece",
            "Coverage by type is not uniform over time: only tornadoes are recorded from 1950 to 1954; tornado, thunderstorm wind and hail from 1955 to 1995; and the full vocabulary only from 1996 on. Counting events per year without separating by type produces a meaningless series. The record carries 57 distinct values, against the 48 the NWS directive 10-1605 defines, including competing spellings (Volcanic Ash and Volcanic Ashfall; Hurricane (Typhoon) and Hurricane) and values outside the vocabulary (Northern Lights, in 2001). Values are preserved as published; the dicionario table records the years over which each appears",
            "La cobertura por tipo no es uniforme a lo largo del tiempo: solo se registran tornados de 1950 a 1954; tornado, viento de tormenta y granizo de 1955 a 1995; y el vocabulario completo solo a partir de 1996. Contar eventos por año sin separar por tipo produce una serie sin sentido. El registro trae 57 valores distintos, frente a los 48 previstos en la directiva NWS 10-1605, incluyendo grafías concurrentes (Volcanic Ash y Volcanic Ashfall; Hurricane (Typhoon) y Hurricane) y valores fuera del vocabulario (Northern Lights, en 2001). Los valores se preservan tal como se publican; la tabla dicionario registra los años en que aparece cada uno",
        ),
    ),
    col(
        "begin_datetime",
        "DATETIME",
        "Data e hora do início do evento, no fuso horário local indicado em timezone",
        "Date and time the event began, in the local time zone given in timezone",
        "Fecha y hora de inicio del evento, en el huso horario local indicado en timezone",
        original="BEGIN_YEARMONTH",
        obs=(
            "Construído a partir dos campos numéricos BEGIN_YEARMONTH, BEGIN_DAY e BEGIN_TIME, e não do campo textual BEGIN_DATE_TIME, que traz o ano com dois dígitos (28-APR-50) e por isso não distingue 1950 de 2050",
            "Built from the numeric fields BEGIN_YEARMONTH, BEGIN_DAY and BEGIN_TIME, not from the text field BEGIN_DATE_TIME, which carries a two-digit year (28-APR-50) and therefore cannot tell 1950 from 2050",
            "Construido a partir de los campos numéricos BEGIN_YEARMONTH, BEGIN_DAY y BEGIN_TIME, y no del campo textual BEGIN_DATE_TIME, que trae el año con dos dígitos (28-APR-50) y por eso no distingue 1950 de 2050",
        ),
    ),
    col(
        "end_datetime",
        "DATETIME",
        "Data e hora do fim do evento, no fuso horário local indicado em timezone",
        "Date and time the event ended, in the local time zone given in timezone",
        "Fecha y hora de finalización del evento, en el huso horario local indicado en timezone",
        original="END_YEARMONTH",
        obs=(
            "Construído a partir dos campos numéricos END_YEARMONTH, END_DAY e END_TIME, pelo mesmo motivo descrito em begin_datetime",
            "Built from the numeric fields END_YEARMONTH, END_DAY and END_TIME, for the reason given under begin_datetime",
            "Construido a partir de los campos numéricos END_YEARMONTH, END_DAY y END_TIME, por el mismo motivo descrito en begin_datetime",
        ),
    ),
    col(
        "timezone",
        "STRING",
        "Fuso horário local a que se referem begin_datetime e end_datetime",
        "Local time zone to which begin_datetime and end_datetime refer",
        "Huso horario local al que se refieren begin_datetime y end_datetime",
        original="CZ_TIMEZONE",
        obs=(
            "Publicado em duas formas ao longo do período, com e sem a diferença em relação ao UTC (CST-6 e CST), e ocasionalmente com a sigla de horário de verão (CDT). Preservado como publicado",
            "Published in two forms over the record, with and without the offset from UTC (CST-6 and CST), and occasionally with the daylight saving abbreviation (CDT). Preserved as published",
            "Publicado en dos formas a lo largo del período, con y sin la diferencia respecto al UTC (CST-6 y CST), y ocasionalmente con la sigla de horario de verano (CDT). Preservado tal como se publica",
        ),
    ),
    col(
        "injuries_direct",
        "INT64",
        "Número de feridos causados diretamente pelo evento",
        "Number of injuries caused directly by the event",
        "Número de heridos causados directamente por el evento",
        unit="person",
        original="INJURIES_DIRECT",
    ),
    col(
        "injuries_indirect",
        "INT64",
        "Número de feridos causados indiretamente pelo evento",
        "Number of injuries caused indirectly by the event",
        "Número de heridos causados indirectamente por el evento",
        unit="person",
        original="INJURIES_INDIRECT",
    ),
    col(
        "deaths_direct",
        "INT64",
        "Número de mortes causadas diretamente pelo evento",
        "Number of deaths caused directly by the event",
        "Número de muertes causadas directamente por el evento",
        unit="person",
        original="DEATHS_DIRECT",
    ),
    col(
        "deaths_indirect",
        "INT64",
        "Número de mortes causadas indiretamente pelo evento",
        "Number of deaths caused indirectly by the event",
        "Número de muertes causadas indirectamente por el evento",
        unit="person",
        original="DEATHS_INDIRECT",
    ),
    col(
        "damage_property",
        "FLOAT64",
        "Valor estimado dos danos materiais causados pelo evento, em dólares correntes",
        "Estimated value of property damage caused by the event, in current dollars",
        "Valor estimado de los daños materiales causados por el evento, en dólares corrientes",
        unit="USD",
        original="DAMAGE_PROPERTY",
        obs=(
            "A fonte publica este campo como texto com sufixo de magnitude (2.5K, 1.50M, 10.00B, e também .5K sem o zero à esquerda), o que faz uma conversão numérica direta produzir nulo ou o valor 2,5 no lugar de 2.500. Aqui o texto é decodificado para dólares e a forma original é mantida em damage_property_source. Valores em dólares correntes do ano do evento, sem correção pela inflação. Nulo em 620.474 registros sem valor informado e em 41 registros cujo texto não permite decodificação (sufixos H e T, ou sufixo sem número)",
            "The source publishes this field as text with a magnitude suffix (2.5K, 1.50M, 10.00B, and also .5K with no leading zero), which makes a direct numeric conversion yield null or the value 2.5 instead of 2,500. Here the text is decoded to dollars and the original form is kept in damage_property_source. Values are in current dollars of the event's year, not inflation-adjusted. Null on 620,474 records with no reported value and on 41 records whose text cannot be decoded (H and T suffixes, or a suffix with no number)",
            "La fuente publica este campo como texto con sufijo de magnitud (2.5K, 1.50M, 10.00B, y también .5K sin el cero a la izquierda), lo que hace que una conversión numérica directa produzca nulo o el valor 2,5 en lugar de 2.500. Aquí el texto se decodifica a dólares y la forma original se mantiene en damage_property_source. Valores en dólares corrientes del año del evento, sin corrección por inflación. Nulo en 620.474 registros sin valor informado y en 41 registros cuyo texto no permite decodificación (sufijos H y T, o sufijo sin número)",
        ),
    ),
    col(
        "damage_property_source",
        "STRING",
        "Valor dos danos materiais na forma textual publicada pela fonte, com o sufixo de magnitude",
        "Property damage value in the text form published by the source, with its magnitude suffix",
        "Valor de los daños materiales en la forma textual publicada por la fuente, con el sufijo de magnitud",
        original="DAMAGE_PROPERTY",
        obs=(
            "Mantido para que a decodificação feita em damage_property seja auditável e para que nenhum valor de origem se perca",
            "Kept so that the decoding done in damage_property is auditable and so that no source value is lost",
            "Mantenido para que la decodificación hecha en damage_property sea auditable y para que ningún valor de origen se pierda",
        ),
    ),
    col(
        "damage_crops",
        "FLOAT64",
        "Valor estimado dos danos às lavouras causados pelo evento, em dólares correntes",
        "Estimated value of crop damage caused by the event, in current dollars",
        "Valor estimado de los daños a los cultivos causados por el evento, en dólares corrientes",
        unit="USD",
        original="DAMAGE_CROPS",
        obs=(
            "Decodificado da mesma forma que damage_property, com a forma original preservada em damage_crops_source. Valores em dólares correntes do ano do evento. Nulo em 732.809 registros sem valor informado e em 8 registros cujo texto não permite decodificação",
            "Decoded in the same way as damage_property, with the original form preserved in damage_crops_source. Values are in current dollars of the event's year. Null on 732,809 records with no reported value and on 8 records whose text cannot be decoded",
            "Decodificado de la misma forma que damage_property, con la forma original preservada en damage_crops_source. Valores en dólares corrientes del año del evento. Nulo en 732.809 registros sin valor informado y en 8 registros cuyo texto no permite decodificación",
        ),
    ),
    col(
        "damage_crops_source",
        "STRING",
        "Valor dos danos às lavouras na forma textual publicada pela fonte, com o sufixo de magnitude",
        "Crop damage value in the text form published by the source, with its magnitude suffix",
        "Valor de los daños a los cultivos en la forma textual publicada por la fuente, con el sufijo de magnitud",
        original="DAMAGE_CROPS",
    ),
    col(
        "source",
        "STRING",
        "Origem do relato do evento",
        "Source that reported the event",
        "Origen del relato del evento",
        original="SOURCE",
        obs=(
            "Campo de texto livre no sistema de origem, sem vocabulário controlado",
            "Free-text field in the source system, with no controlled vocabulary",
            "Campo de texto libre en el sistema de origen, sin vocabulario controlado",
        ),
    ),
    col(
        "magnitude",
        "FLOAT64",
        "Magnitude medida ou estimada do evento",
        "Measured or estimated magnitude of the event",
        "Magnitud medida o estimada del evento",
        original="MAGNITUDE",
        obs=(
            "A unidade depende do tipo de evento e por isso não é declarada no campo measurement_unit: nós para velocidade de vento e polegadas para diâmetro de granizo. Preenchido apenas para eventos de vento e de granizo",
            "The unit depends on the event type and is therefore not declared in the measurement_unit field: knots for wind speed and inches for hail diameter. Populated only for wind and hail events",
            "La unidad depende del tipo de evento y por eso no se declara en el campo measurement_unit: nudos para velocidad de viento y pulgadas para diámetro de granizo. Completado solo para eventos de viento y de granizo",
        ),
    ),
    col(
        "magnitude_type",
        "STRING",
        "Forma de obtenção da magnitude do vento",
        "How the wind magnitude was obtained",
        "Forma de obtención de la magnitud del viento",
        dict_=True,
        original="MAGNITUDE_TYPE",
        obs=(
            "A documentação da fonte prevê EG, ES, MS e MG; o registro também traz E e M, presentes em 31.549 linhas e preservados como publicados",
            "The source documentation defines EG, ES, MS and MG; the record also carries E and M, present on 31,549 rows and preserved as published",
            "La documentación de la fuente prevé EG, ES, MS y MG; el registro también trae E y M, presentes en 31.549 filas y preservados tal como se publican",
        ),
    ),
    col(
        "flood_cause",
        "STRING",
        "Causa relatada ou estimada da inundação",
        "Reported or estimated cause of the flood",
        "Causa relatada o estimada de la inundación",
        dict_=True,
        original="FLOOD_CAUSE",
    ),
    col(
        "hurricane_category",
        "STRING",
        "Categoria do furacão na escala Saffir-Simpson",
        "Hurricane category on the Saffir-Simpson scale",
        "Categoría del huracán en la escala Saffir-Simpson",
        original="CATEGORY",
        obs=(
            "Preenchido em 561 registros. Mantido como texto por ser um rótulo de categoria, e não uma quantidade sobre a qual faça sentido calcular",
            "Populated on 561 records. Kept as text because it is a category label, not a quantity it makes sense to compute with",
            "Completado en 561 registros. Mantenido como texto por ser una etiqueta de categoría, y no una cantidad sobre la que tenga sentido calcular",
        ),
    ),
    col(
        "tornado_scale",
        "STRING",
        "Intensidade do tornado na escala Fujita ou Fujita Melhorada",
        "Tornado intensity on the Fujita or Enhanced Fujita scale",
        "Intensidad del tornado en la escala Fujita o Fujita Mejorada",
        dict_=True,
        original="TOR_F_SCALE",
        obs=(
            "Duas escalas convivem na mesma coluna: F0 a F5 até 2006 e EF0 a EF5 a partir de 2007, quando a escala Fujita Melhorada passou a ser usada nos Estados Unidos. EFU indica tornado de intensidade não determinada. Os graus não são equivalentes entre as escalas e não devem ser somados sem essa ressalva",
            "Two scales share the column: F0 to F5 up to 2006 and EF0 to EF5 from 2007 on, when the Enhanced Fujita scale came into use in the United States. EFU marks a tornado of undetermined intensity. The grades are not equivalent across the two scales and should not be pooled without that caveat",
            "Dos escalas conviven en la misma columna: F0 a F5 hasta 2006 y EF0 a EF5 a partir de 2007, cuando la escala Fujita Mejorada pasó a usarse en los Estados Unidos. EFU indica un tornado de intensidad no determinada. Los grados no son equivalentes entre las escalas y no deben sumarse sin esa salvedad",
        ),
    ),
    col(
        "tornado_length",
        "FLOAT64",
        "Extensão do trajeto percorrido pelo tornado em solo",
        "Length of the path the tornado travelled on the ground",
        "Extensión del trayecto recorrido por el tornado en tierra",
        unit="mile",
        original="TOR_LENGTH",
    ),
    col(
        "tornado_width",
        "FLOAT64",
        "Largura do trajeto percorrido pelo tornado em solo",
        "Width of the path the tornado travelled on the ground",
        "Ancho del trayecto recorrido por el tornado en tierra",
        unit="yard",
        original="TOR_WIDTH",
    ),
    col(
        "tornado_other_wfo",
        "STRING",
        "Sigla do escritório de previsão do NWS para o qual o tornado seguiu ao cruzar a fronteira da área de responsabilidade",
        "Code of the NWS forecast office the tornado continued into on crossing the boundary of the area of responsibility",
        "Sigla de la oficina de pronóstico del NWS hacia la cual siguió el tornado al cruzar la frontera del área de responsabilidad",
        original="TOR_OTHER_WFO",
    ),
    col(
        "tornado_other_state_abbreviation",
        "STRING",
        "Sigla de duas letras do estado para o qual o tornado seguiu",
        "Two-letter abbreviation of the state the tornado continued into",
        "Sigla de dos letras del estado hacia el cual siguió el tornado",
        original="TOR_OTHER_CZ_STATE",
    ),
    col(
        "tornado_other_cz_fips",
        "STRING",
        "Código do condado para o qual o tornado seguiu",
        "Code of the county the tornado continued into",
        "Código del condado hacia el cual siguió el tornado",
        original="TOR_OTHER_CZ_FIPS",
        obs=(
            "Publicado sem o código do estado, e por isso não formando um código FIPS de cinco posições nem ligando ao diretório de condados",
            "Published without the state code, and therefore neither forming a five-digit FIPS code nor linking to the county directory",
            "Publicado sin el código del estado, y por eso no forma un código FIPS de cinco posiciones ni enlaza con el directorio de condados",
        ),
    ),
    col(
        "tornado_other_cz_name",
        "STRING",
        "Nome do condado para o qual o tornado seguiu",
        "Name of the county the tornado continued into",
        "Nombre del condado hacia el cual siguió el tornado",
        original="TOR_OTHER_CZ_NAME",
    ),
    col(
        "begin_range",
        "FLOAT64",
        "Distância entre o ponto inicial do evento e a localidade de referência indicada em begin_location",
        "Distance from the event's begin point to the reference locality given in begin_location",
        "Distancia entre el punto inicial del evento y la localidad de referencia indicada en begin_location",
        unit="mile",
        original="BEGIN_RANGE",
    ),
    col(
        "begin_azimuth",
        "STRING",
        "Direção do ponto inicial do evento em relação à localidade de referência, na rosa dos ventos de 16 pontos",
        "Direction of the event's begin point from the reference locality, on the 16-point compass",
        "Dirección del punto inicial del evento respecto a la localidad de referencia, en la rosa de los vientos de 16 puntos",
        original="BEGIN_AZIMUTH",
    ),
    col(
        "begin_location",
        "STRING",
        "Localidade de referência a partir da qual se medem begin_range e begin_azimuth",
        "Reference locality from which begin_range and begin_azimuth are measured",
        "Localidad de referencia a partir de la cual se miden begin_range y begin_azimuth",
        original="BEGIN_LOCATION",
    ),
    col(
        "end_range",
        "FLOAT64",
        "Distância entre o ponto final do evento e a localidade de referência indicada em end_location",
        "Distance from the event's end point to the reference locality given in end_location",
        "Distancia entre el punto final del evento y la localidad de referencia indicada en end_location",
        unit="mile",
        original="END_RANGE",
    ),
    col(
        "end_azimuth",
        "STRING",
        "Direção do ponto final do evento em relação à localidade de referência, na rosa dos ventos de 16 pontos",
        "Direction of the event's end point from the reference locality, on the 16-point compass",
        "Dirección del punto final del evento respecto a la localidad de referencia, en la rosa de los vientos de 16 puntos",
        original="END_AZIMUTH",
    ),
    col(
        "end_location",
        "STRING",
        "Localidade de referência a partir da qual se medem end_range e end_azimuth",
        "Reference locality from which end_range and end_azimuth are measured",
        "Localidad de referencia a partir de la cual se miden end_range y end_azimuth",
        original="END_LOCATION",
    ),
    col(
        "begin_latitude",
        "FLOAT64",
        "Latitude do ponto inicial do evento, em graus decimais",
        "Latitude of the event's begin point, in decimal degrees",
        "Latitud del punto inicial del evento, en grados decimales",
        unit="degree",
        original="BEGIN_LAT",
        obs=(
            "Vazio em 794.846 registros. Um registro de 1997 traz latitude 97,1, fora do intervalo possível; o valor é preservado como publicado",
            "Empty on 794,846 records. One 1997 record carries latitude 97.1, outside the possible range; the value is preserved as published",
            "Vacío en 794.846 registros. Un registro de 1997 trae latitud 97,1, fuera del intervalo posible; el valor se preserva tal como se publica",
        ),
    ),
    col(
        "begin_longitude",
        "FLOAT64",
        "Longitude do ponto inicial do evento, em graus decimais",
        "Longitude of the event's begin point, in decimal degrees",
        "Longitud del punto inicial del evento, en grados decimales",
        unit="degree",
        original="BEGIN_LON",
    ),
    col(
        "end_latitude",
        "FLOAT64",
        "Latitude do ponto final do evento, em graus decimais",
        "Latitude of the event's end point, in decimal degrees",
        "Latitud del punto final del evento, en grados decimales",
        unit="degree",
        original="END_LAT",
    ),
    col(
        "end_longitude",
        "FLOAT64",
        "Longitude do ponto final do evento, em graus decimais",
        "Longitude of the event's end point, in decimal degrees",
        "Longitud del punto final del evento, en grados decimales",
        unit="degree",
        original="END_LON",
    ),
    col(
        "episode_narrative",
        "STRING",
        "Descrição do episódio meteorológico a que o evento pertence, redigida pelo NWS",
        "Description of the weather episode the event belongs to, written by the NWS",
        "Descripción del episodio meteorológico al que pertenece el evento, redactada por el NWS",
        original="EPISODE_NARRATIVE",
    ),
    col(
        "event_narrative",
        "STRING",
        "Descrição do evento, redigida pelo NWS",
        "Description of the event, written by the NWS",
        "Descripción del evento, redactada por el NWS",
        original="EVENT_NARRATIVE",
    ),
    col(
        "data_source",
        "STRING",
        "Meio pelo qual o registro entrou na base",
        "Route by which the record entered the database",
        "Medio por el cual el registro entró en la base",
        dict_=True,
        original="DATA_SOURCE",
    ),
]

# --------------------------------------------------------------------------
# fatality — one row per death attributed to an event
# --------------------------------------------------------------------------

FATALITY = [
    col(
        "year",
        "INT64",
        "Ano-calendário do evento a que a morte é atribuída",
        "Calendar year of the event the death is attributed to",
        "Año calendario del evento al que se atribuye la muerte",
        directory=DIR_YEAR,
        unit="year",
        original="FAT_YEARMONTH",
        obs=(
            "Coluna de particionamento; corresponde ao ano do arquivo anual de origem",
            "Partition column; matches the year of the source annual file",
            "Columna de particionamiento; coincide con el año del archivo anual de origen",
        ),
    ),
    col(
        "event_id",
        "STRING",
        "Identificador do evento a que a morte é atribuída, atribuído pelo NWS",
        "Identifier of the event the death is attributed to, assigned by the NWS",
        "Identificador del evento al que se atribuye la muerte, asignado por el NWS",
        original="EVENT_ID",
        obs=(
            "Chave de ligação com a tabela event; todos os 24.903 registros têm evento correspondente",
            "Join key to the event table; all 24,903 records have a matching event",
            "Clave de unión con la tabla event; los 24.903 registros tienen evento correspondiente",
        ),
    ),
    col(
        "fatality_id",
        "STRING",
        "Identificador da morte atribuído pelo NWS",
        "Identifier of the death assigned by the NWS",
        "Identificador de la muerte asignado por el NWS",
        original="FATALITY_ID",
        obs=(
            "Não é único em todo o período: 914 identificadores se repetem entre mortes distintas de anos diferentes, porque a numeração foi reiniciada pela fonte. A chave primária da tabela é o par (event_id, fatality_id), que não apresenta repetição",
            "Not unique across the whole record: 914 identifiers repeat between distinct deaths in different years, because the source restarted the numbering. The table's primary key is the pair (event_id, fatality_id), which has no repeats",
            "No es único en todo el período: 914 identificadores se repiten entre muertes distintas de años diferentes, porque la numeración fue reiniciada por la fuente. La clave primaria de la tabla es el par (event_id, fatality_id), que no presenta repetición",
        ),
    ),
    col(
        "fatality_datetime",
        "DATETIME",
        "Data e hora da morte",
        "Date and time of the death",
        "Fecha y hora de la muerte",
        original="FAT_YEARMONTH",
        obs=(
            "Construído a partir dos campos numéricos FAT_YEARMONTH, FAT_DAY e FAT_TIME. O campo textual FATALITY_DATE usa o formato MM/DD/AAAA, diferente do formato DD-MON-AA usado no arquivo de eventos. A hora é 00:00 na maioria dos registros, sem indicar meia-noite. Nulo quando o dia não é informado",
            "Built from the numeric fields FAT_YEARMONTH, FAT_DAY and FAT_TIME. The text field FATALITY_DATE uses MM/DD/YYYY, a different format from the DD-MON-YY used in the events file. The time is 00:00 on most records, without meaning midnight. Null when the day is not reported",
            "Construido a partir de los campos numéricos FAT_YEARMONTH, FAT_DAY y FAT_TIME. El campo textual FATALITY_DATE usa el formato MM/DD/AAAA, distinto del formato DD-MON-AA usado en el archivo de eventos. La hora es 00:00 en la mayoría de los registros, sin indicar medianoche. Nulo cuando no se informa el día",
        ),
    ),
    col(
        "fatality_type",
        "STRING",
        "Indica se a morte foi causada direta (D) ou indiretamente (I) pelo evento",
        "Indicates whether the death was caused directly (D) or indirectly (I) by the event",
        "Indica si la muerte fue causada directa (D) o indirectamente (I) por el evento",
        dict_=True,
        original="FATALITY_TYPE",
    ),
    col(
        "age",
        "INT64",
        "Idade da vítima em anos completos",
        "Age of the victim in completed years",
        "Edad de la víctima en años cumplidos",
        unit="year",
        original="FATALITY_AGE",
        sensitive=True,
        obs=(
            "Não informado em 5.363 dos 24.903 registros",
            "Not reported on 5,363 of the 24,903 records",
            "No informado en 5.363 de los 24.903 registros",
        ),
    ),
    col(
        "sex",
        "STRING",
        "Sexo da vítima",
        "Sex of the victim",
        "Sexo de la víctima",
        dict_=True,
        original="FATALITY_SEX",
        sensitive=True,
        obs=(
            "Não informado em 4.129 dos 24.903 registros",
            "Not reported on 4,129 of the 24,903 records",
            "No informado en 4.129 de los 24.903 registros",
        ),
    ),
    col(
        "location",
        "STRING",
        "Tipo de local em que a vítima se encontrava",
        "Type of place the victim was in",
        "Tipo de lugar en que se encontraba la víctima",
        dict_=True,
        original="FATALITY_LOCATION",
        obs=(
            "A fonte publica o rótulo por extenso, e não o código de duas letras descrito na documentação. Rótulos concorrentes convivem no registro (Boating e Boat; Unknown, Other e valor vazio) e são preservados como publicados",
            "The source publishes the full label, not the two-letter code its documentation describes. Competing labels coexist in the record (Boating and Boat; Unknown, Other and the empty value) and are preserved as published",
            "La fuente publica la etiqueta completa, y no el código de dos letras descrito en la documentación. Etiquetas concurrentes conviven en el registro (Boating y Boat; Unknown, Other y valor vacío) y se preservan tal como se publican",
        ),
    ),
]

# --------------------------------------------------------------------------
# event_location — one row per point location attached to an event
# --------------------------------------------------------------------------

EVENT_LOCATION = [
    col(
        "year",
        "INT64",
        "Ano-calendário do evento a que a localização pertence",
        "Calendar year of the event the location belongs to",
        "Año calendario del evento al que pertenece la ubicación",
        directory=DIR_YEAR,
        unit="year",
        original="YEARMONTH",
        obs=(
            "Coluna de particionamento; corresponde ao ano do arquivo anual de origem",
            "Partition column; matches the year of the source annual file",
            "Columna de particionamiento; coincide con el año del archivo anual de origen",
        ),
    ),
    col(
        "event_id",
        "STRING",
        "Identificador do evento a que a localização pertence, atribuído pelo NWS",
        "Identifier of the event the location belongs to, assigned by the NWS",
        "Identificador del evento al que pertenece la ubicación, asignado por el NWS",
        original="EVENT_ID",
        obs=(
            "Chave de ligação com a tabela event; todos os 1.817.621 registros têm evento correspondente",
            "Join key to the event table; all 1,817,621 records have a matching event",
            "Clave de unión con la tabla event; los 1.817.621 registros tienen evento correspondiente",
        ),
    ),
    col(
        "episode_id",
        "STRING",
        "Identificador do episódio a que o evento pertence, atribuído pelo NWS",
        "Identifier of the episode the event belongs to, assigned by the NWS",
        "Identificador del episodio al que pertenece el evento, asignado por el NWS",
        original="EPISODE_ID",
    ),
    col(
        "location_index",
        "STRING",
        "Número de ordem da localização dentro do mesmo evento",
        "Sequence number of the location within the same event",
        "Número de orden de la ubicación dentro del mismo evento",
        original="LOCATION_INDEX",
        obs=(
            "Forma com event_id a chave primária da tabela. Mantido como texto por ser um número de ordem, sobre o qual não faz sentido calcular",
            "Together with event_id it forms the table's primary key. Kept as text because it is a sequence number, which it makes no sense to compute with",
            "Forma con event_id la clave primaria de la tabla. Mantenido como texto por ser un número de orden, sobre el cual no tiene sentido calcular",
        ),
    ),
    col(
        "location_range",
        "FLOAT64",
        "Distância entre o ponto registrado e a localidade de referência indicada em location_name",
        "Distance from the recorded point to the reference locality given in location_name",
        "Distancia entre el punto registrado y la localidad de referencia indicada en location_name",
        unit="mile",
        original="RANGE",
        obs=(
            "Renomeado a partir de RANGE, que é palavra reservada em BigQuery",
            "Renamed from RANGE, which is a reserved word in BigQuery",
            "Renombrado a partir de RANGE, que es palabra reservada en BigQuery",
        ),
    ),
    col(
        "location_azimuth",
        "STRING",
        "Direção do ponto registrado em relação à localidade de referência, na rosa dos ventos de 16 pontos",
        "Direction of the recorded point from the reference locality, on the 16-point compass",
        "Dirección del punto registrado respecto a la localidad de referencia, en la rosa de los vientos de 16 puntos",
        original="AZIMUTH",
    ),
    col(
        "location_name",
        "STRING",
        "Localidade de referência a partir da qual se medem location_range e location_azimuth",
        "Reference locality from which location_range and location_azimuth are measured",
        "Localidad de referencia a partir de la cual se miden location_range y location_azimuth",
        original="LOCATION",
    ),
    col(
        "latitude",
        "FLOAT64",
        "Latitude do ponto registrado, em graus decimais",
        "Latitude of the recorded point, in decimal degrees",
        "Latitud del punto registrado, en grados decimales",
        unit="degree",
        original="LATITUDE",
        obs=(
            "Vazio em 261.643 registros. Um registro de 1997 traz latitude 97,1, fora do intervalo possível; o valor é preservado como publicado",
            "Empty on 261,643 records. One 1997 record carries latitude 97.1, outside the possible range; the value is preserved as published",
            "Vacío en 261.643 registros. Un registro de 1997 trae latitud 97,1, fuera del intervalo posible; el valor se preserva tal como se publica",
        ),
    ),
    col(
        "longitude",
        "FLOAT64",
        "Longitude do ponto registrado, em graus decimais",
        "Longitude of the recorded point, in decimal degrees",
        "Longitud del punto registrado, en grados decimales",
        unit="degree",
        original="LONGITUDE",
    ),
]

TABLES = {
    "event": EVENT,
    "fatality": FATALITY,
    "event_location": EVENT_LOCATION,
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        names = [c["name"] for c in cols]
        assert len(names) == len(set(names)), (
            f"duplicate column name in {table}"
        )
        for c in cols:
            for lang in ("pt", "en", "es"):
                d = c[f"description_{lang}"]
                assert d and d[0].isupper(), (
                    f"{table}.{c['name']} {lang}: capitalise"
                )
                assert not d.endswith("."), (
                    f"{table}.{c['name']} {lang}: no full stop"
                )
            if c["bigquery_type"] in ("INT64", "FLOAT64"):
                # magnitude is the one documented exception: its unit depends on
                # the event type, so declaring a single one would be wrong.
                assert c["measurement_unit"] or c["name"] == "magnitude", (
                    f"{table}.{c['name']}: numeric column needs a measurement_unit"
                )
        path = OUT / f"sheet_{table}.csv"
        with open(path, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            w.writerows(cols)
        print(f"wrote {path} ({len(cols)} columns)")


if __name__ == "__main__":
    main()
