"""Write the architecture CSVs for world_noaa_ghcn.

The architecture is the source of truth: clean.py takes its column order from
here, gen_dbt.py generates the models from here, and register_metadata.py
registers columns from here.
"""

import csv
from pathlib import Path

HEADER = [
    "name",
    "bigquery_type",
    "description",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]


def col(
    name, typ, pt, en, es, *, dict_="no", unit="", obs="", orig="", cov=""
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": cov,
        "covered_by_dictionary": dict_,
        "directory_column": "",
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": orig,
    }


STATION = [
    col(
        "station_id",
        "STRING",
        "Código de identificação da estação no GHCN-Daily",
        "GHCN-Daily station identification code",
        "Código de identificación de la estación en GHCN-Daily",
        obs="Chave primária da tabela. Os dois primeiros caracteres são o código FIPS do país, o terceiro identifica a rede de numeração. | Primary key of the table. The first two characters are the FIPS country code and the third identifies the station numbering network. | Clave primaria de la tabla. Los dos primeros caracteres son el código FIPS del país y el tercero identifica la red de numeración.",
        orig="ID",
    ),
    col(
        "country_code",
        "STRING",
        "Código FIPS do país onde a estação está localizada",
        "FIPS code of the country where the station is located",
        "Código FIPS del país donde se ubica la estación",
        obs="Derivado dos dois primeiros caracteres de station_id. | Derived from the first two characters of station_id. | Derivado de los dos primeros caracteres de station_id.",
    ),
    col(
        "country_name",
        "STRING",
        "Nome do país onde a estação está localizada",
        "Name of the country where the station is located",
        "Nombre del país donde se ubica la estación",
        orig="ghcnd-countries.txt NAME",
    ),
    col(
        "network_code",
        "STRING",
        "Código da rede de numeração que atribuiu o identificador da estação",
        "Code of the numbering network that assigned the station identifier",
        "Código de la red de numeración que asignó el identificador de la estación",
        dict_="yes",
        obs="Terceiro caractere de station_id. | Third character of station_id. | Tercer carácter de station_id.",
    ),
    col(
        "state_code",
        "STRING",
        "Código postal do estado ou província, apenas para estações dos Estados Unidos e do Canadá",
        "Postal code of the state or province, for United States and Canadian stations only",
        "Código postal del estado o provincia, solo para estaciones de Estados Unidos y Canadá",
        orig="STATE",
    ),
    col(
        "state_name",
        "STRING",
        "Nome do estado, território ou província",
        "Name of the state, territory or province",
        "Nombre del estado, territorio o provincia",
        orig="ghcnd-states.txt NAME",
    ),
    col(
        "station_name",
        "STRING",
        "Nome da estação",
        "Name of the station",
        "Nombre de la estación",
        orig="NAME",
    ),
    col(
        "latitude",
        "FLOAT64",
        "Latitude da estação em graus decimais",
        "Latitude of the station in decimal degrees",
        "Latitud de la estación en grados decimales",
        orig="LATITUDE",
    ),
    col(
        "longitude",
        "FLOAT64",
        "Longitude da estação em graus decimais",
        "Longitude of the station in decimal degrees",
        "Longitud de la estación en grados decimales",
        orig="LONGITUDE",
    ),
    col(
        "elevation",
        "FLOAT64",
        "Altitude da estação",
        "Elevation of the station",
        "Altitud de la estación",
        unit="meter",
        obs="O valor sentinela -999.9 da fonte foi convertido para nulo, o que ocorre em 4.619 das 132.501 estações. | The source sentinel -999.9 was converted to null, which affects 4,619 of the 132,501 stations. | El valor centinela -999.9 de la fuente se convirtió a nulo, lo que afecta a 4.619 de las 132.501 estaciones.",
        orig="ELEVATION",
    ),
    col(
        "gsn_flag",
        "STRING",
        "Indica se a estação pertence à GCOS Surface Network",
        "Indicates whether the station belongs to the GCOS Surface Network",
        "Indica si la estación pertenece a la GCOS Surface Network",
        dict_="yes",
        obs="Nulo para estações fora da rede. | Null for stations outside the network. | Nulo para estaciones fuera de la red.",
        orig="GSN FLAG",
    ),
    col(
        "hcn_crn_flag",
        "STRING",
        "Indica se a estação pertence à U.S. Historical Climatology Network ou à U.S. Climate Reference Network",
        "Indicates whether the station belongs to the U.S. Historical Climatology Network or the U.S. Climate Reference Network",
        "Indica si la estación pertenece a la U.S. Historical Climatology Network o a la U.S. Climate Reference Network",
        dict_="yes",
        obs="Nulo para estações fora dessas redes. | Null for stations outside those networks. | Nulo para estaciones fuera de esas redes.",
        orig="HCN/CRN FLAG",
    ),
    col(
        "wmo_id",
        "STRING",
        "Número da estação na Organização Meteorológica Mundial",
        "World Meteorological Organization number of the station",
        "Número de la estación en la Organización Meteorológica Mundial",
        obs="Nulo quando a estação não tem número WMO atribuído. | Null when the station has no WMO number assigned. | Nulo cuando la estación no tiene número WMO asignado.",
        orig="WMO ID",
    ),
]

INVENTORY = [
    col(
        "station_id",
        "STRING",
        "Código de identificação da estação no GHCN-Daily",
        "GHCN-Daily station identification code",
        "Código de identificación de la estación en GHCN-Daily",
        orig="ID",
    ),
    col(
        "element",
        "STRING",
        "Código do elemento meteorológico medido",
        "Code of the meteorological element measured",
        "Código del elemento meteorológico medido",
        dict_="yes",
        obs="Esta tabela cobre os 144 elementos que o GHCN-Daily já registrou, enquanto a tabela observation cobre apenas os cinco elementos principais. | This table covers all 144 elements GHCN-Daily has ever recorded, whereas the observation table covers only the five core elements. | Esta tabla cubre los 144 elementos que GHCN-Daily ha registrado, mientras que la tabla observation cubre solo los cinco elementos principales.",
        orig="ELEMENT",
    ),
    col(
        "first_year",
        "INT64",
        "Primeiro ano com dados não sinalizados para o elemento na estação",
        "First year with unflagged data for the element at the station",
        "Primer año con datos no señalados para el elemento en la estación",
        unit="year",
        orig="FIRSTYEAR",
    ),
    col(
        "last_year",
        "INT64",
        "Último ano com dados não sinalizados para o elemento na estação",
        "Last year with unflagged data for the element at the station",
        "Último año con datos no señalados para el elemento en la estación",
        unit="year",
        orig="LASTYEAR",
    ),
]

_VALUE_OBS = (
    "A unidade varia por elemento e está registrada na coluna measurement_unit "
    "de cada linha: graus Celsius para TMAX e TMIN, milímetros para PRCP, SNOW "
    "e SNWD. Por isso esta coluna não tem uma unidade de medida única no nível "
    "da coluna. Os fatores de escala da fonte já foram aplicados: PRCP vem em "
    "décimos de milímetro e TMAX/TMIN em décimos de grau, mas SNOW e SNWD já "
    "vêm em milímetros inteiros. O sentinela -9999 da fonte foi removido. | "
    "The unit varies by element and is recorded per row in the measurement_unit "
    "column: degrees Celsius for TMAX and TMIN, millimetres for PRCP, SNOW and "
    "SNWD. This column therefore carries no single column-level measurement "
    "unit. The source scaling factors have already been applied: PRCP arrives "
    "in tenths of a millimetre and TMAX/TMIN in tenths of a degree, but SNOW "
    "and SNWD already arrive in whole millimetres. The source sentinel -9999 "
    "was removed. | "
    "La unidad varía según el elemento y se registra por fila en la columna "
    "measurement_unit: grados Celsius para TMAX y TMIN, milímetros para PRCP, "
    "SNOW y SNWD. Por eso esta columna no tiene una unidad de medida única a "
    "nivel de columna. Los factores de escala de la fuente ya fueron "
    "aplicados: PRCP viene en décimas de milímetro y TMAX/TMIN en décimas de "
    "grado, pero SNOW y SNWD ya vienen en milímetros enteros. El centinela "
    "-9999 de la fuente fue eliminado."
)

_QFLAG_OBS = (
    "Um valor não nulo significa que a observação FALHOU no teste de qualidade "
    "indicado e o valor não deve ser usado sem avaliação. Filtre por "
    "quality_flag IS NULL para manter apenas observações aprovadas. Em 2024, "
    "0,08% das linhas têm sinalizador não nulo, e todos os valores fisicamente "
    "impossíveis verificados (TMAX acima de 60 graus, profundidade de neve "
    "negativa) estavam sinalizados. | "
    "A non-null value means the observation FAILED the named quality assurance "
    "check and the value should not be used without assessment. Filter on "
    "quality_flag IS NULL to keep only observations that passed. In 2024, "
    "0.08% of rows carry a non-null flag, and every physically impossible "
    "value checked (TMAX above 60 degrees, negative snow depth) was flagged. | "
    "Un valor no nulo significa que la observación FALLÓ la prueba de calidad "
    "indicada y el valor no debe usarse sin evaluación. Filtre por "
    "quality_flag IS NULL para conservar solo observaciones aprobadas. En "
    "2024, el 0,08% de las filas tiene un indicador no nulo, y todos los "
    "valores físicamente imposibles verificados estaban señalados."
)

OBSERVATION = [
    col(
        "year",
        "INT64",
        "Ano da observação",
        "Year of the observation",
        "Año de la observación",
        unit="year",
        obs="Coluna de partição da tabela. | Partition column of the table. | Columna de partición de la tabla.",
    ),
    col(
        "station_id",
        "STRING",
        "Código de identificação da estação no GHCN-Daily",
        "GHCN-Daily station identification code",
        "Código de identificación de la estación en GHCN-Daily",
        obs="Referencia station.station_id. Coluna de clusterização da tabela. | References station.station_id. Clustering column of the table. | Referencia station.station_id. Columna de clusterización de la tabla.",
        orig="ID",
    ),
    col(
        "date",
        "DATE",
        "Data da observação",
        "Date of the observation",
        "Fecha de la observación",
        orig="YEAR/MONTH/DAY",
    ),
    col(
        "element",
        "STRING",
        "Código do elemento meteorológico observado",
        "Code of the meteorological element observed",
        "Código del elemento meteorológico observado",
        dict_="yes",
        obs="Restrito aos cinco elementos principais do GHCN-Daily: TMAX, TMIN, PRCP, SNOW e SNWD. | Restricted to the five core GHCN-Daily elements: TMAX, TMIN, PRCP, SNOW and SNWD. | Restringido a los cinco elementos principales de GHCN-Daily: TMAX, TMIN, PRCP, SNOW y SNWD.",
        orig="ELEMENT",
    ),
    col(
        "value",
        "FLOAT64",
        "Valor observado do elemento, convertido para a unidade padrão indicada em measurement_unit",
        "Observed value of the element, converted to the standard unit given in measurement_unit",
        "Valor observado del elemento, convertido a la unidad estándar indicada en measurement_unit",
        obs=_VALUE_OBS,
        orig="DATA VALUE",
    ),
    col(
        "measurement_unit",
        "STRING",
        "Unidade de medida do valor observado nesta linha",
        "Unit of measurement of the observed value in this row",
        "Unidad de medida del valor observado en esta fila",
        obs="Assume celsius para TMAX e TMIN, e millimeter para PRCP, SNOW e SNWD. | Takes celsius for TMAX and TMIN, and millimeter for PRCP, SNOW and SNWD. | Toma celsius para TMAX y TMIN, y millimeter para PRCP, SNOW y SNWD.",
    ),
    col(
        "measurement_flag",
        "STRING",
        "Sinalizador de medição que qualifica como o valor foi obtido",
        "Measurement flag qualifying how the value was obtained",
        "Indicador de medición que califica cómo se obtuvo el valor",
        dict_="yes",
        obs="Nulo quando nenhuma informação de medição se aplica. | Null when no measurement information is applicable. | Nulo cuando no aplica información de medición.",
        orig="M-FLAG",
    ),
    col(
        "quality_flag",
        "STRING",
        "Sinalizador de qualidade que indica qual teste de controle de qualidade a observação reprovou",
        "Quality flag indicating which quality assurance check the observation failed",
        "Indicador de calidad que señala qué prueba de control de calidad reprobó la observación",
        dict_="yes",
        obs=_QFLAG_OBS,
        orig="Q-FLAG",
    ),
    col(
        "source_flag",
        "STRING",
        "Sinalizador de fonte que identifica o conjunto de dados de origem do valor",
        "Source flag identifying the dataset the value originated from",
        "Indicador de fuente que identifica el conjunto de datos de origen del valor",
        dict_="yes",
        orig="S-FLAG",
    ),
    col(
        "observation_time",
        "STRING",
        "Horário da observação no formato HHMM, hora local",
        "Time of observation in HHMM format, local time",
        "Hora de la observación en formato HHMM, hora local",
        obs="Mantido como texto porque é um horário de relógio, não uma quantidade. Preenchido a partir da base HOMR do NCEI e frequentemente nulo. | Kept as text because it is a clock time, not a quantity. Populated from NCEI's HOMR station history database and frequently null. | Se mantiene como texto porque es una hora de reloj, no una cantidad. Se completa desde la base HOMR del NCEI y suele ser nulo.",
        orig="OBS-TIME",
    ),
]

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela",
        "Table name",
        "Nombre de la tabla",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
    ),
    col("chave", "STRING", "Chave", "Key", "Clave"),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal",
        "Temporal coverage",
        "Cobertura temporal",
    ),
    col("valor", "STRING", "Valor", "Value", "Valor"),
]

TABLES = {
    "station": STATION,
    "station_element_inventory": INVENTORY,
    "observation": OBSERVATION,
    "dicionario": DICIONARIO,
}


def main() -> None:
    out = Path(__file__).parent / "architecture"
    out.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        with open(
            out / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.DictWriter(fh, fieldnames=HEADER, lineterminator="\n")
            w.writeheader()
            w.writerows(cols)
        print(f"{table}: {len(cols)} columns")


if __name__ == "__main__":
    main()
