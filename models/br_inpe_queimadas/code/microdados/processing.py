# -*- coding: utf-8 -*-
import duckdb


def readYearData():
    duckdb.read_csv("./input/year_fire_data.csv", header=True)
    duckdb.sql("""
        CREATE TABLE year_data AS
        SELECT
            lat AS latitude,
            lon AS longitude,
            data_pas AS data_hora,
            municipio,
            estado,
            bioma
        FROM './input/year_fire_data.csv'
    """)


def readMonthData():
    duckdb.read_csv("./input/month_fire_data.csv", header=True)
    duckdb.sql("""
        CREATE TABLE month_data AS
        SELECT
            lat AS latitude,
            lon AS longitude,
            data_hora_gmt AS data_hora,
            satelite,
            municipio,
            estado,
            numero_dias_sem_chuva AS dias_sem_chuva,
            precipitacao,
            risco_fogo,
            bioma,
            frp AS potencia_radiativa_fogo
        FROM './input/month_fire_data.csv'
    """)


def mergeData():
    duckdb.sql("""
        CREATE TABLE complete_data AS
            SELECT * FROM year_data
            UNION BY NAME
            SELECT * FROM month_data
    """)


def getDateFields():
    duckdb.sql("""
        CREATE VIEW date_data AS
            SELECT
                * EXCLUDE(data_hora),
                EXTRACT(YEAR FROM data_hora) AS ano,
                EXTRACT(MONTH FROM data_hora) AS mes,
                EXTRACT(DAY FROM data_hora) AS dia,
                EXTRACT(HOUR FROM data_hora) AS hora,
                EXTRACT(MINUTE FROM data_hora) AS minuto,
                EXTRACT(SECOND FROM data_hora) AS segundo,
            FROM complete_data
    """)


def getStateLetters():
    duckdb.sql("""
        CREATE VIEW state_data AS
            SELECT
                * EXCLUDE(estado),
                CASE
                    WHEN LOWER(estado) = 'acre' THEN 'AC'
                    WHEN LOWER(estado) = 'alagoas' THEN 'AL'
                    WHEN LOWER(estado) = 'amapá' THEN 'AP'
                    WHEN LOWER(estado) = 'amazonas' THEN 'AM'
                    WHEN LOWER(estado) = 'bahia' THEN 'BA'
                    WHEN LOWER(estado) = 'ceará' THEN 'CE'
                    WHEN LOWER(estado) = 'distrito federal' THEN 'DF'
                    WHEN LOWER(estado) = 'espírito santo' THEN 'ES'
                    WHEN LOWER(estado) = 'goiás' THEN 'GO'
                    WHEN LOWER(estado) = 'maranhão' THEN 'MA'
                    WHEN LOWER(estado) = 'mato grosso' THEN 'MT'
                    WHEN LOWER(estado) = 'mato grosso do sul' THEN 'MS'
                    WHEN LOWER(estado) = 'minas gerais' THEN 'MG'
                    WHEN LOWER(estado) = 'pará' THEN 'PA'
                    WHEN LOWER(estado) = 'paraíba' THEN 'PB'
                    WHEN LOWER(estado) = 'paraná' THEN 'PR'
                    WHEN LOWER(estado) = 'pernambuco' THEN 'PE'
                    WHEN LOWER(estado) = 'piauí' THEN 'PI'
                    WHEN LOWER(estado) = 'rio de janeiro' THEN 'RJ'
                    WHEN LOWER(estado) = 'rio grande do norte' THEN 'RN'
                    WHEN LOWER(estado) = 'rio grande do sul' THEN 'RS'
                    WHEN LOWER(estado) = 'rondônia' THEN 'RO'
                    WHEN LOWER(estado) = 'roraima' THEN 'RR'
                    WHEN LOWER(estado) = 'santa catarina' THEN 'SC'
                    WHEN LOWER(estado) = 'são paulo' THEN 'SP'
                    WHEN LOWER(estado) = 'sergipe' THEN 'SE'
                    WHEN LOWER(estado) = 'tocantins' THEN 'TO'
                    ELSE NULL
                END AS sigla_uf
            FROM date_data
    """)


def mapCityNames():
    duckdb.sql("""
        CREATE VIEW cidades_dir AS
        SELECT cidade_uf, cidade_uf_dir
        FROM read_csv('./extra/auxiliary_files/map_cities.csv', header = true)""")

    duckdb.sql("""
        CREATE VIEW city_data AS
            SELECT s.* EXCLUDE (municipio),
            s.municipio as municipio
            FROM state_data s
            LEFT JOIN cidades_dir c
                ON s.municipio || ' - ' || s.sigla_uf = c.cidade_uf

    """)


def saveData():
    duckdb.sql(
        "COPY city_data TO './output/fire_data' (FORMAT CSV, PARTITION_BY (ano, mes), OVERWRITE_OR_IGNORE);"
    )


if __name__ == "__main__":
    readYearData()
    readMonthData()
    mergeData()
    getDateFields()
    getStateLetters()
    mapCityNames()
    # saveData()
    duckdb.sql("SELECT * FROM city_data").show()
