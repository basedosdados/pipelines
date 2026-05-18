"""
ETL de microdados de focos de queimadas (INPE) para o formato do staging no BigQuery.

Carga incremental (atualizacao 2025-2026):
    - Entrada: month_fire_data_new.csv (gerado por extraction.py)
    - Saida: CSVs particionados em output/fire_data/ano=YYYY/mes=M/

Carga historica completa (nao usada nesta atualizacao):
    - Requer year_fire_data.csv e month_fire_data.csv
    - Usar read_year_data(), merge_data() e o fluxo legado no __main__ comentado
"""

from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Configuracao da carga incremental
# ---------------------------------------------------------------------------

# Arquivo produzido pelo extraction.py (focos mensais 2025 e 2026).
INCREMENTAL_INPUT = "./input/month_fire_data_new.csv"

# Ultimo registro ja publicado em basedosdados.br_inpe_queimadas.microdados (passo 1).
# Registros com data_hora <= este valor sao descartados para evitar duplicidade.
LAST_LOADED_TIMESTAMP = "2025-02-12 23:10:00"

# Diretorio de saida particionado (compativel com upload para br_inpe_queimadas_staging).
OUTPUT_DIR = "./output/fire_data"

# Mapeamento opcional municipio INPE -> municipio_uf do diretorio BD.
MAP_CITIES_PATH = Path("./extra/auxiliary_files/map_cities.csv")


def read_year_data():
    """Le dados anuais legados (2003+). Usado apenas em cargas historicas completas."""
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


def read_month_data(input_path: str = "./input/month_fire_data.csv"):
    """
    Le CSV mensal do INPE e padroniza nomes de colunas para o pipeline.

    Args:
        input_path: Caminho do CSV. Na carga incremental, usar INCREMENTAL_INPUT.
    """
    duckdb.read_csv(input_path, header=True)
    duckdb.sql(f"""
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
        FROM '{input_path}'
    """)


def create_complete_data_from_month():
    """Define complete_data apenas a partir do mensal (carga incremental)."""
    duckdb.sql("""
        CREATE TABLE complete_data AS
        SELECT * FROM month_data
    """)


def merge_data():
    """Une dados anuais e mensais. Usado apenas em cargas historicas completas."""
    duckdb.sql("""
        CREATE TABLE complete_data AS
            SELECT * FROM year_data
            UNION BY NAME
            SELECT * FROM month_data
    """)


def get_date_fields():
    """
    Extrai componentes de data/hora e aplica filtro incremental.

    Mantem somente registros posteriores a LAST_LOADED_TIMESTAMP.
    """
    duckdb.sql(f"""
        CREATE VIEW date_data AS
            SELECT
                * EXCLUDE(data_hora),
                EXTRACT(YEAR FROM data_hora) AS ano,
                EXTRACT(MONTH FROM data_hora) AS mes,
                EXTRACT(DAY FROM data_hora) AS dia,
                EXTRACT(HOUR FROM data_hora) AS hora,
                EXTRACT(MINUTE FROM data_hora) AS minuto,
                EXTRACT(SECOND FROM data_hora) AS segundo
            FROM complete_data
            WHERE data_hora > TIMESTAMP '{LAST_LOADED_TIMESTAMP}'
    """)


def get_state_letters():
    """Converte nome do estado (texto) para sigla_uf."""
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


def map_city_names():
    """
    Gera municipio_uf no padrao do staging (Nome do Municipio - UF).

    Se map_cities.csv existir, aplica correcoes de nomes via cidade_uf_dir.
    Caso contrario, usa apenas a concatenacao municipio + sigla_uf.
    """
    if MAP_CITIES_PATH.exists():
        duckdb.sql(f"""
            CREATE VIEW cidades_dir AS
            SELECT cidade_uf, cidade_uf_dir
            FROM read_csv('{MAP_CITIES_PATH}', header = true)
        """)
        duckdb.sql("""
            CREATE VIEW city_data AS
                SELECT
                    s.* EXCLUDE (municipio),
                    COALESCE(
                        c.cidade_uf_dir,
                        s.municipio || ' - ' || s.sigla_uf
                    ) AS municipio_uf
                FROM state_data s
                LEFT JOIN cidades_dir c
                    ON s.municipio || ' - ' || s.sigla_uf = c.cidade_uf
        """)
    else:
        duckdb.sql("""
            CREATE VIEW city_data AS
                SELECT
                    s.* EXCLUDE (municipio),
                    s.municipio || ' - ' || s.sigla_uf AS municipio_uf
                FROM state_data s
        """)


def save_data():
    """Exporta city_data para CSVs particionados por ano e mes."""
    duckdb.sql(
        f"COPY city_data TO '{OUTPUT_DIR}' "
        "(FORMAT CSV, PARTITION_BY (ano, mes), OVERWRITE_OR_IGNORE);"
    )


def print_summary():
    """Exibe contagem e intervalo de datas da carga incremental processada."""
    result = duckdb.sql("""
        SELECT
            COUNT(*) AS total_linhas,
            MIN(
                MAKE_TIMESTAMP(
                    ano::BIGINT, mes::BIGINT, dia::BIGINT,
                    hora::BIGINT, minuto::BIGINT, segundo::DOUBLE
                )
            ) AS data_minima,
            MAX(
                MAKE_TIMESTAMP(
                    ano::BIGINT, mes::BIGINT, dia::BIGINT,
                    hora::BIGINT, minuto::BIGINT, segundo::DOUBLE
                )
            ) AS data_maxima
        FROM city_data
    """).fetchone()
    print(f"Linhas exportadas: {result[0]}")
    print(f"Data minima: {result[1]}")
    print(f"Data maxima: {result[2]}")


if __name__ == "__main__":
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Carga incremental (atualizacao 2025-2026) ---
    print(f"Lendo: {INCREMENTAL_INPUT}")
    read_month_data(INCREMENTAL_INPUT)
    create_complete_data_from_month()
    get_date_fields()
    get_state_letters()
    map_city_names()
    print_summary()
    print(f"Exportando particoes em: {OUTPUT_DIR}")
    save_data()
    print("Processamento concluido.")

    # --- Carga historica completa (desativada nesta task) ---
    # read_year_data()
    # read_month_data("./input/month_fire_data.csv")
    # merge_data()
    # get_date_fields()  # remover filtro WHERE para carga full
    # get_state_letters()
    # map_city_names()
    # save_data()
