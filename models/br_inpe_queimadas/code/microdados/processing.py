import os
from pathlib import Path

# pyrefly: ignore [missing-import]
import duckdb

# Último registro já carregado em basedosdados.br_inpe_queimadas.microdados.
# Registros com data_hora <= este valor são descartados para evitar duplicidade.
LAST_LOADED_TIMESTAMP = "2025-02-12 23:10:00"


# Lê o CSV mensal e padroniza os nomes das colunas para o schema do staging
def read_month_data(input_path: str) -> None:
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE month_data AS
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


# Alias para carga incremental: complete_data contém apenas os dados mensais
def create_complete_data_from_month() -> None:
    duckdb.sql("""
        CREATE OR REPLACE TABLE complete_data AS
        SELECT * FROM month_data
    """)


# Extrai componentes de data/hora e descarta registros já carregados
# last_loaded_timestamp define o corte: somente registros posteriores são mantidos
def get_date_fields(last_loaded_timestamp: str) -> None:
    duckdb.sql(f"""
        CREATE OR REPLACE VIEW date_data AS
            SELECT
                * EXCLUDE(data_hora),
                EXTRACT(YEAR FROM data_hora) AS ano,
                EXTRACT(MONTH FROM data_hora) AS mes,
                EXTRACT(DAY FROM data_hora) AS dia,
                EXTRACT(HOUR FROM data_hora) AS hora,
                EXTRACT(MINUTE FROM data_hora) AS minuto,
                EXTRACT(SECOND FROM data_hora) AS segundo
            FROM complete_data
            WHERE data_hora > TIMESTAMP '{last_loaded_timestamp}'
    """)


# Converte o nome completo do estado para sigla_uf (ex: 'são paulo' → 'SP')
def get_state_letters() -> None:
    duckdb.sql("""
        CREATE OR REPLACE VIEW state_data AS
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


# Padroniza o nome do município usando o mapeamento auxiliar (map_cities.csv),
# gerando a coluna municipio_uf no formato 'Municipio - UF'
def map_city_names(map_cities_path: str) -> None:
    if Path(map_cities_path).exists():
        duckdb.sql(f"""
            CREATE OR REPLACE VIEW cidades_dir AS
            SELECT cidade_uf, cidade_uf_dir
            FROM read_csv('{map_cities_path}', header = true)
        """)
        duckdb.sql("""
            CREATE OR REPLACE VIEW city_data AS
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
        # Sem o mapeamento auxiliar, usa a concatenação direta
        duckdb.sql("""
            CREATE OR REPLACE VIEW city_data AS
                SELECT
                    s.* EXCLUDE (municipio),
                    s.municipio || ' - ' || s.sigla_uf AS municipio_uf
                FROM state_data s
        """)


# Exporta os dados processados como CSVs particionados por ano e mes
def save_data(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    duckdb.sql(
        f"COPY city_data TO '{output_dir}' "
        "(FORMAT CSV, PARTITION_BY (ano, mes), OVERWRITE_OR_IGNORE);"
    )


# Executa todas as etapas de processamento em sequência
def run_processing(
    input_path: str, output_dir: str, map_cities_path: str
) -> None:
    read_month_data(input_path)
    create_complete_data_from_month()
    get_date_fields(LAST_LOADED_TIMESTAMP)
    get_state_letters()
    map_city_names(map_cities_path)
