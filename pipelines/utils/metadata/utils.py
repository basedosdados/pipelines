"""
Funções utilitárias de propósito geral da camada de metadata.

Reúne os helpers consumidos pelo restante da camada: as leituras de BigQuery
usadas pelo `BigQueryReader` (`extract_last_date_from_bq`,
`update_date_from_bq_metadata`, `update_row_access_policy`, ...) e as funções de
leitura de cobertura na API consumidas pelos crawlers e tasks
(`get_api_most_recent_date`, `get_url`).
"""

import datetime
from time import sleep

import basedosdados as bd
from basedosdados.download.download import _google_client

from pipelines.constants import constants
from pipelines.utils.discord import notify_discord
from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.utils import log

################################
#
# update_django_metadata Utils
#
################################


def check_if_values_are_accepted(
    coverage_type: str, time_delta: dict, date_column_name: dict
):
    if time_delta:
        if len(time_delta) != 1:
            raise ValueError(
                "Dicionário de delta tempo inválido. O dicionário deve conter apenas uma chave e um valor"
            )
        key = list(time_delta)[0]  # noqa: RUF015
        if key not in metadata_constants.ACCEPTED_TIME_UNITS.value:
            raise ValueError(
                f"Unidade temporal inválida. Escolha entre {metadata_constants.ACCEPTED_TIME_UNITS.value}"
            )
        if not isinstance(time_delta[key], int):
            raise ValueError(
                "Valor de delta inválido. O valor deve ser um inteiro"
            )

    if coverage_type not in metadata_constants.ACCEPTED_COVERAGE_TYPE.value:
        raise ValueError(
            f"Tipo de cobertura temporal inválida. Escolha entre {metadata_constants.ACCEPTED_COVERAGE_TYPE.value}"
        )

    if (
        set(list(date_column_name))
        not in metadata_constants.ACCEPTED_COLUMN_KEY_VALUES.value
    ):
        raise ValueError(
            f"Dicionário das colunas de data inválido. As chaves só podem assumir os valores: {metadata_constants.ACCEPTED_COLUMN_KEY_VALUES.value} "
        )


def get_billing_project_id(mode: str) -> bool:
    return metadata_constants.MODE_PROJECT.value[mode]


def get_url(api_mode: str) -> str:
    return constants.API_URL.value[api_mode]


def extract_last_date_from_bq(
    dataset_id: str,
    table_id: str,
    date_format: str,
    date_column: dict,
    billing_project_id: str,
    project_id: str = "basedosdados",
    historical_database: bool = True,
) -> str:
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): ID do conjunto de dados.
        table_id (str): ID da tabela.
        date_format (str): Formato de atualização do metadado no Django ("%Y", "%Y-%m" ou "%Y-%m-%d").
        date_column (dict): Nomes das colunas usadas para consulta no BigQuery com datas.
            As chaves do dicionário determinam como a data máxima será consultada na tabela:
            - {'date'}: MAX('date')
            - {'year'}: MAX(DATE(year, 1, 1))
            - {'year', 'quarter'}: MAX(DATE(year, month*3, 1))
            - {'year', 'month'}: MAX(DATE(year, month, 1))
        billing_project_id (str): Projeto BigQuery utilizado para faturamento.
        project_id (str): Projeto padrão usado para obter a data da última atualização (padrão é "basedosdados").
        historical_database (bool): Defina como False se a base não tiver uma coluna representando a data de cobertura.
            Nesses casos, a consulta usará a última data de atualização da tabela no BigQuery.

    Returns:
        str: The last update date in the format "%Y",'%Y-%m' or '%Y-%m-%d'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    if not historical_database:
        log(
            "WARNING: Non-historical data mode selected. Single-date temporal coverage applied"
        )
        last_date_dt = update_date_from_bq_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            billing_project_id=billing_project_id,
            project_id=project_id,
        )
        last_date = datetime.datetime.strftime(last_date_dt, date_format)

        return last_date

    query_date_column = format_date_column(date_column)

    try:
        query_bd = f"""
        SELECT
        MAX({query_date_column}) as max_date
        FROM
        `{project_id}.{dataset_id}.{table_id}`
        """
        log(query_bd)
        t = bd.read_sql(
            query=query_bd,
            billing_project_id=billing_project_id,
            from_file=True,
        )

        query_result_date = t["max_date"][0]

        last_date = query_result_date.strftime(date_format)
        log(f"Última data: {last_date}")

        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last date: {e!s}")
        raise


def format_date_column(date_column: dict) -> str:
    if date_column.keys() == {"date"}:
        query_date_column = date_column["date"]
    if date_column.keys() == {"year"}:
        query_date_column = f"DATE({date_column['year']},1,1)"
    if date_column.keys() == {"year", "quarter"}:
        query_date_column = (
            f"DATE({date_column['year']},{date_column['quarter']}*3,1)"
        )
    if date_column.keys() == {"year", "month"}:
        query_date_column = (
            f"DATE({date_column['year']},{date_column['month']},1)"
        )
    return query_date_column


def able_to_query_bigquery_metadata(
    billing_project_id: str, bq_project: str
) -> bool:
    """
    To check the table metadata in BigQuery it is necessary that the billing project id has some special permissions,
    So, in our case, it is necessary that billing_project_id == bq_project
    """
    return billing_project_id == bq_project


def update_date_from_bq_metadata(
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    project_id: str,
) -> str:
    try:
        query_bd = f"""
        SELECT
            last_modified_time
        FROM
        `{project_id}.{dataset_id}.__TABLES__`
        WHERE table_id = '{table_id}'
        """
        log(query_bd)
        t = bd.read_sql(
            query=query_bd,
            billing_project_id=billing_project_id,
            from_file=True,
        )
        timestamp = (
            t["last_modified_time"][0] / 1000
        )  # Convert to seconds by dividing by 1000
        last_date = datetime.datetime.fromtimestamp(timestamp)
        log(f"Última data: {last_date}")
        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last update date: {e!s}")
        raise


def update_row_access_policy(
    project_id: str,
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    date_column_name: dict,
    date_format: str,
    free_parameters: dict,
) -> None:
    client = _google_client(billing_project_id, from_file=True, reauth=False)

    query_bdpro_access = f'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter  ON  `{project_id}.{dataset_id}.{table_id}` GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (TRUE)'
    job = client["bigquery"].query(query_bdpro_access)
    while not job.done():
        sleep(1)
    log("BDpro filter was included")

    query_date_column = format_date_column(date_column_name)
    all_free_last_date = format_date_parameters(free_parameters, date_format)

    query_allusers_access = f'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter  ON  `{project_id}.{dataset_id}.{table_id}` GRANT TO ("allUsers") FILTER USING ({query_date_column}<="{all_free_last_date}")'
    job = client["bigquery"].query(query_allusers_access)
    while not job.done():
        sleep(1)
    log("All users filter was included")


def format_date_parameters(free_parameters: dict, date_format: str) -> str:
    if date_format == "%Y-%m-%d":
        formated_date = f"{free_parameters['endYear']}-{free_parameters['endMonth']}-{free_parameters['endDay']}"
    elif date_format == "%Y-%m":
        formated_date = (
            f"{free_parameters['endYear']}-{free_parameters['endMonth']}-01"
        )
    elif date_format == "%Y":
        formated_date = f"{free_parameters['endYear']}-01-01"

    return formated_date


#######################
# get_api_most_recent_date Utils
#######################
def get_coverage_value(
    dataset_name: str, table_name: str, date_format: str, backend: bd.Backend
) -> dict:
    try:
        # get table ID in the PROD API
        table_id = backend._get_table_id_from_name(
            gcp_dataset_id=dataset_name, gcp_table_id=table_name
        )

        # get coverage values in the PROD API for the table ID
        datetime_result = get_datetimerange(table_id, backend)

        date_objects = parse_datetime_ranges(datetime_result, date_format)
        return date_objects

    except Exception as e:
        log(
            f"Error occurred while retrieving Table IDs values or Coverage values from the PROD API: {e!s}"
        )
        raise


def get_datetimerange(table_id: str, backend=bd.Backend) -> dict:
    """This function retrieves the datetimeRanges from the API for the specified Table ID in the Query Parameters
    Returns:
        dict: A dictionary containing datetimeRanges from the API
    """

    query = """
    query($table_Id: ID) {
      allCoverage(table_Id: $table_Id) {
        edges {
          node {
            id
            datetimeRanges {
              edges {
                node {
                  id
                  startYear
                  startMonth
                  startDay
                  endYear
                  endMonth
                  endDay
                }
              }
            }
            table {
              _id
              name
              isClosed
              dataset {
                name
              }
            }
          }
        }
      }
    }
    """
    variables = {"table_Id": table_id}
    response = backend._execute_query(query, variables)

    return response


def parse_datetime_ranges(datetime_result: dict, date_format: str) -> dict:
    """This function parses datetime ranges from the PROD API and returns a dictionary with the coverage values. Used within get_datetimerange function.

    Args:
        datetime_result (dict): A dictionary with datetime ranges from the PROD API (given by the output of get_datetimerange function)
        date_format (str): A format string to correctly parse the datetime ranges

    Returns:
        dict: A dictionary with one or more datetime ranges for the given table
    """

    date_objects = {}

    edges = datetime_result["allCoverage"]["items"]

    # iterates over each edge
    for edge in edges:
        datetime_ranges = edge["datetimeRanges"]["items"]

        # iterates over each edge of datetime_ranges

        # ps: If the table has bd_pro coverage,
        # it will have more than one datetime_range
        for dt_range in datetime_ranges:
            end_year = dt_range.get("endYear")
            end_month = dt_range.get("endMonth")
            end_day = dt_range.get("endDay")

            date_values = (end_year, end_month, end_day)

            date_string = format_and_check_date(date_values, date_format)
            # log(f"The following coverage is being added {date_objects}")
            date_objects[dt_range["id"]] = date_string

    return date_objects


def format_and_check_date(date_values: tuple, date_format: str) -> str:
    """This function formats a date according to the given date_format and make 2 checks:
        1) If the date_format is valid for the given date_values; in other words, if the date_format is correct for the table.
        2) If the date_values are not NONE;

    Args:
        date_values (tuple): A tuple with date values
        date_format (str): The format string

    Raises:
        ValueError: Checks 1 and 2 for %Y-%m-%d date format
        ValueError: Checks 1 and 2 for %Y-%m date format
        ValueError: Checks 1 and 2 for %Y date format

    Returns:
        str: A string with a date in the format %Y-%m-%d, %Y-%m, %Y
    """

    end_year, end_month, end_day = date_values

    if (
        date_format == "%Y-%m-%d"
        and end_year is not None
        and end_month is not None
        and end_day is not None
    ):
        return f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

    if (
        date_format == "%Y-%m"
        and end_year is not None
        and end_month is not None
    ):
        return f"{end_year:04d}-{end_month:02d}"

    if date_format == "Y%" and end_year is not None:
        return f"{end_year:04d}"

    raise ValueError(
        f"Attention! The input date_format ->> {date_format} is wrong for the current Table. One of the elements have NONE values in the PROD API. If that's not the case, check the Coverage values in the Prod API, they may be not filled"
    )


def get_api_most_recent_date(
    dataset_id: str,
    table_id: str,
    backend: bd.Backend,
    date_format: str = "%Y-%m-%d",
) -> datetime.date:
    """get the max table coverage for a given table id.

    This task will:

    1. Collect the credentials_utils;
    2. Access the API and collect the DJANGO Table ID with the given GCS table ID;
    3. Collet all the end coverages for the given table_id;
    4. Parse the coverages;
    5. Compare the coverages and return the most recent date;

    Args:
        dataset_id (str): Dataset ID in GCS
        table_id (str): Table ID in GCS
        backend (bd.Backend): defines backend connection is going to be made
        date_format (str, optional): Date format values %Y-%m-%d; %Y-%m; Y%

    Raises:
        ValueError: if date_format is not valid

    Returns:
        str: a string representation of the date: %Y-%m-%d; %Y-%m; Y%
    """

    accepted_date_format = ["%Y-%m-%d", "%Y-%m", "%Y"]
    if date_format not in accepted_date_format:
        raise ValueError(
            f"The date_format  ->>  ->> {date_format} is not supported. Please choose among {accepted_date_format}"
        )

    # collect all table coverages for the given table id
    coverages = get_coverage_value(
        dataset_id, table_id, date_format, backend=backend
    )

    # Convert the date strings to date objects
    date_objects = {}
    for key, date_string in coverages.items():
        date_objects[key] = datetime.datetime.strptime(
            date_string, date_format
        )

    max_date_key = max(date_objects, key=date_objects.get)
    max_date_value = date_objects[max_date_key].date()

    return max_date_value
