# -*- coding: utf-8 -*-
"""
General purpose functions for the metadata project
"""
# pylint: disable=too-many-arguments
import requests
import basedosdados as bd

from datetime import datetime
from time import sleep
from typing import Dict, Tuple
from basedosdados.download.base import google_client
from dateutil.relativedelta import relativedelta

from pipelines.utils.constants import constants
from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.utils import get_credentials_from_secret, log


###############################
# update_django_metadata Utils
###############################


def check_if_values_are_accepted(
    coverage_type: str, time_delta: Dict, date_column_name: Dict
):
    if time_delta:
        if len(time_delta) != 1:
            raise ValueError(
                "Dicionário de delta tempo inválido. O dicionário deve conter apenas uma chave e um valor"
            )
        key = list(time_delta)[0]
        if key not in metadata_constants.ACCEPTED_TIME_UNITS.value:
            raise ValueError(
                f"Unidade temporal inválida. Escolha entre {metadata_constants.ACCEPTED_TIME_UNITS.value}"
            )
        if type(time_delta[key]) is not int:
            raise ValueError("Valor de delta inválido. O valor deve ser um inteiro")

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

def get_url(api_mode:str) -> str:
    return constants.API_URL.value[api_mode]

def get_coverage_ids(
    table_id: str,
    coverage_type: str,
    backend: bd.Backend,
) -> dict:
    """
    Obtains the coverage IDs based on the table_id.
    Checks if the coverage type match the available api coverages
    """
    coverage_id_pro = get_coverage_id(
        table_id=table_id,
        is_closed=True,
        backend=backend,
    )

    coverage_id_free = get_coverage_id(
        table_id=table_id,
        is_closed=False,
        backend=backend,
    )

    if coverage_type == "part_bdpro" and coverage_id_free and coverage_id_pro:
        return {
            "coverage_id_free": coverage_id_free,
            "coverage_id_pro": coverage_id_pro,
        }

    if coverage_type == "all_bdpro" and not coverage_id_free and coverage_id_pro:
        return {"coverage_id_pro": coverage_id_pro}

    if coverage_type == "all_free" and coverage_id_free and not coverage_id_pro:
        return {"coverage_id_free": coverage_id_free}

    raise ValueError(f"\n\nThe selected coverage type '{coverage_type}' does not match the available coverages. Coverages found:\n"
                f"  - Pro: {coverage_id_pro}\n"
                f"  - Free: {coverage_id_free}\n"
                f"Please ensure you have selected the correct coverage type or correct it in the api.\n\n")

def get_coverage_id(
    table_id: str, is_closed: bool, backend: bd.Backend) -> str:

    _, coverage_id = get_id(
        query_class="allCoverage",
        query_parameters={
            "$table_Id: ID": table_id,
            "$isClosed: Boolean": is_closed,
        },
        backend = backend
    )

    return coverage_id

def get_id(
    query_class: str,
    query_parameters: dict,
    backend: bd.Backend,
) -> Tuple:
    """
    Returns the ID based on the query parameters
    Raise an Error if the query parameters yield multiple matching items
    """

    _filter = ", ".join(list(query_parameters.keys()))

    keys = [
        parameter.replace("$", "").split(":")[0]
        for parameter in list(query_parameters.keys())
    ]

    values = list(query_parameters.values())

    _input = ", ".join([f"{key}:${key}" for key in keys])

    query = f"""query({_filter}) {{
                        {query_class}({_input}){{
                        edges{{
                            node{{
                            _id,
                            }}
                        }}
                        }}
                    }}"""

    variables = dict(zip(keys, values))

    response = backend._execute_query(query, variables=variables)
    nodes = response[query_class]['edges']

    if len(nodes)>1:
        raise ValueError(f'More than 1 node was found in this query. Plese give query parameters that retrieve only one object. \nQuery:\n\t{query}\nVariables:{variables} \nNodes found:{nodes}')

    if len(nodes) == 0:
        return response, None

    id = nodes[0]['node']['_id']

    return response, id

def get_table_status(table_id:str, backend: bd.Backend) -> str:
    query = """query($table_id: ID) {
        allTable(id: $table_id) {
            edges {
                node {
                    status{
                        slug
                    }
                }
            }
        }
    } """

    response = backend._execute_query(query,{"table_id": table_id})

    nodes = response['allTable']['edges']

    if nodes == []:
        return None

    return nodes[0]['node']['status']['slug']

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
        last_date = datetime.strftime(last_date_dt, date_format)

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
        log(f"An error occurred while extracting the last date: {str(e)}")
        raise

def format_date_column(date_column: dict) -> str:
    if date_column.keys() == {"date"}:
        query_date_column = date_column["date"]
    if date_column.keys() == {"year", "quarter"}:
        query_date_column = f"DATE({date_column['year']},{date_column['quarter']}*3,1)"
    if date_column.keys() == {"year", "month"}:
        query_date_column = f"DATE({date_column['year']},{date_column['month']},1)"
    return query_date_column

def able_to_query_bigquery_metadata(billing_project_id:str, bq_project:str) -> bool:
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
        last_date = datetime.fromtimestamp(timestamp)
        log(f"Última data: {last_date}")
        return last_date
    except Exception as e:
        log(f"An error occurred while extracting the last update date: {str(e)}")
        raise

def get_coverage_parameters(
    coverage_type: str, last_date:str, time_delta: dict, table_id:str, date_format:str, historical_database: bool, backend: bd.Backend
) -> dict:

    coverage_ids = get_coverage_ids(
        table_id=table_id,
        coverage_type=coverage_type,
        backend = backend
    )

    if coverage_type == "all_free":
        free_parameters = get_date_parameters(position="end", date_str=last_date)
        free_parameters["coverage"] = coverage_ids.get("coverage_id_free")
        return free_parameters, None

    if coverage_type == "all_bdpro":
        bdpro_parameters = get_date_parameters(position="end", date_str=last_date)
        bdpro_parameters["coverage"] = coverage_ids.get("coverage_id_pro")
        return None, bdpro_parameters

    if coverage_type == "part_bdpro":
        if not historical_database:
            raise ValueError(
                "Invalid Selection: Non-historical base and partially bdpro coverage chosen, not compatible."
            )

        bdpro_parameters = get_date_parameters(position="end", date_str=last_date)
        bdpro_parameters["coverage"] = coverage_ids.get("coverage_id_pro")

        delta = relativedelta(**time_delta)
        free_access_max_date = datetime.strptime(last_date, date_format) - delta
        free_access_max_date = free_access_max_date.strftime(date_format)
        free_parameters = get_date_parameters(position="end", date_str=free_access_max_date)
        free_parameters["coverage"] = coverage_ids.get("coverage_id_free")

        bdpro_parameters = sync_bdpro_and_free_coverage(
            date_format=date_format,
            bdpro_parameters=bdpro_parameters,
            free_parameters=free_parameters,
        )

        log(f"Cobertura Grátis ->> {free_access_max_date} || Cobertura PRO ->> {last_date}")

        return free_parameters, bdpro_parameters

def get_date_parameters(position: str, date_str: str):
    date_len = len(date_str.split("-") if date_str != "" else 0)
    date_parameters = {}
    if date_len == 3:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        date_parameters[f"{position}Year"] = date.year
        date_parameters[f"{position}Month"] = date.month
        date_parameters[f"{position}Day"] = date.day
    elif date_len == 2:
        date = datetime.strptime(date_str, "%Y-%m")
        date_parameters[f"{position}Year"] = date.year
        date_parameters[f"{position}Month"] = date.month
    elif date_len == 1:
        date = datetime.strptime(date_str, "%Y")
        date_parameters[f"{position}Year"] = date.year
    return date_parameters

def sync_bdpro_and_free_coverage(
    date_format: str, bdpro_parameters: dict, free_parameters: dict
) -> dict:
    if date_format == "%Y-%m-%d":
        bdpro_parameters["startYear"] = free_parameters["endYear"]
        bdpro_parameters["startMonth"] = free_parameters["endMonth"]
        bdpro_parameters["startDay"] = free_parameters["endDay"]
    elif date_format == "%Y-%m":
        bdpro_parameters["startYear"] = free_parameters["endYear"]
        bdpro_parameters["startMonth"] = free_parameters["endMonth"]
    elif date_format == "%Y":
        bdpro_parameters["startYear"] = free_parameters["endYear"]

    return bdpro_parameters

def create_update(
    mutation_class: str,
    mutation_parameters: dict,
    query_class: str,
    query_parameters: dict,
    backend: bd.Backend,
    update: bool = True,
):
    """
    Creates or updates metadata within the backend API.

    The `mutation_class` and `mutation_parameters` define the metadata to be created or updated,
    while `query_class` and `query_parameters` specify the element to be located for updating.
    """
    r, id = get_id(
        query_class,
        query_parameters,
        backend
    )
    if id and not update:
        r["r"] = "query"
        return r, id

    _classe = mutation_class.replace("CreateUpdate", "").lower()
    query = f"""
                mutation($input:{mutation_class}Input!){{
                    {mutation_class}(input: $input){{
                    errors {{
                        field,
                        messages
                    }},
                    clientMutationId,
                    {_classe} {{
                        id,
                    }}
                }}
                }}
            """

    if update is True and not isinstance(id, type(None)):
        print(f'update {query_parameters}')
        mutation_parameters["id"] = id

    response = backend._execute_query(query,variables = {"input": mutation_parameters}, headers=get_headers(backend))
    response["r"] = "mutation"
    id = response[mutation_class][_classe]["id"]
    id = id.split(":")[1]
    return response, id

def update_row_access_policy(
    project_id: str,
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    date_column_name: dict,
    date_format: str,
    free_parameters: dict,
) -> None:
    client = google_client(billing_project_id, from_file=True, reauth=False)

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

def format_date_parameters(free_parameters: dict, date_format: str)->str:
    if date_format == "%Y-%m-%d":
        formated_date = f'{free_parameters["endYear"]}-{free_parameters["endMonth"]}-{free_parameters["endDay"]}'
    elif date_format == "%Y-%m":
        formated_date = f'{free_parameters["endYear"]}-{free_parameters["endMonth"]}-01'
    elif date_format == "%Y":
        formated_date = f'{free_parameters["endYear"]}-01-01'

    return formated_date



#######################
# check_if_data_is_outdated Utils
#######################
def get_coverage_value(
    dataset_name: str,
    table_name: str,
    date_format: str,
    backend: bd.Backend
) -> dict:
    try:
        # get table ID in the PROD API
        table_id = backend._get_table_id_from_name(gcp_dataset_id=dataset_name, gcp_table_id=table_name)

        # get coverage values in the PROD API for the table ID
        datetime_result = get_datetimerange(
            table_id,
            backend
        )

        date_objects = parse_datetime_ranges(datetime_result, date_format)
        return date_objects

    except Exception as e:
        log(
            f"Error occurred while retrieving Table IDs values or Coverage values from the PROD API: {str(e)}"
        )
        raise

def get_datetimerange(
    table_id: str,
    backend = bd.Backend
) -> dict:
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

    # log(f"the chosen format to parse the coverage was {date_format}")

    edges = datetime_result["data"]["allCoverage"]["edges"]

    # iterates over each edge
    for edge in edges:
        node = edge["node"]
        datetime_ranges = node["datetimeRanges"]["edges"]

        # iterates over each edge of datetime_ranges

        # ps: If the table has bd_pro coverage,
        # it will have more than one datetime_range
        for dt_range in datetime_ranges:
            dt_node = dt_range["node"]
            end_year = dt_node.get("endYear")
            end_month = dt_node.get("endMonth")
            end_day = dt_node.get("endDay")

            date_values = (end_year, end_month, end_day)

            date_string = format_and_check_date(date_values, date_format)
            # log(f"The following coverage is being added {date_objects}")
            date_objects[dt_node["id"]] = date_string

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

    if date_format == "%Y-%m-%d" and  end_year is not None and end_month is not None and end_day is not None:
        return f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

    if date_format == "%Y-%m" and end_year is not None and end_month is not None:
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
        dataset_id,
        table_id,
        date_format,
        backend=backend
    )

    # Convert the date strings to date objects
    date_objects = {}
    for key, date_string in coverages.items():
        date_objects[key] = datetime.strptime(date_string, date_format)

    max_date_key = max(date_objects, key=date_objects.get)
    max_date_value = date_objects[max_date_key].date()

    return max_date_value

def get_headers(backend: bd.Backend) -> dict:
    """
    Get headers to be able to do mutations in backend api
    """

    api_mode = 'prod'
    if 'staging' in backend.graphql_url:
        api_mode = 'staging'

    credentials = get_credentials_from_secret(secret_path=f"api_user_{api_mode}")

    mutation = """
        mutation ($email: String!, $password: String!) {
            tokenAuth(email: $email, password: $password) {
                token
            }
        }
    """
    variables = {"email": credentials["email"], "password": credentials["password"]}

    response = backend._execute_query(query=mutation, variables=variables)
    token = response["tokenAuth"]["token"]

    header_for_mutation_query = {"Authorization": f"Bearer {token}"}

    return header_for_mutation_query

def get_credentials_utils(secret_path: str) -> Tuple[str, str]:
    """
    Returns the user and password for the given secret path.
    """
    # log(f"Getting user and password for secret path: {secret_path}")
    tokens_dict = get_credentials_from_secret(secret_path)
    email = tokens_dict.get("email")
    password = tokens_dict.get("password")
    return email, password

def get_token(email:str, password:str, api_mode: str = "prod") -> str:
    """
    Get api token.
    """
    r = None

    url = constants.API_URL.value[api_mode]

    r = requests.post(
        url=url,
        headers={"Content-Type": "application/json"},
        json={
            "query": """
        mutation ($email: String!, $password: String!) {
            tokenAuth(email: $email, password: $password) {
                token
            }
        }
    """,
            "variables": {"email": email, "password": password},
        },
    )
    r.raise_for_status()

    return r.json()["data"]["tokenAuth"]["token"]
