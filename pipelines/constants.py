"""
Constants for all flows
"""

from enum import Enum


class constants(Enum):
    """
    Constants used in the BD flows.
    """

    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    # Prefect agents AUTO_FIND=M9w=k-b_
    DATA_TWEET_AGENT_LABEL = "bd8347ba-4969-42ec-aa52-045cef55c4af"
    BASEDOSDADOS_DEV_AGENT_LABEL = "basedosdados-dev"
    BASEDOSDADOS_PROD_AGENT_LABEL = "basedosdados"
    BASEDOSDADOS_PERGUNTAS_AGENT_LABEL = "basedosdados-perguntas"

    MODE_TO_PROJECT_DICT = {"prod": "basedosdados", "dev": "basedosdados-dev"}

    ######################################
    # Other constants
    ######################################
    # Prefect
    GCS_FLOWS_BUCKET = "basedosdados-dev-prefect"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    PREFECT_DEFAULT_PROJECT = "main"
    PREFECT_STAGING_PROJECT = "staging"

    FLOW_DUMP_TO_GCS_NAME = "BD template: Ingerir tabela zipada para GCS"
    FLOW_EXECUTE_DBT_MODEL_NAME = "BD template: Executa DBT model"

    # dbt constants
    RUN_DBT_MODEL_MAX_RETRIES = 1
    WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS = 3
    WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL = 5
    DISABLE_ELEMENTARY_VARS = {
        "disable_dbt_artifacts_autoupload": True,
        "disable_run_results": True,
        "disable_tests_results": True,
        "disable_dbt_invocation_autoupload": True,
    }
    ENABLE_ELEMENTARY_VARS = {
        "disable_dbt_artifacts_autoupload": False,
        "disable_run_results": True,
        "disable_tests_results": False,
        "disable_dbt_invocation_autoupload": False,
    }

    GOOGLE_SHEETS_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

    API_URL = {
        "staging": "https://staging.backend.basedosdados.org/api/v1/graphql",
        "prod": "https://backend.basedosdados.org/api/v1/graphql",
    }
    # Code Owners #

    ######################################
    # Discord code owners constants
    ######################################
    BD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    DEFAULT_CODE_OWNERS = ["equipe_dados"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        # create equipe_dados channel
        "equipe_dados": {
            "user_id": "865034571469160458",
            "type": "role",
        },
        "lauris": {
            "user_id": "725799350516842636",
            "type": "user_nickname",
        },
        "rdahis": {
            "user_id": "290670932756922381",
            "type": "user_nickname",
        },
        "Gabriel Pisa": {
            "user_id": "313757164747358209",
            "type": "user_nickname",
        },
        "trick": {
            "user_id": "252923443052281856",
            "type": "user_nickname",
        },
        "aspeddro": {"user_id": "767136614140346389", "type": "user_nickname"},
        "luiz": {"user_id": "322508262303989760", "type": "user_nickname"},
        "Luiza": {"user_id": "818259139221913611", "type": "user_nickname"},
    }
