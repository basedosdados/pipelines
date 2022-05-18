# -*- coding: utf-8 -*-
"""
Constants for all flows
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
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

    ######################################
    # Other constants
    ######################################
    # Prefect
    GCS_FLOWS_BUCKET = "basedosdados-dev-prefect"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    PREFECT_DEFAULT_PROJECT = "main"
    # Code Owners #

    ######################################
    # Discord code owners constants
    ######################################
    BD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    DEFAULT_CODE_OWNERS = ["equipe_infra", "equipe_dados"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        "equipe_infra": {
            "user_id": "865223885031997455",
            "type": "role",
        },
        "equipe_dados": {
            "user_id": "865034571469160458",
            "type": "role",
        },
        "pimbel": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "lucas_cr": {
            "user_id": "776914459545436200",
            "type": "user_nickname",
        },
        "crislanealves": {
            "user_id": "740986161652301886",
            "type": "user_nickname",
        },
    }
