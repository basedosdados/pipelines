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
    BASEDOSDADOS_PERGUNTAS_AGENT_LABEL = "basedosdados-perguntas"

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
    DEFAULT_CODE_OWNERS = ["equipe_infra"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        "equipe_infra": {
            "user_id": "865223885031997455",
            "type": "role",
        },
        "diego": {
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
        "ath67": {
            "user_id": "467788821527003136",
            "type": "user_nickname",
        },
        "guialvesp1": {
            "user_id": "307722006818979840",
            "type": "user_nickname",
        },
        "lauris": {
            "user_id": "725799350516842636",
            "type": "user_nickname",
        },
        "Mauricio Fagundes": {
            "user_id": "238816891337048064",
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
        "arthurfg": {
            "user_id": "1011467113682190427",
            "type": "user_nickname",
        },
        "trick": {
            "user_id": "252923443052281856",
            "type": "user_nickname",
        },
        "lucasmoreira": {
            "user_id": "765511326675763233",
            "type": "user_nickname",
        },
        "Gabs": {
            "user_id": "690361518973649006",
            "type": "user_nickname",
        },
        "gustavoalcantara": {
            "user_id": "951646117584592946",
            "type": "user_nickname",
        },
    }
