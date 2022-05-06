# -*- coding: utf-8 -*-
"""
Constants for all flows
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants used in the EMD flows.
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
    # Discord
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"
    # Prefect
    GCS_FLOWS_BUCKET = "basedosdados-dev-prefect"
    DEFAULT_CODE_OWNERS = ["@equipe_infra", "@equipe_dados"]
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    PREFECT_DEFAULT_PROJECT = "main"
