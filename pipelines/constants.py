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
    BR_CVM_ADMINISTRADORES_CARTEIRA_AGENT_LABEL = "86596753-2a29-44bb-8d04-232b45b81bfa"
    BR_CVM_OFERTA_PUBLICA_DISTRIBUICAO_AGENT_LABEL = "985838a5-3849-4dbc-984b-77d1966a7bc6"
    BASEDOSDADOS_AGENT_LABEL = "bd"

    ######################################
    # Other constants
    ######################################
    # Prefect
    GCS_FLOWS_BUCKET = "basedosdados-dev-prefect"
