"""
utils for botdosdados
"""
import hvac

from pipelines.utils.utils import get_vault_secret


def get_credentials_from_secret(
    secret_path: str,
    client: hvac.Client = None,
) -> dict:
    """
    Returns a username and password from a secret in Vault.
    """
    secret = get_vault_secret(secret_path, client)
    return secret["data"]
