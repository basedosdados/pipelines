"""
Integração com Hashicorp Vault.
"""

from os import getenv

import hvac


def get_vault_client() -> hvac.Client:
    return hvac.Client(
        url=getenv("VAULT_ADDRESS").strip(),
        token=getenv("VAULT_TOKEN").strip(),
    )


def get_vault_secret(
    secret_path: str, client: hvac.Client | None = None
) -> dict:
    vault_client = client if client else get_vault_client()
    return vault_client.secrets.kv.read_secret_version(secret_path)["data"]


def get_credentials_from_secret(
    secret_path: str, client: hvac.Client | None = None
) -> dict:
    return get_vault_secret(secret_path, client)["data"]
