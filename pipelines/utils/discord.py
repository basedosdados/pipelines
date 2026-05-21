"""
Notificações via Discord.
"""

import requests

from pipelines.constants import constants
from pipelines.utils.utils import is_running_in_prod
from pipelines.utils.vault import get_vault_secret


def send_discord_message(message: str, webhook_url: str) -> None:
    requests.post(webhook_url, data={"content": message}, timeout=5)


def notify_discord_on_failure(
    flow,
    flow_run,
    state,
    secret_path: str,
    code_owners: list[str] | None = None,
) -> None:
    """
    Hook de on_failure para Prefect 3. Use com functools.partial:

        from functools import partial
        from pipelines.utils.discord import notify_discord_on_failure

        @flow(on_failure=[partial(notify_discord_on_failure, secret_path="caminho/secret")])
        def meu_flow(): ...
    """
    if not is_running_in_prod():
        return

    url = get_vault_secret(secret_path)["data"]["url"]
    code_owners = code_owners or constants.DEFAULT_CODE_OWNERS.value
    code_owner_dict = constants.OWNERS_DISCORD_MENTIONS.value

    mentions = _build_mentions(code_owners, code_owner_dict)
    message = (
        f":man_facepalming: Flow **{flow.name}** falhou."
        + f'\n  - Mensagem: *"{state.message}"*'
        + f"\n  - Flow run: https://prefect3.basedosdados.org/flow-runs/flow-run/{flow_run.id}"
        + "\n  - Atenção:\n"
        + mentions
    )
    send_discord_message(message=message, webhook_url=url)


def notify_discord(
    secret_path: str,
    message: str,
    code_owners: list[str] | None = None,
) -> None:
    """Envia mensagem manual ao Discord. Só executa em prod."""
    if not is_running_in_prod():
        return

    url = get_vault_secret(secret_path)["data"]["url"]
    code_owners = code_owners or constants.DEFAULT_CODE_OWNERS.value
    code_owner_dict = constants.OWNERS_DISCORD_MENTIONS.value

    send_discord_message(
        message=message + "\n" + _build_mentions(code_owners, code_owner_dict),
        webhook_url=url,
    )


def _build_mentions(code_owners: list[str], code_owner_dict: dict) -> str:
    lines = []
    for owner in code_owners:
        owner_id = code_owner_dict[owner]["user_id"]
        owner_type = code_owner_dict[owner]["type"]
        if owner_type == "user":
            lines.append(f"    - <@{owner_id}>\n")
        elif owner_type == "user_nickname":
            lines.append(f"    - <@!{owner_id}>\n")
        elif owner_type == "channel":
            lines.append(f"    - <#{owner_id}>\n")
        elif owner_type == "role":
            lines.append(f"    - <@&{owner_id}>\n")
    return "".join(lines)
