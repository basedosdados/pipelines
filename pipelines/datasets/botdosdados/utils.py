# -*- coding: utf-8 -*-
"""
utils for botdosdados
"""
import hvac
import tweepy

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


def send_tweet(
    access_token: str,
    access_token_secret: str,
    consumer_key: str,
    consumer_secret: str,
    bearer_token: str,
    text: str,
    reply_to: str,
    is_reply: bool,
):
    """
    Sends one tweet for each new table added recently. Uses 10 seconds interval for each new tweet
    """

    client = tweepy.Client(
        bearer_token=bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )

    if is_reply:
        client.create_tweet(text=text)
    else:
        client.create_tweet(text=text, in_reply_to_tweet_id=reply_to)
