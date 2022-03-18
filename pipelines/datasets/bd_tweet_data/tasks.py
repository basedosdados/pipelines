"""
Tasks for bd_tweet_data
"""

from prefect import task

@task
def say_hello(name: str = 'World') -> str:
    """
    Greeting task.
    """
    return f'Hello, {name}!'
