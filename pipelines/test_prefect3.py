from prefect import flow, task


@task
def say_hello(name: str) -> str:
    message = f"Hello from Prefect 3, {name}!"
    print(message)
    return message


@flow(log_prints=True)
def hello_prefect3(name: str = "basedosdados") -> str:
    return say_hello(name)
