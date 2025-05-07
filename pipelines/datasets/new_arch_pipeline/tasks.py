# -*- coding: utf-8 -*-
import os

import pandas as pd
import prefect
from prefect import Client, context


@prefect.task
def criar_dataframe():
    df = pd.DataFrame(
        {
            "ano": [2021, 2022, 2023],
            "equipe_dados": [
                "Gabriel Pisa",
                "Patrick Teixeira",
                "Luiz Jordão",
            ],
            "github": ["folhesgabriel", "tricktx", "winzen"],
            "idade": [28, 28, 27],
            "sexo": ["Masculino", "Masculino", "Masculino"],
        }
    )

    os.makedirs("/tmp/output/", exist_ok=True)
    output = "/tmp/output/data.csv"
    print("Creating output directory", output)

    df.to_csv(output, sep=",", index=False)

    return output


@prefect.task
def print_ids():
    logger = prefect.context.get("logger")
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    flow_id = client.get_flow_run_info(flow_run_id)
    logger.info(f"[INFO] Flow Run ID: {flow_run_id}")

    for tr in flow_id.task_runs:
        task_slug = tr.task_slug
        logger.info(f"[INFO] Task Slug: {task_slug}")
        task_state = tr.state
        logger.info(f"[INFO] Task State: {task_state}")
    try:
        if task_slug == "create_table_and_upload_to_gcs-1":
            if task_state == "Success":
                logger.info(f"[INFO] Task {task_slug} completed successfully.")
            else:
                logger.info(f"[INFO] Task {task_slug} failed.")

        return True

    except Exception as e:
        logger.error(f"[ERROR] An error occurred: {e}")
        raise


@prefect.task
def get_flow_id():
    flow_run = context.get("flow_run_id")
    client = Client()
    flow_run_info = client.get_flow_run_info(flow_run_id=flow_run)
    print(f"[INFO] Flow Run Info: {flow_run_info}")

    return flow_run_info


@prefect.task
def get_info_from_context():
    # Usar o logger do contexto é a melhor prática
    logger = prefect.context.get("logger")

    flow_run_id = prefect.context.get("flow_run_id")
    flow_name = prefect.context.get("flow_name")
    flow_run_name = prefect.context.get("flow_run_name")
    task_run_id = prefect.context.get("task_run_id")
    params = prefect.context.get("parameters", {})

    logger.info("--- Informações do Contexto ---")
    logger.info(f"Flow Run ID: {flow_run_id}")
    logger.info(f"Flow Name: {flow_name}")
    logger.info(f"Flow Run Name: {flow_run_name}")
    logger.info(f"Task Run ID (atual): {task_run_id}")
    logger.info(f"Parâmetros: {params}")
    logger.info("-----------------------------")

    # Retorne as informações que você realmente precisa
    return {
        "flow_run_id": flow_run_id,
        "parameters": params,
        "flow_name": flow_name,
    }
