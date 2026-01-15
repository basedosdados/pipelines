"""
Tasks for basedosdados
"""

import os

import pandas as pd
from prefect import task


@task
def get_data_path() -> str:
    df = pd.DataFrame(
        {
            "ano": [2021, 2022, 2023],
            "github": ["folhesgabriel", "tricktx", "winzen"],
            "idade": [28, 28, 27],
            "sexo": ["Masculino", "Masculino", "Masculino"],
        }
    )

    os.makedirs("/tmp/output/", exist_ok=True)
    output = "/tmp/output/data.csv"
    df.to_csv(output, index=False)

    return output
