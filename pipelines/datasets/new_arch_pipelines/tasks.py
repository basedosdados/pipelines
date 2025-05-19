import os

import pandas as pd
import prefect


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
