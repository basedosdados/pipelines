import os
import warnings

import pandas as pd

warnings.filterwarnings("ignore")


def read_csv_enem_questionario():
    valor = 0
    for df in pd.read_csv(
        "/home/tricktx/dados/br_inep_enem/Microdados ENEM 2023/DADOS/MICRODADOS_ENEM_2023.csv",
        sep=";",
        encoding="latin1",
        chunksize=100000,
    ):
        valor = valor + 1
        print(valor)
        df = df[
            [
                "NU_INSCRICAO",
                "Q001",
                "Q002",
                "Q003",
                "Q004",
                "Q005",
                "Q006",
                "Q007",
                "Q008",
                "Q009",
                "Q010",
                "Q011",
                "Q012",
                "Q013",
                "Q014",
                "Q015",
                "Q016",
                "Q017",
                "Q018",
                "Q019",
                "Q020",
                "Q021",
                "Q022",
                "Q023",
                "Q024",
                "Q025",
            ]
        ]
        df = df.rename(columns={"NU_INSCRICAO": "id_inscricao"})

        path = "/home/tricktx/dados/br_inep_enem/output/questionario/"

        if not os.path.exists(path):
            os.makedirs(path)
            df.to_csv(
                os.path.join(path, "questionario_socioeconomico_2023.csv"),
                index=False,
                mode="a",
                header=True,
            )

        elif os.path.exists(path):
            df.to_csv(
                os.path.join(path, "questionario_socioeconomico_2023.csv"),
                index=False,
                mode="a",
                header=False,
            )


read_csv_enem_questionario()
