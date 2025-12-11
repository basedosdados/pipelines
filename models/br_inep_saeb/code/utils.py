import pandas as pd


def get_disciplina_serie(value: str) -> tuple[str, str]:
    _, serie, *rest = value.split("_")

    return ("_".join(rest), serie)


def get_nivel_serie_disciplina(value: str) -> tuple[int, str, str]:
    _, nivel_number, disciplina_serie = value.split("_")
    disciplina = disciplina_serie[0:2]
    serie = disciplina_serie[2:]
    return (int(nivel_number), disciplina, serie)


def convert_to_pd_dtype(type: str) -> str:
    if type == "INTEGER":
        return "int"
    elif type == "STRING":
        return "str"
    elif type == "FLOAT":
        return "float64"
    else:
        raise AssertionError


def drop_empty_lines(df: pd.DataFrame) -> pd.DataFrame:
    cp = df.copy()
    return cp.loc[
        (cp["media"].notna())
        | (cp["nivel_0"].notna())
        | (cp["nivel_1"].notna())
        | (cp["nivel_2"].notna())
        | (cp["nivel_3"].notna())
        | (cp["nivel_4"].notna())
        | (cp["nivel_5"].notna())
        | (cp["nivel_6"].notna())
        | (cp["nivel_7"].notna())
        | (cp["nivel_8"].notna())
        | (cp["nivel_9"].notna())
        | (cp["nivel_10"].notna())
    ]
