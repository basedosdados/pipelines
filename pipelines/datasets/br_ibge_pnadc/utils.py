from datetime import datetime

import pandas as pd


def get_extraction_year() -> int:
    current_year = datetime.now().year

    if datetime.now().month <= 4:
        current_year -= 1

    return current_year


def replace_null_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces common representations of null or empty string values with None in string columns.
    """
    null_strings = {"nan", "Nan", "NaN", "none", "None", "-", " ", ""}
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda x: None if str(x).strip() in null_strings else x
        )
    return df
