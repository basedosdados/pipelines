import pandas as pd
import re

DICT_UFS = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}


def guess_header(df: pd.DataFrame, max_header_guess: int = 4) -> int:
    header_guess = 0
    while header_guess < max_header_guess:
        # Iffy logic, but essentially: if all rows of the column are strings, then this is a good candidate for a header.
        if all(df.iloc[header_guess].apply(lambda x: isinstance(x, str))):
            return header_guess

        header_guess += 1
    return 0  # If nothing is ever found until the max, let's just assume it's the first row as per usual.


def change_df_header(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    new_header = df.iloc[header_row]
    new_df = df[(header_row + 1) :].reset_index(drop=True)
    new_df.rename(columns=new_header, inplace=True)
    return new_df


def get_year_month_from_filename(filename: str) -> tuple[int, int]:
    match = re.search(r"(\w+)_(\d{1,2})-(\d{4})\.(xls|xlsx)$", filename)
    if match:
        month = match.group(2)
        year = match.group(3)
        return month, year
    else:
        raise ValueError("No match found")
