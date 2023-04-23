# -*- coding: utf-8 -*-
import pandas as pd
import polars as pl
import difflib
import re
from br_denatran_frota.code.constants import DICT_UFS, REGRAS


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


def verify_total(df: pl.DataFrame) -> None:
    columns_for_total = df.select(pl.exclude("TOTAL")).select(pl.exclude([pl.Utf8]))
    calculated_total = columns_for_total.select(
        pl.fold(
            acc=pl.lit(0), function=lambda acc, x: acc + x, exprs=pl.col("*")
        ).alias("calculated_total")
    )["calculated_total"]

    mask = df["TOTAL"] == calculated_total
    if pl.sum(~mask) != 0:
        raise ValueError(
            "A coluna de TOTAL da base original tem inconsistências e não soma tudo das demais colunas."
        )


def fix_suggested_nome_ibge(row) -> str:
    key = (row[0], row[1])
    if key in REGRAS:
        return REGRAS[key]
    else:
        return row[-1]


def match_ibge(denatran_uf: pl.DataFrame, ibge_uf: pl.DataFrame) -> None:
    joined_df = denatran_uf.join(
        ibge_uf,
        left_on=["suggested_nome_ibge", "sigla_uf"],
        right_on=["nome", "sigla_uf"],
        how="left",
    )
    mismatched_rows = joined_df.filter(pl.col("id_municipio").is_null())

    if len(mismatched_rows) > 0:
        error_message = "Os seguintes municípios falharam: \n"
        for row in mismatched_rows.rows(named=True):
            error_message += f"{row['nome_denatran']} ({row['sigla_uf']})\n"
        raise ValueError(error_message)


def get_city_name_ibge(denatran_name: str, ibge_uf: pl.DataFrame) -> str:
    matches = difflib.get_close_matches(
        denatran_name.lower(), ibge_uf["nome"].str.to_lowercase(), n=1
    )
    if matches:
        return matches[0]
    else:
        return ""  # I don't want this to error out directly, because then I can get all municipalities.
