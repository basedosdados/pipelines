# -*- coding: utf-8 -*-
"""
General purpose functions for the br_me_cnpj project
"""

###############################################################################

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


def fill_left_zeros(df, column, num_digits):
    df[column] = df[column].astype(str).str.zfill(num_digits)
    return df


def remove_decimal_suffix(df, column):
    df[column] = df[column].astype(str).str.rstrip(".0")
    return df


def convert_columns_to_string(df, columns):
    if df is None:
        return df

    for col in columns:
        if col in df.columns and df[col].notna().all():
            df[col] = df[col].astype(str)
    return df


def data_url(url, headers):
    link_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(link_data.text, "html.parser")
    span_element = soup.find_all("td", align="right")
    data_completa = span_element[1].text.strip()
    data_str = data_completa[0:10]
    data = datetime.strptime(data_str, "%Y-%m-%d")

    return data
