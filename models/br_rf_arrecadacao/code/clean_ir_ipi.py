# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
from clean_functions import *

def rename_columns(df):
    name_dict = {
        'Ano':'ano',
        'Mês':'mes',
        'Tributo':'tributo',
        'Decêndio':'decendio',
        'Arrecadação Bruta':'arrecadacao_bruta',
        'Retificação':'retificacao',
        'Compensação':'compensacao',
        'Restituição':'restituicao',
        'Outros':'outros',
        'Arrecadação Líquida':'arrecadacao_liquida'
    }

    return df.rename(columns=name_dict)

def change_types(df):
    df['ano'] = df['ano'].astype('int')
    df['mes'] = get_month_number(df['mes'])

    #All remaining columns are monetary values
    for col in df.columns[4:]:
        df[col] = df[col].apply(replace_commas).apply(remove_dots).astype('float')

    return df

if __name__ == '__main__':
    df = read_data(file_dir='../input/arrecadacao-ir-ipi.csv')
    df = remove_empty_rows(df)
    df = rename_columns(df)
    df = change_types(df)
    save_data(df=df,file_dir='../output/br_rf_arrecadacao_ir_ipi',partition_cols=['ano','mes'])
