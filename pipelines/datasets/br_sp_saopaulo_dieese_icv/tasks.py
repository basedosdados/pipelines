"""
Tasks for br_sp_saopaulo_dieese_icv
"""
import os 
import pandas       as pd
import ipeadatapy   as idpy
from prefect import task

@task
def clean_dieese_icv ():
    os.makedirs('/tmp/data/br_sp_saopaulo_dieese_icv', exist_ok = True)
           
    codes  = ['DIEESE12_ICVSPD12', 'DIEESE12_ICVSPDG12']
    drop_m = ['DATE', 'DAY', 'CODE',         'RAW DATE']

    rename_m = {
        'YEAR'             : 'ano', 
        'MONTH'            : 'mes', 
        'VALUE (-)'        : 'indice', 
        'VALUE ((% a.m.))' : 'variacao_mensal'}  

    indice = idpy.timeseries(codes[0]).reset_index()   
    indice.drop(drop_m, axis = 1,     inplace = True)                    
    indice.rename(columns = rename_m, inplace = True)

    variacao_mensal = idpy.timeseries(codes[1]).reset_index()   
    variacao_mensal.drop(drop_m, axis = 1,     inplace = True)                    
    variacao_mensal.rename(columns = rename_m, inplace = True)                   
    
    icv_mes = pd.merge(indice, variacao_mensal, 
                   how      = 'left', 
                   left_on  = ['ano', 'mes'], 
                   right_on = ['ano', 'mes'])
                   
    filepath = '/tmp/data/br_sp_saopaulo_dieese_icv/mes.csv'
    icv_mes.to_csv(filepath)

    return filepath