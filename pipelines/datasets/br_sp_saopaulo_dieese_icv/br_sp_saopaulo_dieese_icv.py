#------------------------------------------------------------------------------#
# setup
#------------------------------------------------------------------------------#

pip install ipeadatapy

import os 
import pandas       as pd
import ipeadatapy   as idpy
import basedosdados as bd

#------------------------------------------------------------------------------#
# path
#------------------------------------------------------------------------------#
os.system('mkdir -p /tmp')

os.system('mkdir -p /tmp/data')
path_dados  =  '/tmp/data/br_sp_saopaulo_dieese_icv/'
path_output = path_dados + 'output/'

# /tmp/data/


#------------------------------------------------------------------------------#
# tratamento + output
#------------------------------------------------------------------------------#

def clean_dieese_icv ():

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
    return icv_mes 